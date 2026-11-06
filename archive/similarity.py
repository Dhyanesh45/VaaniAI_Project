"""Step 6: fingerprints and "stations that behave most like this one".

The hard part is defining "behaves like". A fingerprint is a 128-number summary
of HOW a station's data moves, not WHAT level it sits at. Each channel is
z-normalised first, so a hot desert and a mild coast with the same daily rhythm
look alike, while a noisy or gappy station looks different.

Layout (128 dims):
  8 channel slots x 12 features = 96
      shape:    skew, excess kurtosis, outlier rate, IQR in z units
      rhythm:   daily Fourier amplitude at 1, 2, 3 cycles/day, yearly at 1 cycle/year
      memory:   autocorrelation at lags 1 h, 6 h, 24 h, 168 h
  28 cross-channel correlations (every pair of the 8 slots)
  4 reporting features: gap rate, mean gap length, longest gap, burstiness

Gaps are skipped, never filled with zeros. Every feature is computed only on
hours that have data. A bitmask records which channel slots exist, and search
compares stations only with stations that have the same channels.

Search is a flat cosine scan: 100,000 x 128 float32 = 51 MB, a few ms with NumPy.
"""
import io
import json
import os
import uuid

import numpy as np

CHANNELS = ["temp", "humidity", "pressure", "wind", "rain", "solar", "co2", "pm25"]
PER_CHANNEL = 12
DIM = len(CHANNELS) * PER_CHANNEL + 28 + 4  # 128
MIN_DAYS = 30
POINTER = "fingerprints/CURRENT"


class NotEnoughHistory(Exception):
    """Under 30 days of data: say so rather than return a confident bad match."""


def _corr(a, b):
    ok = ~(np.isnan(a) | np.isnan(b))
    if ok.sum() < 3:
        return 0.0
    a, b = a[ok], b[ok]
    sa, sb = a.std(), b.std()
    if sa == 0 or sb == 0:
        return 0.0
    return float(((a - a.mean()) * (b - b.mean())).mean() / (sa * sb))


def _amplitude(z, period_h):
    """Fourier amplitude using only observed hours (gaps skipped, not zero-filled)."""
    idx = np.flatnonzero(~np.isnan(z))
    if len(idx) < 3:
        return 0.0
    phase = np.exp(-2j * np.pi * idx / period_h)
    return float(2 * abs((z[idx] * phase).sum()) / len(idx))


def _channel_features(x, span_h):
    obs = x[~np.isnan(x)]
    if len(obs) < 3:
        return np.zeros(PER_CHANNEL)
    # Robust scaling: median and MAD, not mean and std. One bad value (a sensor
    # glitch, a wild correction) would otherwise stretch the whole scale and
    # erase the station's real rhythm. Found in the demo: a single 99 degree
    # correction moved a coastal station next to desert stations.
    med = np.median(obs)
    scale = np.median(np.abs(obs - med)) * 1.4826
    if scale == 0:
        scale = obs.std()
    if scale == 0:
        return np.zeros(PER_CHANNEL)
    raw_z = (x - med) / scale
    outlier_rate = float((np.abs(raw_z[~np.isnan(raw_z)]) > 3).mean())
    z = np.clip(raw_z, -4, 4)  # outliers are counted above, then stop dominating
    zo = z[~np.isnan(z)]
    zc = (zo - zo.mean()) / (zo.std() or 1.0)
    q75, q25 = np.percentile(zo, [75, 25])
    feats = [
        float((zc ** 3).mean()),
        float((zc ** 4).mean() - 3),
        outlier_rate,
        float(q75 - q25),
        _amplitude(z, 24), _amplitude(z, 12), _amplitude(z, 8),
        _amplitude(z, 24 * 365) if span_h >= 24 * 365 else 0.0,
    ]
    for lag in (1, 6, 24, 168):
        feats.append(_corr(z[:-lag], z[lag:]) if len(z) > lag + 3 else 0.0)
    return np.array(feats)


def _runs(empty):
    runs, n = [], 0
    for e in empty:
        if e:
            n += 1
        elif n:
            runs.append(n)
            n = 0
    if n:
        runs.append(n)
    return runs


def fingerprint(points, now=None, days=365):
    """points: resolved history (see query.history). Returns (vector, channel_mask)."""
    if not points:
        raise NotEnoughHistory("no data")
    now = max(p["t"] for p in points) if now is None else now
    first = min(p["t"] for p in points)
    start = max(now - days * 86400, first)
    if now - start < MIN_DAYS * 86400:
        raise NotEnoughHistory(f"{(now - start) / 86400:.1f} days, need {MIN_DAYS}")
    hours = int((now - start) // 3600) + 1
    sums = np.zeros((hours, len(CHANNELS)))
    counts = np.zeros((hours, len(CHANNELS)))
    reports = np.zeros(hours)
    seen = set()
    slot = {c: i for i, c in enumerate(CHANNELS)}
    for p in points:
        if p["channel"] not in slot or not start <= p["t"] <= now:
            continue
        h = int((p["t"] - start) // 3600)
        sums[h, slot[p["channel"]]] += p["v"]
        counts[h, slot[p["channel"]]] += 1
        if p["t"] not in seen:
            seen.add(p["t"])
            reports[h] += 1
    with np.errstate(invalid="ignore", divide="ignore"):
        hourly = np.where(counts > 0, sums / counts, np.nan)

    mask = 0
    parts = []
    for c in range(len(CHANNELS)):
        if counts[:, c].any():
            mask |= 1 << c
        parts.append(_channel_features(hourly[:, c], hours))
    cross = [_corr(hourly[:, i], hourly[:, j])
             for i in range(len(CHANNELS)) for j in range(i + 1, len(CHANNELS))]
    empty = reports == 0
    runs = _runs(empty)
    reporting = [
        float(empty.mean()),
        min(float(np.mean(runs)) / 24, 1.0) if runs else 0.0,
        float(max(runs)) / hours if runs else 0.0,
        min(float(reports.std() / reports.mean()), 3.0) if reports.mean() else 0.0,
    ]
    vec = np.concatenate(parts + [np.array(cross), np.array(reporting)]).astype(np.float32)
    vec = np.nan_to_num(vec)
    norm = np.linalg.norm(vec)
    return (vec / norm if norm else vec), mask


class Snapshot:
    """One immutable set of fingerprints. Queries hold a reference, so a swap
    to a newer snapshot never disturbs a query that is already running."""

    def __init__(self, ids, vectors, masks, version=None):
        self.ids = list(ids)
        self.pos = {s: i for i, s in enumerate(self.ids)}
        self.vectors, self.masks, self.version = vectors, masks, version

    @classmethod
    def build(cls, fingerprints):
        """fingerprints: {station: (vector, mask)}

        Raw features live on different scales, and features that every station
        shares swamp the few that tell stations apart. (Found in testing: the
        temp/humidity correlation sign that separates coast from desert is 1 of
        128 dims, and raw cosine scored them 0.9 alike.) So each dimension is
        standardised across the whole population before the cosine. A feature
        then counts by how much it varies between stations, not by its raw size.
        The per-dimension mean and std belong to the snapshot, so vectors from
        different snapshots are never compared.
        """
        ids = sorted(fingerprints)
        raw = np.stack([fingerprints[s][0] for s in ids]).astype(np.float64)
        mean, std = raw.mean(axis=0), raw.std(axis=0)
        std[std < 1e-9] = 1.0  # constant dims (e.g. absent channels) stay 0
        vecs = (raw - mean) / std
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        vecs = (vecs / norms).astype(np.float32)
        masks = np.array([fingerprints[s][1] for s in ids], dtype=np.uint16)
        return cls(ids, vecs, masks)

    def similar(self, station, k=5):
        """Top-k stations by cosine similarity with the same channel set."""
        i = self.pos[station]
        scores = self.vectors @ self.vectors[i]  # rows are unit length: dot = cosine
        scores = np.where(self.masks == self.masks[i], scores, -np.inf)
        scores[i] = -np.inf
        k = min(k, int(np.isfinite(scores).sum()))
        if k <= 0:
            return []
        top = np.argpartition(-scores, k - 1)[:k]
        top = top[np.argsort(-scores[top])]
        return [(self.ids[j], float(scores[j])) for j in top]


def _npy(arr):
    buf = io.BytesIO()
    np.save(buf, arr)
    return buf.getvalue()


def publish(store, snap):
    """Write a new snapshot, then move the pointer to it by compare-and-swap."""
    version = uuid.uuid4().hex[:12]
    base = f"fingerprints/{version}"
    store.put_if_absent(f"{base}/vectors.npy", _npy(snap.vectors))
    store.put_if_absent(f"{base}/masks.npy", _npy(snap.masks))
    store.put_if_absent(f"{base}/ids.json", json.dumps(snap.ids).encode())
    pointer = json.dumps({"version": version}).encode()
    while True:
        current, tag = store.get_with_etag(POINTER)
        if current is None:
            if store.put_if_absent(POINTER, pointer):
                return version
        elif store.swap(POINTER, tag, pointer):
            return version


def load(store, cache_dir):
    """Download the current snapshot to local disk once, then mmap the vectors.

    mmap is acceptable here (Crotty et al.): the file is read-only and fits in
    memory, so there are no dirty pages and no eviction surprises. Segments
    are different and use explicit reads with our own bounded cache.
    """
    version = json.loads(store.get(POINTER))["version"]
    local = os.path.join(cache_dir, version)
    os.makedirs(local, exist_ok=True)
    for name in ("vectors.npy", "masks.npy", "ids.json"):
        path = os.path.join(local, name)
        if not os.path.exists(path):
            tmp = path + ".part"
            with open(tmp, "wb") as f:
                f.write(store.get(f"fingerprints/{version}/{name}"))
            os.replace(tmp, path)
    with open(os.path.join(local, "ids.json")) as f:
        ids = json.load(f)
    vectors = np.load(os.path.join(local, "vectors.npy"), mmap_mode="r")
    masks = np.load(os.path.join(local, "masks.npy"))
    return Snapshot(ids, vectors, masks, version)
