"""Step 2: what a point is, and how late, duplicated and corrected data resolve.

Brief: "Data can arrive late, out of order, duplicated, or corrected long after
the fact."

A point is one channel of one report:

    {"station": "st-7", "channel": "temp", "t": 1730800000, "v": 21.5, "rev": 0}

  t        event time: when the sensor measured it (seconds)
  rev      source revision: the station bumps it when it corrects a value
  arrived  arrival time, stamped by the gateway (added on ingest)
  pos      log position [seq, index], added when read back from the log

Rules (the "Arrival" table in the design doc):
  late / out of order    filed by event time t; arrival order never decides placement
  exact duplicate        same key + rev + payload hash collapse to one copy
  correction             higher rev wins; older versions are kept, not overwritten
  original after fix     lower rev is stored but never wins
  no rev from source     log position breaks the tie, and the result is flagged weak
  silence                nothing is stored; a gap is absence, never a zero
"""
import hashlib
import math


class Invalid(ValueError):
    """The gateway rejects this report (HTTP 400). Nothing is stored."""


def validate(raw):
    station, channel, t, v = (raw.get(k) for k in ("station", "channel", "t", "v"))
    if not isinstance(station, str) or not station:
        raise Invalid("station must be a non-empty string")
    if not isinstance(channel, str) or not channel:
        raise Invalid("channel must be a non-empty string")
    if isinstance(t, bool) or not isinstance(t, int) or t < 0:
        raise Invalid("t must be the event time in whole seconds")
    if v is None:
        raise Invalid("a missing value is silence: send nothing, not null or 0")
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise Invalid("v must be a finite number")
    rev = raw.get("rev")
    if rev is not None and (isinstance(rev, bool) or not isinstance(rev, int) or rev < 0):
        raise Invalid("rev must be a non-negative integer when present")
    return {"station": station, "channel": channel, "t": t, "v": float(v), "rev": rev}


def is_point(r):
    return all(k in r for k in ("station", "channel", "t", "v"))


def point_key(r):
    """Where a point lives: one station, one channel, one event time."""
    return (r["station"], r["channel"], r["t"])


def payload_hash(r):
    return hashlib.sha256(repr(float(r["v"])).encode()).hexdigest()[:16]


def version(r):
    """Which version wins: source revision first, log position as tie-breaker."""
    rev = r.get("rev")
    return (rev if rev is not None else 0, tuple(r.get("pos", (0, 0))))


def dedupe(rows):
    """Collapse exact duplicates (retries, double sends). Keeps the earliest copy.

    Corrections are NOT duplicates: a different rev or value is a new version.
    Rows that are not points (tests use {"id": ...}) pass through untouched.
    """
    out, seen = [], set()
    for r in sorted(rows, key=lambda r: tuple(r.get("pos", (0, 0)))):
        if not is_point(r):
            out.append(r)
            continue
        ident = point_key(r) + (r.get("rev"), payload_hash(r))
        if ident in seen:
            continue
        seen.add(ident)
        out.append(r)
    return out


def dedupe_sorted(rows):
    """Streaming dedupe for rows already sorted by (station, channel, t, pos).
    Copies of one point are adjacent, so memory is one point's versions."""
    current, seen = None, set()
    for r in rows:
        if not is_point(r):
            yield r
            continue
        key = point_key(r)
        if key != current:
            current, seen = key, set()
        ident = (r.get("rev"), payload_hash(r))
        if ident in seen:
            continue
        seen.add(ident)
        yield r


def resolve(rows, as_of=None):
    """Turn every stored version into one current value per point.

    as_of: only consider versions that had arrived by then. This answers
    "what did we know on 1 March?" rather than "what happened on 1 March?".
    """
    groups = {}
    for r in dedupe(rows):
        if not is_point(r):
            continue
        if as_of is not None and r.get("arrived", 0) > as_of:
            continue
        groups.setdefault(point_key(r), []).append(r)
    out = []
    for versions in groups.values():
        win = max(versions, key=version)
        out.append({
            "station": win["station"],
            "channel": win["channel"],
            "t": win["t"],
            "v": win["v"],
            "rev": win.get("rev"),
            "versions": len(versions),
            # Two different values and no revision to choose between them:
            # arrival order decided, which is weaker and worth surfacing.
            "weak": win.get("rev") is None and len(versions) > 1,
        })
    out.sort(key=lambda p: (p["channel"], p["t"]))
    return out
