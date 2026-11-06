"""End to end, one requirement from the brief at a time.

    python -m archive.demo
"""
import json
import tempfile
from pathlib import Path

from . import model_check, segment
from .ingest import Gateway, shard_for
from .log import (Fenced, LogWriter, compact, gc, init_log, merge, read_head,
                  set_write_format, upgrade_formats)
from .query import FooterCache, gaps, history, status
from .records import resolve
from .similarity import NotEnoughHistory, Snapshot, fingerprint, load, publish
from .synthetic import station_points

DAY = 86400
T0 = 1767225600  # 2026-01-01 00:00 UTC: event times start here
SHARDS = 4
DAYS = 35
CADENCE = 1800
KINDS = ("coastal", "desert", "flaky")
PER_KIND = 12


def title(text):
    print(f"\n== {text} " + "=" * max(0, 66 - len(text)))


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now


def main():
    root = Path(tempfile.mkdtemp(prefix="archive-demo-"))
    clock = Clock()
    stores, writers = [], []
    for i in range(SHARDS):
        s = __import__("archive.store", fromlist=["LocalStore"]).LocalStore(root / f"shard-{i}")
        init_log(s)
        w = LogWriter(s)
        w.take_over()
        stores.append(s)
        writers.append(w)
    gw = Gateway(writers, clock=clock)
    store_of = lambda st: stores[shard_for(st, SHARDS)]  # noqa: E731

    title("1. Stations report regularly, until they don't")
    names = [f"{k}-{i}" for k in KINDS for i in range(PER_KIND)]
    data = {n: station_points(n, n.split("-")[0], T0, DAYS, CADENCE) for n in names}
    raw_bytes = 0
    for day in range(DAYS):
        clock.now = (day + 1) * DAY
        batch = [p for n in names for p in data[n] if T0 + day * DAY <= p["t"] < T0 + (day + 1) * DAY]
        raw_bytes += sum(len(json.dumps(p)) for p in batch)
        gw.submit(batch)
        gw.flush()  # only now are these reports acknowledged
    total = sum(len(v) for v in data.values())
    print(f"{len(names)} stations x {DAYS} days -> {total:,} points in {SHARDS} shards,"
          f" one commit per shard per flush")

    title("2. Data arrives late, out of order, duplicated, corrected")
    st = "coastal-0"
    t_fix = T0 + 10 * DAY
    clock.now = 40 * DAY
    gw.submit([{"station": st, "channel": "temp", "t": t_fix, "v": 99.0, "rev": 1}])  # correction
    gw.submit([{"station": st, "channel": "temp", "t": t_fix + 900, "v": 14.2, "rev": 0}])  # late, between reports
    gw.flush()
    clock.now = 41 * DAY
    gw.submit([p for p in data[st] if p["t"] < T0 + DAY])  # a whole day resent (duplicate)
    gw.submit([{"station": st, "channel": "temp", "t": t_fix, "v": 15.0, "rev": 0}])  # stale original after fix
    gw.flush()
    h = {(p["channel"], p["t"]): p for p in history(store_of(st), st)}
    before = history(store_of(st), st, as_of=39 * DAY)
    old = next(p for p in before if p["channel"] == "temp" and p["t"] == t_fix)
    print(f"correction wins:      temp@day10 = {h[('temp', t_fix)]['v']} "
          f"(versions kept: {h[('temp', t_fix)]['versions']})")
    print(f"as known on day 39:   temp@day10 = {old['v']}")
    print(f"late point filed:     temp@day10+15min = {h[('temp', t_fix + 900)]['v']}")
    print(f"duplicates collapsed: {sum(1 for k in h if k[0] == 'temp' and k[1] < T0 + DAY)} temp points on day 0 (48 expected)")

    title("3. Machines disappear: takeover and fencing")
    s0 = store_of(st)
    old_owner = writers[shard_for(st, SHARDS)]
    new_owner = LogWriter(s0)
    new_owner.take_over()  # the old node froze; another node raised the epoch
    try:
        old_owner.commit([{"station": st, "channel": "temp", "t": T0 + 1, "v": 0.0, "rev": 0}])
        print("BUG: stale writer acknowledged")
    except Fenced as e:
        print(f"old owner wakes up and is fenced: {e}")
    seq = new_owner.commit([{"station": st, "channel": "temp", "t": T0 + 2, "v": 0.0, "rev": 0}])
    print(f"new owner commits seq {seq}; old owner never acknowledged, so the station resends")
    writers[shard_for(st, SHARDS)] = new_owner
    gw = Gateway(writers, clock=clock)

    title("4. Keep serving while data and formats change")
    for s in stores:
        compact(s)
    set_write_format(s0, 2)  # readers already understand format 2
    gw.submit([{"station": st, "channel": "temp", "t": T0 + 3, "v": 0.0, "rev": 0}])
    gw.flush()
    compact(s0)
    head, _ = read_head(s0)
    fmts = [segment.read_footer(s0, n)["format"] for n in head["segments"]]
    print(f"shard formats now: {fmts}  (mixed, still readable)")
    print(f"history during migration: {len(history(s0, st))} points")
    upgrade_formats(s0)
    merge(s0)
    removed = gc(s0, grace_s=0)
    head, _ = read_head(s0)
    fmts = [segment.read_footer(s0, n)["format"] for n in head["segments"]]
    print(f"after upgrade + merge + gc: formats {fmts}, {len(removed)} old objects removed")
    print(f"history after migration:  {len(history(s0, st))} points")
    print("files by event month:     " + ", ".join(
        f"{n.split('/')[1]}/{n.split('/')[2][:2]}" for n in head["segments"]))
    seg_bytes = sum(len(s.get(n)) for s in stores for n in read_head(s)[0]["segments"])
    print(f"storage: {raw_bytes / 1e6:.1f} MB as JSON -> {seg_bytes / 1e6:.2f} MB in segments"
          f" ({raw_bytes / seg_bytes:.1f}:1 with zlib; Gorilla + zstd would do better)")

    title("5. One station's history (ranged reads, cached footers)")
    cache = FooterCache()
    flaky = "flaky-0"
    pts = history(store_of(flaky), flaky, cache=cache)
    g = gaps(pts, CADENCE)
    longest = max((b - a for a, b in g), default=0)
    print(f"{flaky}: {len(pts)} points, {len(g)} gaps, longest {longest / 3600:.1f} h"
          " (stored as gaps, never zeros)")
    reg = history(store_of("desert-0"), "desert-0")
    for label, p in ((flaky, pts), ("desert-0", reg)):
        print(f"{label:9} status: day {DAYS} {status(p, T0 + DAYS * DAY, CADENCE)}, "
              f"day {DAYS}+12h {status(p, T0 + DAYS * DAY + 12 * 3600, CADENCE)}, "
              f"day {DAYS + 3} {status(p, T0 + (DAYS + 3) * DAY, CADENCE)}")
    history(store_of(flaky), flaky, cache=cache)
    print(f"footer cache: {cache.misses} misses then {cache.hits} hits")

    title("6. The stations that behave most like this one")
    fps = {}
    for n in names:
        try:
            fps[n] = fingerprint(history(store_of(n), n), now=T0 + DAYS * DAY)
        except NotEnoughHistory as e:
            print(f"{n}: {e}")
    try:
        fingerprint(resolve([dict(p, pos=[0, 0]) for p in data[st] if p["t"] < T0 + 10 * DAY]))
    except NotEnoughHistory as e:
        print(f"a 10-day-old station -> NotEnoughHistory ({e})")
    publish(stores[0], Snapshot.build(fps))
    snap = load(stores[0], root / "cache")  # local copy, vectors mmapped
    for q in ("coastal-0", "desert-0", "flaky-0"):
        print(f"{q:10} -> " + ", ".join(f"{n} {s:.2f}" for n, s in snap.similar(q, 4)))

    title("7. Acknowledged data must never be lost: every interleaving")
    for rule in ("put_only", "read_after_put"):
        n, trace = model_check.explore(rule)
        verdict = f"UNSAFE ({len(trace)}-step counterexample)" if trace else f"safe in all {n} states"
        print(f"{rule:15} {verdict}")
    print(f"\nfiles left in {root}")


if __name__ == "__main__":
    main()
