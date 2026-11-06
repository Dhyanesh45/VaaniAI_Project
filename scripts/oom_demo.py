"""Compact N rows under a given memory cap and report real memory use.

Meant to run inside a Linux container with a hard cgroup memory limit:

    python scripts/oom_demo.py --rows 400000 --cap-mb 16      # streaming: fits
    python scripts/oom_demo.py --rows 400000 --cap-mb 100000  # all in RAM: OOM-killed

Prints the kernel's view of memory (peak RSS and the cgroup's limit and peak),
not Python's estimate, because the OOM killer only cares about the kernel's.
"""
import argparse
import os
import resource
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from archive.log import LogWriter, compact, init_log, read_head  # noqa: E402
from archive import segment  # noqa: E402
from archive.store import LocalStore  # noqa: E402

T0 = 1767225600


def cgroup(name):
    try:
        with open(f"/sys/fs/cgroup/{name}") as f:
            v = f.read().strip()
        return v if v == "max" else f"{int(v) / 2**20:.0f} MB"
    except OSError:
        return "n/a (not Linux cgroup v2)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=400_000)
    ap.add_argument("--cap-mb", type=float, default=16)
    ap.add_argument("--batch", type=int, default=1000)
    a = ap.parse_args()

    print(f"kernel: {os.uname().sysname} {os.uname().release}  cgroup memory.max = {cgroup('memory.max')}",
          flush=True)
    store = LocalStore(tempfile.mkdtemp(prefix="oom-"))
    init_log(store)
    w = LogWriter(store)
    w.take_over()
    for b in range(a.rows // a.batch):
        w.commit([{"station": f"st-{(b * a.batch + i) % 500}", "channel": "temp",
                   "t": T0 + (b * a.batch + i) * 60, "v": float(i), "rev": 0}
                  for i in range(a.batch)])
    print(f"wrote {a.rows:,} rows to the log; compacting with a {a.cap_mb:g} MB sort budget...",
          flush=True)
    t = time.time()
    compact(store, memory=int(a.cap_mb * 2**20))
    rows = sum(segment.read_footer(store, n)["rows"] for n in read_head(store)[0]["segments"])
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB on Linux
    print(f"compacted {rows:,} rows in {time.time() - t:.1f}s  "
          f"peak RSS = {peak_rss:.0f} MB  cgroup memory.peak = {cgroup('memory.peak')}")


if __name__ == "__main__":
    main()
