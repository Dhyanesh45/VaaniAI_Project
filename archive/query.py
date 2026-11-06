"""Step 5: one station's history, gaps and status.

Brief: "People ask for one station's history" and "Stations report regularly,
until they don't."

Read path for one station:
  1. read the head: which segments, which floor
  2. skip segments whose month is outside [start, end)
  3. per remaining segment: footer (cached, files never change) -> one ranged
     GET for this station's blocks
  4. log objects above the floor (the recent tail not yet compacted)
  5. read the head again; if it moved, data may have moved, so start over
  6. resolve versions: dedupe, corrections, as-of
A year for one station is 12 monthly files -> 12 small ranged reads.
"""
from collections import OrderedDict

from . import segment
from .log import consistent_read, month_of_name, tail_rows
from .records import resolve


class FooterCache:
    """LRU capped in bytes. Footers never change (files are written once), so
    entries never need invalidating, only evicting."""

    def __init__(self, cap_bytes=128 * 1024 * 1024):
        self.cap, self.used, self.items = cap_bytes, 0, OrderedDict()
        self.hits = self.misses = 0

    def get(self, store, name):
        if name in self.items:
            self.items.move_to_end(name)
            self.hits += 1
            return self.items[name][0]
        self.misses += 1
        footer = segment.read_footer(store, name)
        if footer is None:
            return None
        size = 256 + sum(64 + 48 * len(b) for b in footer["stations"].values())  # rough
        self.items[name] = (footer, size)
        self.used += size
        while self.used > self.cap and len(self.items) > 1:
            _, (_, s) = self.items.popitem(last=False)
            self.used -= s
        return footer


def _overlaps(name, start, end):
    m_start, m_end = segment.month_bounds(month_of_name(name))
    return (start is None or m_end > start) and (end is None or m_start < end)


def station_rows(store, station, cache=None, start=None, end=None):
    """Every stored version of every point for one station (month-pruned)."""
    cache = cache or FooterCache()

    def read(head):
        rows = []
        for name in head["segments"]:
            if not _overlaps(name, start, end):
                continue
            footer = cache.get(store, name)
            block = None if footer is None else segment.read_station(store, name, footer, station)
            if block is None:
                raise segment.Missing(name)
            rows.extend(block)
        return rows + tail_rows(store, head, lambda r: r.get("station") == station)

    return consistent_read(store, read)


def history(store, station, start=None, end=None, as_of=None, cache=None):
    """Current value of each point in [start, end), as known at as_of."""
    rows = [r for r in station_rows(store, station, cache, start, end)
            if (start is None or r.get("t", 0) >= start) and (end is None or r.get("t", 0) < end)]
    return resolve(rows, as_of=as_of)


def gaps(points, cadence_s, tolerance=1.5):
    """Silences longer than tolerance x cadence, as (last_seen, next_seen) pairs.
    Missing data is reported as a gap. It is never filled in with zeros."""
    times = sorted({p["t"] for p in points})
    return [(a, b) for a, b in zip(times, times[1:]) if b - a > cadence_s * tolerance]


def status(points, now, cadence_s):
    """reporting / late / silent, from last-seen time only."""
    if not points:
        return "unknown"
    age = now - max(p["t"] for p in points)
    if age <= 2 * cadence_s:
        return "reporting"
    if age <= 60 * cadence_s:
        return "late"
    return "silent"
