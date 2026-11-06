"""Brief: "the archive keeps growing" for years, on "very little memory".

1. Files are partitioned by event month, so one station's year is 12 small
   reads, and late data lands in the month it belongs to.
2. Compaction memory stays flat as the input grows (external sort + spill).
"""
import calendar
import tracemalloc

from archive import segment
from archive.log import (LogWriter, compact, init_log, merge, month_of_name, plan_merges,
                         read_head)
from archive.query import FooterCache, history
from conftest import CountingStore

T0 = calendar.timegm((2026, 1, 1, 0, 0, 0))
T1 = calendar.timegm((2027, 1, 1, 0, 0, 0))
DAY = 86400


def fresh(new_store, name="store"):
    s = new_store(name)
    init_log(s)
    w = LogWriter(s)
    w.take_over()
    return s, w


def daily(station, days, start=T0, v=1.0, rev=0):
    return [{"station": station, "channel": "temp", "t": start + DAY * i, "v": v, "rev": rev}
            for i in range(days)]


# ---------- partitioning ----------

def test_one_file_per_event_month(new_store):
    store, w = fresh(new_store)
    w.commit(daily("a", 365) + daily("b", 365))
    names = compact(store)
    assert sorted(month_of_name(n) for n in names) == [f"2026-{m:02d}" for m in range(1, 13)]


def test_one_station_year_is_12_small_reads(new_store):
    store, w = fresh(new_store)
    w.commit(daily("a", 365) + daily("b", 365) + daily("c", 365))
    compact(store)
    counted = CountingStore(store)
    cache = FooterCache()
    assert len(history(counted, "a", T0, T1, cache=cache)) == 365
    assert counted.counts["get_range"] == 24  # 12 footers + 12 station blocks
    counted.counts.clear()
    history(counted, "a", T0, T1, cache=cache)
    assert counted.counts["get_range"] == 12  # footers cached: 12 block reads


def test_query_for_one_month_skips_the_other_eleven(new_store):
    store, w = fresh(new_store)
    w.commit(daily("a", 365))
    compact(store)
    counted = CountingStore(store)
    march = calendar.timegm((2026, 3, 1, 0, 0, 0)), calendar.timegm((2026, 4, 1, 0, 0, 0))
    assert len(history(counted, "a", *march)) == 31
    assert counted.counts["get_range"] == 2  # March footer + March block


def test_late_data_lands_in_its_own_month(new_store):
    store, w = fresh(new_store)
    w.commit(daily("a", 90))
    compact(store)
    w.commit([{"station": "a", "channel": "temp", "t": T0 + 5 * DAY, "v": 9.0, "rev": 1}])
    late, = compact(store)
    assert month_of_name(late) == "2026-01"
    got = {p["t"]: p["v"] for p in history(store, "a")}
    assert got[T0 + 5 * DAY] == 9.0 and len(got) == 90


def test_sealed_month_is_rewritten_only_after_5_percent_late_data(new_store):
    store, w = fresh(new_store)
    for half in range(2):  # two L1 files for January -> merged into one L2
        w.commit([{"station": f"s{i}", "channel": "temp", "t": T0 + DAY * d + half * 3600,
                   "v": 1.0, "rev": 0} for i in range(20) for d in range(31)])
        compact(store)
    jan_l2, = merge(store)
    assert "/L2-" in jan_l2
    w.commit(daily("s0", 3, v=2.0, rev=1))  # 3 late rows vs 1240: under 5%
    compact(store)
    assert plan_merges(store) == []  # kept as a small delta, merged on read
    assert history(store, "s0", T0, T0 + DAY)[0]["v"] == 2.0
    w.commit([{"station": f"s{i}", "channel": "temp", "t": T0 + DAY * d, "v": 3.0, "rev": 2}
              for i in range(20) for d in range(4)])  # 80 more: now over 5%
    compact(store)
    rewritten, = merge(store)
    head, _ = read_head(store)
    assert [n for n in head["segments"] if month_of_name(n) == "2026-01"] == [rewritten]
    assert history(store, "s0", T0, T0 + DAY)[0]["v"] == 3.0


# ---------- memory ----------

def _peak_compaction(new_store, name, batches, memory):
    store, w = fresh(new_store, name)
    for b in range(batches):
        w.commit([{"station": f"st-{(b * 1000 + i) % 200}", "channel": "temp",
                   "t": T0 + (b * 1000 + i) * 60, "v": float(i), "rev": 0} for i in range(1000)])
    tracemalloc.start()
    compact(store, memory=memory)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    rows = sum(segment.read_footer(store, n)["rows"] for n in read_head(store)[0]["segments"])
    return peak, rows


def test_compaction_memory_stays_flat_as_input_doubles(new_store):
    small, rows_small = _peak_compaction(new_store, "small", 20, memory=1 << 20)
    big, rows_big = _peak_compaction(new_store, "big", 40, memory=1 << 20)
    unbounded, _ = _peak_compaction(new_store, "ram", 40, memory=1 << 34)
    assert (rows_small, rows_big) == (20_000, 40_000)  # nothing lost
    assert big < small * 1.5            # 2x the data, about the same memory
    assert big < unbounded / 2          # and far below loading everything
    assert big < 12_000_000             # absolute: ~1 MB cap + one batch + one block
