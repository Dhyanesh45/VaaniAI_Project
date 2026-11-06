"""Brief: "the system must keep serving while its data and formats change".

A reader is interrupted at EVERY one of its storage calls by another machine
that commits, compacts, merges and garbage-collects. Whatever the timing, the
reader must return every acknowledged point, exactly once.
"""
import pytest

from archive.log import LogWriter, compact, gc, init_log, merge, read_all, set_write_format
from archive.query import history
from conftest import InterruptingStore

DAY = 86400
T0 = 1767225600  # 2026-01-01 UTC


def pts(station, start, n, step=3600):
    return [{"station": station, "channel": "temp", "t": start + step * i, "v": float(i), "rev": 0}
            for i in range(n)]


def setup(store):
    init_log(store)
    w = LogWriter(store)
    w.take_over()
    # Two months of data, part compacted, part still in the log.
    w.commit(pts("a", T0, 30) + pts("b", T0, 30))
    w.commit(pts("a", T0 + 40 * DAY, 30))
    compact(store)
    w.commit(pts("a", T0 + 2 * DAY, 10))
    w.commit(pts("b", T0 + 41 * DAY, 10))
    return w


def churn(store, writer):
    def action():
        writer.commit(pts("a", T0 + 50 * DAY, 5))  # new acknowledged data
        compact(store)
        set_write_format(store, 2)
        merge(store)
        gc(store, grace_s=0)  # worst case: delete orphans immediately
    return action


EXPECTED_A = 30 + 30 + 10  # before churn; churn adds 5 more


@pytest.mark.parametrize("fire_at", range(1, 25))
def test_history_is_complete_whenever_compaction_strikes(new_store, fire_at):
    base = new_store()
    w = setup(base)
    reader = InterruptingStore(base, fire_at, churn(base, w))
    got = history(reader, "a")
    fired = reader.calls >= fire_at
    # Every point acknowledged before the read started must be there. The 5
    # committed mid-read may or may not be, depending on timing.
    assert len(got) in ((EXPECTED_A, EXPECTED_A + 5) if fired else (EXPECTED_A,))
    assert len({(p["channel"], p["t"]) for p in got}) == len(got)  # no duplicates
    assert len(history(base, "a")) == EXPECTED_A + (5 if fired else 0)


@pytest.mark.parametrize("fire_at", range(1, 25))
def test_full_scan_is_complete_whenever_compaction_strikes(new_store, fire_at):
    base = new_store()
    w = setup(base)
    reader = InterruptingStore(base, fire_at, churn(base, w))
    stations = [r["station"] for r in read_all(reader)]
    assert stations.count("b") == 40
    assert stations.count("a") >= EXPECTED_A
