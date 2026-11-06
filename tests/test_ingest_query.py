"""Steps 4 and 5: gateway, overload, history reads, gaps and status."""
import pytest

from archive.ingest import Gateway, Overloaded, shard_for
from archive.log import LogWriter, compact, init_log
from archive.query import FooterCache, gaps, history, status
from archive.records import Invalid


def make(new_store, shards=2, cap=10_000_000, clock=lambda: 1000):
    stores = []
    writers = []
    for i in range(shards):
        s = new_store(f"shard-{i}")
        init_log(s)
        w = LogWriter(s)
        w.take_over()
        stores.append(s)
        writers.append(w)
    return stores, writers, Gateway(writers, cap, clock)


def rep(station, t, v, rev=0):
    return {"station": station, "channel": "temp", "t": t, "v": v, "rev": rev}


def test_routing_is_stable():
    assert shard_for("st-42") == shard_for("st-42")
    assert {shard_for(f"st-{i}") for i in range(200)} == {0, 1, 2, 3}


def test_ack_only_after_commit_and_history_reads_back(new_store):
    stores, _, gw = make(new_store)
    gw.submit([rep("st-1", 60 * i, float(i)) for i in range(10)])
    assert gw.flush()  # acknowledged
    store = stores[shard_for("st-1", 2)]
    assert [p["v"] for p in history(store, "st-1")] == [float(i) for i in range(10)]


def test_history_merges_segments_with_the_unflushed_log_tail(new_store):
    stores, _, gw = make(new_store)
    store = stores[shard_for("st-1", 2)]
    gw.submit([rep("st-1", 0, 1.0), rep("st-1", 60, 2.0)])
    gw.flush()
    compact(store)
    gw.submit([rep("st-1", 60, 2.5, rev=1), rep("st-1", 30, 1.5)])  # correction + late
    gw.flush()
    out = history(store, "st-1")
    assert [(p["t"], p["v"]) for p in out] == [(0, 1.0), (30, 1.5), (60, 2.5)]


def test_overload_returns_429_and_buffers_nothing(new_store):
    _, _, gw = make(new_store, cap=300)
    with pytest.raises(Overloaded):
        gw.submit([rep("st-1", 60 * i, 1.0) for i in range(50)])
    assert all(not b.pending for b in gw.buffers)


def test_invalid_report_rejects_the_whole_request(new_store):
    _, _, gw = make(new_store)
    with pytest.raises(Invalid):
        gw.submit([rep("st-1", 0, 1.0), rep("st-1", 60, None)])
    assert all(not b.pending for b in gw.buffers)


def test_fenced_shard_does_not_acknowledge(new_store):
    stores, writers, gw = make(new_store, shards=1)
    LogWriter(stores[0]).take_over()  # another node takes the shard
    gw.submit([rep("st-1", 0, 1.0)])
    assert gw.flush() == {}  # nothing acknowledged; the station will resend


def test_footer_cache_avoids_repeat_fetches(new_store):
    stores, _, gw = make(new_store, shards=1)
    gw.submit([rep("st-1", 0, 1.0)])
    gw.flush()
    compact(stores[0])
    cache = FooterCache()
    history(stores[0], "st-1", cache=cache)
    history(stores[0], "st-1", cache=cache)
    assert (cache.misses, cache.hits) == (1, 1)


def test_silence_is_a_gap_not_a_zero():
    pts = [{"t": t} for t in (0, 60, 120, 900, 960)]
    assert gaps(pts, 60) == [(120, 900)]
    assert status(pts, now=1000, cadence_s=60) == "reporting"
    assert status(pts, now=3000, cadence_s=60) == "late"
    assert status(pts, now=50_000, cadence_s=60) == "silent"
