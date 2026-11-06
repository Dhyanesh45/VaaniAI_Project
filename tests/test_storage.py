"""Step 3: segments, compaction levels, format changes and safe garbage collection."""
import json

import pytest

from archive import segment
from archive.log import (LogWriter, compact, gc, init_log, merge, read_all, read_head,
                         set_write_format, upgrade_formats)


@pytest.fixture
def store(new_store):
    s = new_store()
    init_log(s)
    return s


def pts(station, n, t0=0):
    return [{"station": station, "channel": "temp", "t": t0 + 60 * i, "v": float(i), "rev": 0}
            for i in range(n)]


def stations(store):
    return {r["station"] for r in read_all(store)}


def test_one_station_is_read_with_a_ranged_get(store):
    w = LogWriter(store)
    w.take_over()
    for s in ("a", "b", "c"):
        w.commit(pts(s, 100))
    name, = compact(store)
    footer = segment.read_footer(store, name)
    assert set(footer["stations"]) == {"a", "b", "c"}
    rows = segment.read_station(store, name, footer, "b")
    assert len(rows) == 100 and {r["station"] for r in rows} == {"b"}
    length = sum(b[1] for b in footer["stations"]["b"])
    assert length < len(store.get(name)) / 2  # we fetched a slice, not the file


def test_delta_of_delta_round_trips():
    ts = [0, 60, 120, 180, 245, 300, 1000]
    assert segment._undod(segment._dod(ts)) == ts
    assert segment._dod([0, 60, 120, 180, 240])[2:] == [0, 0, 0]


def test_format_change_while_serving(store):
    w = LogWriter(store)
    w.take_over()
    w.commit(pts("a", 50))
    old, = compact(store)
    set_write_format(store, 2)  # readers already understand 1 and 2
    w.commit(pts("b", 50))
    new, = compact(store)
    assert segment.read_footer(store, old)["format"] == 1
    assert segment.read_footer(store, new)["format"] == 2
    assert stations(store) == {"a", "b"}  # mixed formats read fine
    upgrade_formats(store)
    head, _ = read_head(store)
    assert {segment.read_footer(store, n)["format"] for n in head["segments"]} == {2}
    assert stations(store) == {"a", "b"}


def test_reader_rejects_a_format_it_does_not_know(store):
    data = bytearray(segment.build(pts("a", 3), 1, 1, 1))
    data[-5] = 9  # pretend a newer release wrote format 9
    store.put_if_absent("segments/future", bytes(data))
    with pytest.raises(segment.UnknownFormat):
        segment.read_footer(store, "segments/future")


def test_merge_l1_into_l2_keeps_everything(store):
    w = LogWriter(store)
    w.take_over()
    for i, s in enumerate("abcd"):
        w.commit(pts(s, 20))
        compact(store)
    new, = merge(store)
    assert "/L2-" in new
    head, _ = read_head(store)
    assert len(head["segments"]) == 1
    assert stations(store) == set("abcd")


def test_duplicate_retry_collapses_in_compaction(store):
    w = LogWriter(store)
    w.take_over()
    w.commit(pts("a", 10))
    w.commit(pts("a", 10))  # the sender never saw the ack and resent
    compact(store)
    assert len(read_all(store)) == 10


def test_two_compactors_one_wins(store):
    w = LogWriter(store)
    w.take_over()
    w.commit(pts("a", 10))
    head, tag = read_head(store)  # compactor 2 reads the head ...
    assert compact(store)          # ... compactor 1 finishes first
    from archive.log import swap_head
    assert not swap_head(store, tag, dict(head, floor=1, segments=["segments/loser"]))
    assert len(read_all(store)) == 10


def test_gc_cannot_delete_a_file_a_paused_compactor_is_about_to_publish(store):
    """The same 'deletion can undo a lock' trap, this time in garbage collection."""
    w = LogWriter(store)
    w.take_over()
    w.commit(pts("a", 10))
    # A compactor reads the head and writes its segment, then pauses.
    head, tag = read_head(store)
    name = "segments/1970-01/L1-paused"
    store.put_if_absent(name, segment.build(read_all(store), 1, 1, 1))
    # gc sees an unreferenced file and deletes it (grace 0 to force it).
    assert name in gc(store, grace_s=0)
    # The compactor wakes and tries to publish. It must fail, because gc
    # changed the head before deleting.
    from archive.log import swap_head
    assert not swap_head(store, tag, dict(head, floor=1, segments=[name]))
    assert len(read_all(store)) == 10


def test_gc_removes_stale_writes_below_the_floor(store):
    w = LogWriter(store)
    w.take_over()
    w.commit(pts("a", 5))
    compact(store)
    store.put_if_absent("log/000000000001", json.dumps(pts("ghost", 1)).encode())
    assert "log/000000000001" in gc(store, grace_s=0)
    assert stations(store) == {"a"}
