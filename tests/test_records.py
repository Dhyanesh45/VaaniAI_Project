"""Step 2: late, out-of-order, duplicated and corrected data."""
import pytest

from archive.records import Invalid, resolve, validate


def row(t, v, rev=0, seq=1, i=0, arrived=0, channel="temp"):
    return {"station": "s", "channel": channel, "t": t, "v": v, "rev": rev,
            "pos": [seq, i], "arrived": arrived}


def test_out_of_order_is_filed_by_event_time():
    out = resolve([row(300, 3.0, seq=1), row(100, 1.0, seq=2), row(200, 2.0, seq=3)])
    assert [p["t"] for p in out] == [100, 200, 300]


def test_exact_duplicates_collapse():
    out = resolve([row(100, 1.0, seq=1), row(100, 1.0, seq=7), row(100, 1.0, seq=9)])
    assert len(out) == 1 and out[0]["versions"] == 1


def test_correction_wins_and_old_version_is_kept():
    out = resolve([row(100, 1.0, rev=0, seq=1), row(100, 1.5, rev=1, seq=50)])
    assert out[0]["v"] == 1.5 and out[0]["versions"] == 2


def test_original_arriving_after_its_correction_never_wins():
    out = resolve([row(100, 1.5, rev=1, seq=1), row(100, 1.0, rev=0, seq=50)])
    assert out[0]["v"] == 1.5


def test_no_revision_falls_back_to_log_position_and_is_flagged():
    out = resolve([row(100, 1.0, rev=None, seq=1), row(100, 2.0, rev=None, seq=2)])
    assert out[0]["v"] == 2.0 and out[0]["weak"]


def test_as_of_answers_what_we_knew_then():
    rows = [row(100, 1.0, rev=0, seq=1, arrived=1000), row(100, 1.5, rev=1, seq=2, arrived=5000)]
    assert resolve(rows, as_of=2000)[0]["v"] == 1.0
    assert resolve(rows)[0]["v"] == 1.5


@pytest.mark.parametrize("bad", [
    {"station": "s", "channel": "temp", "t": 1, "v": None},
    {"station": "s", "channel": "temp", "t": 1, "v": float("nan")},
    {"station": "", "channel": "temp", "t": 1, "v": 1},
    {"station": "s", "channel": "temp", "t": "noon", "v": 1},
    {"station": "s", "channel": "temp", "t": 1, "v": 1, "rev": -1},
])
def test_invalid_reports_are_rejected(bad):
    with pytest.raises(Invalid):
        validate(bad)
