"""Steps 6 and 7: similar stations, and the exhaustive interleaving check."""
import numpy as np
import pytest

from archive import model_check
from archive.records import resolve
from archive.similarity import DIM, NotEnoughHistory, Snapshot, fingerprint, load, publish
from archive.synthetic import station_points

KINDS = ("coastal", "desert", "flaky")
PER_KIND = 12  # standardisation needs a population; 4 per kind was too few


@pytest.fixture(scope="module")
def snap():
    fps = {}
    for kind in KINDS:
        for i in range(PER_KIND):
            name = f"{kind}-{i}"
            rows = [dict(p, pos=[0, 0]) for p in station_points(name, kind, 0, 40)]
            fps[name] = fingerprint(resolve(rows))
    return Snapshot.build(fps)


def kind(name):
    return name.split("-")[0]


def test_fingerprint_shape(snap):
    assert snap.vectors.shape == (len(KINDS) * PER_KIND, DIM) and DIM == 128
    assert np.allclose(np.linalg.norm(snap.vectors, axis=1), 1, atol=1e-5)


def test_nearest_neighbour_usually_behaves_alike(snap):
    """Similarity is statistical: measure quality, don't demand perfection.
    Measured: 35 of 36 (the miss is a 0.64 vs 0.64 near-tie)."""
    right = sum(kind(snap.similar(s, 1)[0][0]) == kind(s) for s in snap.ids)
    assert right / len(snap.ids) >= 0.9


def test_precision_at_5_is_high(snap):
    hits = sum(kind(n) == kind(s) for s in snap.ids for n, _ in snap.similar(s, 5))
    assert hits / (5 * len(snap.ids)) >= 0.9


def test_short_history_says_so():
    rows = [dict(p, pos=[0, 0]) for p in station_points("new", "coastal", 0, 10)]
    with pytest.raises(NotEnoughHistory):
        fingerprint(resolve(rows))


def test_different_channel_sets_are_not_compared(snap):
    masks = snap.masks.copy()
    masks[snap.pos["desert-1"]] = 1  # pretend desert-1 only has temperature
    other = Snapshot(snap.ids, snap.vectors, masks)
    assert "desert-1" not in [s for s, _ in other.similar("desert-0", k=10)]


def test_snapshot_swap_does_not_disturb_running_queries(snap, tmp_path, new_store):
    store = new_store("store")
    publish(store, snap)
    first = load(store, tmp_path / "cache")
    v2 = Snapshot(snap.ids, snap.vectors[::-1].copy(), snap.masks[::-1].copy())
    publish(store, v2)
    second = load(store, tmp_path / "cache")
    assert first.version != second.version
    assert first.similar("coastal-0", 3) == snap.similar("coastal-0", 3)  # old still usable


def test_put_only_rule_loses_acknowledged_data():
    _, trace = model_check.explore("put_only")
    assert trace is not None


def test_read_after_put_rule_is_safe_in_every_interleaving():
    states, trace = model_check.explore("read_after_put")
    assert trace is None and states > 100
