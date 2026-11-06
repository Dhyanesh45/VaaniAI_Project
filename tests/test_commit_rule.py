import random

import pytest

from archive.log import Fenced, LogWriter, compact, init_log, read_all
from archive.store import LocalStore


@pytest.fixture
def store(tmp_path):
    s = LocalStore(tmp_path)
    init_log(s)
    return s


def writer(store):
    w = LogWriter(store)
    w.take_over()
    return w


def ids(store):
    return {r["id"] for r in read_all(store)}


def test_acknowledged_batch_is_readable(store):
    w = writer(store)
    assert w.commit([{"id": "a"}]) == 1
    assert w.commit([{"id": "b"}]) == 2
    assert ids(store) == {"a", "b"}


def test_stale_writer_loses_a_taken_number(store):
    a = writer(store)
    a.commit([{"id": "a1"}])
    b = writer(store)
    b.commit([{"id": "b1"}])
    with pytest.raises(Fenced):
        a.commit([{"id": "a2"}])


def test_stale_writer_with_a_free_number_never_acknowledges(store):
    a = writer(store)
    writer(store)  # b takes over and writes nothing
    with pytest.raises(Fenced):
        a.commit([{"id": "a1"}])


def test_stale_writer_cannot_reuse_a_deleted_number(store):
    """The failure taught in the design doc."""
    a = writer(store)
    a.commit([{"id": "a1"}])
    b = writer(store)
    b.commit([{"id": "b1"}])
    compact(store)
    with pytest.raises(Fenced):
        a.commit([{"id": "a2"}])


def test_owner_skips_a_number_taken_by_a_stale_writer(store):
    a = writer(store)
    b = writer(store)
    with pytest.raises(Fenced):
        a.commit([{"id": "a1"}])
    assert b.commit([{"id": "b1"}]) == 2
    assert "b1" in ids(store)


class InterruptingStore:
    """Runs another machine's work just before one chosen storage call."""

    def __init__(self, inner, fire_at, action):
        self.inner, self.fire_at, self.action = inner, fire_at, action
        self.calls = 0

    def __getattr__(self, name):
        real = getattr(self.inner, name)

        def wrapped(*args):
            self.calls += 1
            if self.calls == self.fire_at:
                self.action()
            return real(*args)

        return wrapped


@pytest.mark.parametrize("seed", range(300))
def test_acknowledged_data_survives_random_takeovers(tmp_path, seed):
    rng = random.Random(seed)
    base = LocalStore(tmp_path)
    init_log(base)
    acked = set()
    new_owner = []

    def other_machine():
        b = writer(base)
        for i in range(rng.randint(0, 3)):
            b.commit([{"id": f"b{i}"}])
            acked.add(f"b{i}")
        compact(base)
        new_owner.append(b)

    a = LogWriter(InterruptingStore(base, rng.randint(1, 16), other_machine))
    a.take_over()
    for i in range(6):
        try:
            a.commit([{"id": f"a{i}"}])
            acked.add(f"a{i}")
        except Fenced:
            break
    if new_owner:
        try:  # If the interruption landed during a's takeover, a owns the log.
            new_owner[0].commit([{"id": "after"}])
            acked.add("after")
        except Fenced:
            pass
        compact(base)
    assert acked <= ids(base)
