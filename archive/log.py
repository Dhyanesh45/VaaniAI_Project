"""One log shard: the commit rule, takeover, compaction and reads.

The head object is the shard's single source of truth (the design doc calls it
the head for the log and the manifest for segments; here they are one object):

    {"epoch": 3,              who may write: raised by every takeover
     "floor": 120,            log objects at or below this are folded into segments
     "segments": [...],       the files readers should use
     "write_format": 2,       format new files are written in
     "gc": 5}                 bumped by garbage collection before it deletes

Every change to the head is a compare-and-swap. That one rule replaces
etcd/ZooKeeper: two machines may both try, exactly one wins.

Segments are named by the calendar month of their EVENT times:
    segments/2026-03/L1-000000000101-000000000140-3f2a9c1b
so late data lands in the month it belongs to, and a query for March only
touches March's files.
"""
import heapq
import json
import os
import tempfile
import time
import uuid

from . import segment
from .records import dedupe_sorted
from .spill import ExternalSorter

HEAD = "head"
COMPACT_MEMORY = 256 * 1024 * 1024  # the design doc's compaction budget
REWRITE_FRACTION = 0.05  # rewrite a sealed month once its late deltas reach 5%


class Fenced(Exception):
    """This writer no longer owns the log. It must stop and must not acknowledge."""


def log_key(seq):
    return f"log/{seq:012d}"


def seq_of(key):
    return int(key.split("/")[1])


def init_log(store, write_format=1):
    head = {"epoch": 0, "floor": 0, "segments": [], "write_format": write_format, "gc": 0}
    store.put_if_absent(HEAD, json.dumps(head).encode())


def read_head(store):
    raw, tag = store.get_with_etag(HEAD)
    return json.loads(raw), tag


def swap_head(store, tag, new_head):
    return store.swap(HEAD, tag, json.dumps(new_head).encode())


def head_seq(store):
    keys = store.list("log")
    return seq_of(keys[-1]) if keys else 0


class LogWriter:
    def __init__(self, store):
        self.store = store
        self.epoch = None
        self.next_seq = None

    def take_over(self):
        """Raise the epoch by compare-and-swap, then find the end of the log."""
        while True:
            head, tag = read_head(self.store)
            new_head = dict(head, epoch=head["epoch"] + 1)
            if self.store.swap(HEAD, tag, json.dumps(new_head).encode()):
                self.epoch = new_head["epoch"]
                self.next_seq = max(head_seq(self.store), new_head["floor"]) + 1
                return

    def commit(self, records):
        """Durably store records. Return the sequence number once safe to acknowledge.

        Raise Fenced if this writer has lost ownership.

        1. Encode the batch: json.dumps(records).encode()
        2. Try store.put_if_absent(log_key(self.next_seq), data).
        3. Read the head with read_head(self.store).
        4. If the head's epoch differs from self.epoch, raise Fenced.
        5. If the PUT returned False, you still own the log, so a stale writer
           took this number. Add 1 to self.next_seq and go back to step 2.
        6. If the head's floor is at or above this sequence number, raise Fenced.
        7. Otherwise save the sequence number, add 1 to self.next_seq, return it.
        """
        data = json.dumps(records).encode()
        while True:
            seq = self.next_seq
            # Create-only write: never overwrites another writer's batch.
            created = self.store.put_if_absent(log_key(seq), data)
            # Re-read the head AFTER the write. Checking before would leave a
            # gap where a takeover could slip in unnoticed.
            head, _ = read_head(self.store)
            if head["epoch"] != self.epoch:
                raise Fenced(f"epoch moved {self.epoch} -> {head['epoch']}")
            if not created:
                # We still own the log, so a stale writer grabbed this slot.
                # Skip it; its contents were never acknowledged by us.
                self.next_seq += 1
                continue
            if head["floor"] >= seq:
                # Compaction already covers this number, so our object sits
                # below the floor where readers ignore it. Not durable.
                raise Fenced(f"seq {seq} is at or below floor {head['floor']}")
            self.next_seq += 1
            return seq


def _log_rows(store, seq):
    raw = store.get(log_key(seq))
    if raw is None:
        return None
    return [dict(r, pos=[seq, i]) for i, r in enumerate(json.loads(raw))]


def month_of_name(name):
    return name.split("/")[1]


def level_of_name(name):
    return name.split("/")[2].split("-")[0]


def _segment_name(level, month, lo, hi):
    # The random suffix means two compactors never collide on a name; the
    # loser's file simply becomes an orphan for gc().
    return f"segments/{month}/{level}-{lo:012d}-{hi:012d}-{uuid.uuid4().hex[:8]}"


def compact(store, memory=COMPACT_MEMORY, spill_dir=None):
    """L0 -> L1. Fold the log into one segment per event month, publish, then delete.

    Streaming: rows go through an external sort capped at `memory` bytes that
    spills sorted runs to local disk, and segments are written block by block
    to local files, then uploaded. Memory does not grow with the input.

    Order matters: the floor is published BEFORE the log objects are deleted.
    A stale writer that later reuses a deleted number will see that floor in
    its head check and refuse to acknowledge.
    Returns the new segment names ([] if nothing to do or we lost a race).
    """
    head, tag = read_head(store)
    upto = head_seq(store)
    lo = head["floor"] + 1
    if upto < lo:
        return []
    fmt = head.get("write_format", 1)
    with tempfile.TemporaryDirectory(dir=spill_dir, prefix="compact-") as tmp:
        sorter = ExternalSorter(segment.month_sort_key, memory, tmp)
        for seq in range(lo, upto + 1):
            batch = _log_rows(store, seq)
            if batch is None:
                return []  # Another compactor already folded this range.
            for r in batch:
                sorter.add(r)
        outputs, out, writer, month = [], None, None, None
        for r in dedupe_sorted(sorter.sorted_rows()):
            m = segment.month_of(r.get("t"))
            if m != month:
                if writer:
                    writer.close()
                    out.close()
                month = m
                path = os.path.join(tmp, f"out-{len(outputs)}")
                out = open(path, "wb")
                writer = segment.SegmentWriter(out, fmt, lo, upto)
                outputs.append((_segment_name("L1", month, lo, upto), path))
            writer.add(r)
        if writer:
            writer.close()
            out.close()
        for name, path in outputs:
            store.put_file_if_absent(name, path)
    names = [n for n, _ in outputs]
    new_head = dict(head, floor=upto, segments=head["segments"] + names)
    if not swap_head(store, tag, new_head):
        return []  # The head moved. Our files are orphans; gc() removes them.
    for seq in range(lo, upto + 1):
        store.delete(log_key(seq))
    return names


def merge_files(store, names, spill_dir=None):
    """Merge segments of ONE month into one file. Readers never notice.

    Each input is already sorted, so a heap merge streams them with one block
    per input in memory. The new file takes effect only through a
    compare-and-swap on the head. Old files stay until gc().
    """
    head, tag = read_head(store)
    names = [n for n in names if n in head["segments"]]
    if not names:
        return None
    months = {month_of_name(n) for n in names}
    if len(months) != 1:
        raise ValueError("merge one month at a time")
    footers = [segment.read_footer(store, n) for n in names]
    if any(f is None for f in footers):
        return None
    lo = min(f["lo"] for f in footers)
    hi = max(f["hi"] for f in footers)
    level = "L1" if len(names) == 1 and level_of_name(names[0]) == "L1" else "L2"
    new = _segment_name(level, months.pop(), lo, hi)
    fd, path = tempfile.mkstemp(dir=spill_dir, prefix="merge-")
    try:
        with os.fdopen(fd, "wb") as out:
            writer = segment.SegmentWriter(out, head.get("write_format", 1), lo, hi)
            streams = [segment.iter_rows(store, n, f) for n, f in zip(names, footers)]
            for r in dedupe_sorted(heapq.merge(*streams, key=segment.sort_key)):
                writer.add(r)
            writer.close()
        store.put_file_if_absent(new, path)
    except segment.Missing:
        return None
    finally:
        os.unlink(path)
    kept = [n for n in head["segments"] if n not in names]
    if not swap_head(store, tag, dict(head, segments=kept + [new])):
        return None
    return new


def plan_merges(store):
    """Which months to merge.

    A month with several L1 files and no L2 becomes one L2. A sealed month
    (has an L2) keeps late corrections as small L1 "delta" files that readers
    merge on the fly, and is rewritten only once deltas reach 5% of its rows,
    so a trickle of late data doesn't rewrite a 500 MB file every hour.
    """
    head, _ = read_head(store)
    months = {}
    for n in head["segments"]:
        months.setdefault(month_of_name(n), {"L1": [], "L2": []})[level_of_name(n)].append(n)
    plans = []
    for month, g in sorted(months.items()):
        if not g["L2"]:
            if len(g["L1"]) >= 2:
                plans.append(g["L1"])
            continue
        rows = lambda names: sum((segment.read_footer(store, n) or {"rows": 0})["rows"] for n in names)  # noqa: E731
        if len(g["L2"]) > 1 or (g["L1"] and rows(g["L1"]) >= REWRITE_FRACTION * rows(g["L2"])):
            plans.append(g["L2"] + g["L1"])
    return plans


def merge(store, spill_dir=None):
    """Run every planned merge. Returns the new file names."""
    done = []
    for names in plan_merges(store):
        new = merge_files(store, names, spill_dir)
        if new:
            done.append(new)
    return done


def set_write_format(store, fmt):
    """Flip the format new files use. Deploy readers for fmt everywhere first."""
    if fmt not in segment.SUPPORTED:
        raise segment.UnknownFormat(fmt)
    while True:
        head, tag = read_head(store)
        if swap_head(store, tag, dict(head, write_format=fmt)):
            return


def upgrade_formats(store):
    """Rewrite files whose format differs from write_format, one at a time."""
    head, _ = read_head(store)
    target = head.get("write_format", 1)
    done = []
    for name in head["segments"]:
        footer = segment.read_footer(store, name)
        if footer is not None and footer["format"] != target:
            new = merge_files(store, [name])
            if new:
                done.append(new)
    return done


def gc(store, now=None, grace_s=7 * 86400):
    """Delete orphan segments and log objects already below the floor.

    Deleting is where this design gets hurt (see "deletion can undo a lock"), so
    gc follows the same publish-then-delete rule as compaction:

    A compactor that read the head, then paused, could still be about to swap in
    a segment that gc thinks is an orphan. If gc deleted that file and the swap
    then succeeded, the head would point at nothing while compaction deleted the
    log objects it came from. So gc first bumps head["gc"] by compare-and-swap.
    Any compactor holding the older head now fails its swap and never references
    a deleted file. The grace period protects readers on an older head.
    grace_s <= 0 skips the age check (tests use it for the worst case).
    """
    now = time.time() if now is None else now
    head, tag = read_head(store)
    referenced = set(head["segments"])
    victims = []
    for key in store.list("segments"):
        if key in referenced:
            continue
        if grace_s > 0:
            age = store.modified(key)
            if age is None or now - age < grace_s:
                continue
        victims.append(key)
    victims += [k for k in store.list("log") if seq_of(k) <= head["floor"]]
    if not victims:
        return []
    if not swap_head(store, tag, dict(head, gc=head.get("gc", 0) + 1)):
        return []  # Someone changed the head. Try again next round.
    for key in victims:
        store.delete(key)
    return victims


def consistent_read(store, read, attempts=5):
    """Optimistic read: head -> data -> head again. Retry if the head moved.

    Found by test_serving_while_changing: a reader read the head, compaction
    then folded and DELETED the log tail, and the reader's LIST came back empty.
    Nothing was missing from its point of view, so it silently returned 10
    fewer points. Checking that the head is unchanged afterwards catches every
    such move, because compaction, merge and gc all change the head before they
    delete. Commits never touch the head, so retries are rare.
    """
    for _ in range(attempts):
        head, tag = read_head(store)
        try:
            result = read(head)
        except segment.Missing:
            continue
        if read_head(store)[1] == tag:
            return result
    raise RuntimeError("the store kept changing under the reader")


def tail_rows(store, head, keep=lambda r: True):
    """Rows in log objects above the floor. Raises Missing if one vanished."""
    rows = []
    for key in store.list("log"):
        seq = seq_of(key)
        if seq > head["floor"]:
            batch = _log_rows(store, seq)
            if batch is None:
                raise segment.Missing(key)
            rows.extend(r for r in batch if keep(r))
    return rows


def read_all(store):
    """Everything a reader can see: segments, then log objects above the floor."""
    def read(head):
        rows = []
        for name in head["segments"]:
            rows.extend(segment.iter_rows(store, name))
        return rows + tail_rows(store, head)
    return consistent_read(store, read)
