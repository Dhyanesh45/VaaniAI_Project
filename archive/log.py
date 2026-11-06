"""One log shard: the commit rule, takeover, compaction and reads."""
import json

from .store import etag

HEAD = "head"


class Fenced(Exception):
    """This writer no longer owns the log. It must stop and must not acknowledge."""


def log_key(seq):
    return f"log/{seq:012d}"


def init_log(store):
    head = {"epoch": 0, "floor": 0, "segments": []}
    store.put_if_absent(HEAD, json.dumps(head).encode())


def read_head(store):
    raw = store.get(HEAD)
    return json.loads(raw), etag(raw)


def head_seq(store):
    keys = store.list("log")
    return int(keys[-1].split("/")[1]) if keys else 0


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

        YOUR TASK. Follow the design doc's commit rule:

        1. Encode the batch: json.dumps(records).encode()
        2. Try store.put_if_absent(log_key(self.next_seq), data).
        3. Read the head with read_head(self.store).
        4. If the head's epoch differs from self.epoch, raise Fenced.
        5. If the PUT returned False, you still own the log, so a stale writer
           took this number. Add 1 to self.next_seq and go back to step 2.
        6. If the head's floor is at or above this sequence number, raise Fenced.
        7. Otherwise save the sequence number, add 1 to self.next_seq, return it.
        """
        raise NotImplementedError("Step 1: write the commit rule")


def compact(store):
    """Fold the log into one segment, publish the floor, then delete old objects."""
    head, tag = read_head(store)
    upto = head_seq(store)
    if upto <= head["floor"]:
        return
    records = []
    for seq in range(head["floor"] + 1, upto + 1):
        records.extend(json.loads(store.get(log_key(seq))))
    name = f"segments/{head['floor'] + 1:012d}-{upto:012d}"
    store.put_if_absent(name, json.dumps(records).encode())
    new_head = dict(head, floor=upto, segments=head["segments"] + [name])
    if not store.swap(HEAD, tag, json.dumps(new_head).encode()):
        return  # The head moved. Leave the log alone and try again later.
    for seq in range(head["floor"] + 1, upto + 1):
        store.delete(log_key(seq))


def read_all(store):
    """Everything a reader can see: segments, then log objects above the floor."""
    head, _ = read_head(store)
    records = []
    for name in head["segments"]:
        records.extend(json.loads(store.get(name)))
    for key in store.list("log"):
        if int(key.split("/")[1]) > head["floor"]:
            data = store.get(key)
            if data is not None:
                records.extend(json.loads(data))
    return records
