"""Step 4: the gateway and the per-shard ingest buffer.

Brief: "runs on very little memory" and "acknowledged data must never be lost".

  report -> validate (400 on bad input)
         -> route by station hash to one of N log shards
         -> buffer for up to ~1 s, hard-capped in bytes (429 when full)
         -> one commit per shard per flush; only then acknowledge

Every buffer is bounded, so overload slows stations down instead of growing
memory until the OOM killer ends the process.
"""
import hashlib
import json
import time

from .records import validate

NUM_SHARDS = 4


def shard_for(station, shards=NUM_SHARDS):
    """Stable routing. Python's hash() changes per process, so it cannot route."""
    return int.from_bytes(hashlib.sha256(station.encode()).digest()[:8], "big") % shards


class Overloaded(Exception):
    """HTTP 429: the buffer is full. The station keeps its data and retries."""


class ShardBuffer:
    def __init__(self, writer, cap_bytes=64 * 1024 * 1024, clock=time.time):
        self.writer, self.cap, self.clock = writer, cap_bytes, clock
        self.pending, self.bytes = [], 0

    def room_for(self, size):
        return self.bytes + size <= self.cap

    def add(self, points, size):
        if not self.room_for(size):
            raise Overloaded(f"buffer at {self.bytes}/{self.cap} bytes")
        now = int(self.clock())
        self.pending.extend(dict(p, arrived=now) for p in points)
        self.bytes += size

    def flush(self):
        """Commit everything buffered. Returns the sequence number, or None if empty.

        If commit raises (Fenced, store down, crash), the batch is dropped and was
        never acknowledged, so the stations resend it. A resend that duplicates a
        batch which did land collapses in dedupe().
        """
        if not self.pending:
            return None
        batch, self.pending, self.bytes = self.pending, [], 0
        return self.writer.commit(batch)


class Gateway:
    """Stateless front door. writers[i] is the LogWriter owning shard i."""

    def __init__(self, writers, cap_bytes=64 * 1024 * 1024, clock=time.time):
        self.buffers = [ShardBuffer(w, cap_bytes, clock) for w in writers]

    def submit(self, reports):
        """Validate and buffer. Raises Invalid (400) or Overloaded (429).

        All-or-nothing: either every report is buffered or none is.
        """
        points = [validate(r) for r in reports]
        by_shard = {}
        for p in points:
            by_shard.setdefault(shard_for(p["station"], len(self.buffers)), []).append(p)
        sizes = {s: sum(len(json.dumps(p)) for p in ps) for s, ps in by_shard.items()}
        for s, size in sizes.items():
            if not self.buffers[s].room_for(size):
                raise Overloaded(f"shard {s} is full")
        for s, ps in by_shard.items():
            self.buffers[s].add(ps, sizes[s])
        return sorted(by_shard)

    def flush(self):
        """Commit every shard. Returns {shard: seq}; a shard that failed is absent."""
        acked = {}
        for s, buf in enumerate(self.buffers):
            try:
                seq = buf.flush()
            except Exception:
                continue  # Not acknowledged; senders retry.
            if seq is not None:
                acked[s] = seq
        return acked
