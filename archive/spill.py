"""External sort: sort more rows than fit in memory.

Brief: "runs on very little memory".

Compaction must sort an hour (or a month) of data by station, which can be far
bigger than RAM. So:
  1. Collect rows until the memory budget is reached.
  2. Sort that chunk and write it to local disk as a "run".
  3. Repeat until the input ends.
  4. Merge all runs with a heap, reading each one line by line.
Memory is the budget plus one row per run, whatever the input size. Local SSD
is only scratch space here: if the machine dies, the job restarts from the log.
"""
import heapq
import json
import os

ROW_OVERHEAD = 250  # bytes of Python objects per buffered row, measured roughly


class ExternalSorter:
    def __init__(self, key, cap_bytes, tmpdir):
        self.key, self.cap, self.tmpdir = key, cap_bytes, tmpdir
        self.buf, self.used, self.runs = [], 0, []

    def add(self, row):
        # Keep a compact JSON string, not the dict, while buffering.
        line = json.dumps(row, separators=(",", ":"))
        self.buf.append((self.key(row), line))
        self.used += len(line) + ROW_OVERHEAD
        if self.used >= self.cap:
            self._spill()

    def _spill(self):
        if not self.buf:
            return
        self.buf.sort(key=lambda e: e[0])
        path = os.path.join(self.tmpdir, f"run-{len(self.runs):05d}")
        with open(path, "w") as f:
            for _, line in self.buf:
                f.write(line)
                f.write("\n")
        self.runs.append(path)
        self.buf, self.used = [], 0

    @staticmethod
    def _read(path):
        with open(path) as f:
            for line in f:
                yield json.loads(line)

    def sorted_rows(self):
        if not self.runs:  # everything fit in memory: no disk needed
            self.buf.sort(key=lambda e: e[0])
            for _, line in self.buf:
                yield json.loads(line)
            return
        self._spill()
        yield from heapq.merge(*(self._read(p) for p in self.runs), key=self.key)
