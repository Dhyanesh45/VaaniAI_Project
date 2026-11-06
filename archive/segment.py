"""Step 3: the segment file that compaction writes.

Brief: "very little memory" and "keep serving while its data and formats change".

One segment holds one calendar month (by EVENT time) of one shard, sorted by
station. Each station's rows are split into blocks of at most BLOCK_ROWS, and
the blocks sit next to each other, so one ranged GET reads one station-month.

    [MAGIC][fmt] [st-1 block][st-1 block][st-2 block]... [footer JSON] [footer_len u32][fmt u8][MAGIC]

The footer maps station -> [[offset, length, rows], ...]. The trailer sits at a
fixed distance from the end, so a reader fetches the last 64 KB (S3
`Range: bytes=-65536`) and usually gets the whole footer in one request.

Format versions (every file records its own):
  1  block = zlib(JSON rows)
  2  block = zlib(JSON) with timestamps delta-of-delta encoded
Readers accept both, which is how a format change ships without downtime.
Production would add Gorilla XOR coding for values and use zstd.
"""
import calendar
import io
import json
import struct
import time
import zlib

MAGIC = b"ARC1"
TRAILER = struct.Struct(">IB4s")  # footer length, format version, magic
SUPPORTED = {1, 2}
TAIL_GUESS = 64 * 1024
BLOCK_ROWS = 4096  # caps memory per block for readers and writers


class UnknownFormat(Exception):
    """A file written by a newer release. Deploy readers before writers."""


class Missing(Exception):
    """A file vanished mid-read (compaction or gc). The caller re-reads the head."""


def month_of(t):
    return time.strftime("%Y-%m", time.gmtime(t if isinstance(t, int) and not isinstance(t, bool) else 0))


def month_bounds(month):
    y, m = map(int, month.split("-"))
    start = calendar.timegm((y, m, 1, 0, 0, 0))
    end = calendar.timegm((y + (m == 12), m % 12 + 1, 1, 0, 0, 0))
    return start, end


def sort_key(r):
    return (str(r.get("station", "")), str(r.get("channel", "")),
            r.get("t") or 0, tuple(r.get("pos", (0, 0))))


def month_sort_key(r):
    return (month_of(r.get("t")),) + sort_key(r)


def _dod(ts):
    out, prev, prev_delta = [], 0, 0
    for i, t in enumerate(ts):
        if i == 0:
            out.append(t)
        else:
            delta = t - prev
            out.append(delta - prev_delta)
            prev_delta = delta
        prev = t
    return out


def _undod(xs):
    if not xs:
        return []
    out, delta = [xs[0]], 0
    for x in xs[1:]:
        delta += x
        out.append(out[-1] + delta)
    return out


def encode_block(rows, fmt):
    if fmt == 1:
        body = rows
    elif fmt == 2:
        ts = [r.get("t") for r in rows]
        if all(isinstance(t, int) and not isinstance(t, bool) for t in ts):
            body = {"t": _dod(ts), "rows": [{k: v for k, v in r.items() if k != "t"} for r in rows]}
        else:
            body = {"t": None, "rows": rows}
    else:
        raise UnknownFormat(fmt)
    return zlib.compress(json.dumps(body, separators=(",", ":")).encode(), 6)


def decode_block(raw, fmt):
    body = json.loads(zlib.decompress(raw))
    if fmt == 1:
        return body
    if fmt == 2:
        if body["t"] is None:
            return body["rows"]
        return [dict(r, t=t) for r, t in zip(body["rows"], _undod(body["t"]))]
    raise UnknownFormat(fmt)


class SegmentWriter:
    """Streams rows, already sorted by station, into a file one block at a time.
    Memory: one block (<= BLOCK_ROWS rows) plus the footer index."""

    def __init__(self, f, fmt, lo, hi, block_rows=BLOCK_ROWS):
        self.f, self.fmt, self.lo, self.hi, self.block_rows = f, fmt, lo, hi, block_rows
        header = MAGIC + bytes([fmt])
        f.write(header)
        self.offset, self.index, self.pending, self.station, self.rows = len(header), {}, [], None, 0

    def add(self, row):
        station = str(row.get("station", ""))
        if station != self.station:
            self._flush()
            if station in self.index:
                raise ValueError("rows must arrive sorted by station")
            self.station = station
        self.pending.append(row)
        if len(self.pending) >= self.block_rows:
            self._flush()

    def _flush(self):
        if not self.pending:
            return
        blob = encode_block(self.pending, self.fmt)
        self.f.write(blob)
        self.index.setdefault(self.station, []).append([self.offset, len(blob), len(self.pending)])
        self.offset += len(blob)
        self.rows += len(self.pending)
        self.pending = []

    def close(self):
        self._flush()
        footer = json.dumps({"format": self.fmt, "lo": self.lo, "hi": self.hi, "rows": self.rows,
                             "stations": self.index}, separators=(",", ":")).encode()
        self.f.write(footer)
        self.f.write(TRAILER.pack(len(footer), self.fmt, MAGIC))
        return self.rows


def build(rows, fmt, lo, hi):
    """Serialise rows into one segment in memory (small inputs and tests)."""
    buf = io.BytesIO()
    w = SegmentWriter(buf, fmt, lo, hi)
    for r in sorted(rows, key=sort_key):
        w.add(r)
    w.close()
    return buf.getvalue()


def _parse_tail(tail):
    flen, fmt, magic = TRAILER.unpack(tail[-TRAILER.size:])
    if magic != MAGIC:
        raise ValueError("not a segment file")
    if fmt not in SUPPORTED:
        raise UnknownFormat(fmt)
    return flen + TRAILER.size


def read_footer(store, name):
    """One ranged GET, two if the footer is larger than the guess. None if missing."""
    tail = store.get_range(name, -TAIL_GUESS)
    if tail is None:
        return None
    need = _parse_tail(tail)
    if need > len(tail):
        tail = store.get_range(name, -need)
        if tail is None:
            return None
    return json.loads(tail[-need:-TRAILER.size])


def read_station(store, name, footer, station):
    """One station's rows with a single ranged GET. None if the file vanished."""
    blocks = footer["stations"].get(str(station))
    if not blocks:
        return []
    start = blocks[0][0]
    end = blocks[-1][0] + blocks[-1][1]
    raw = store.get_range(name, start, end - start)
    if raw is None:
        return None
    rows = []
    for offset, length, _ in blocks:
        rows.extend(decode_block(raw[offset - start:offset - start + length], footer["format"]))
    return rows


def iter_rows(store, name, footer=None):
    """Every row, in sorted order, one block in memory at a time. Raises Missing."""
    footer = footer or read_footer(store, name)
    if footer is None:
        raise Missing(name)
    for blocks in footer["stations"].values():
        for offset, length, _ in blocks:
            raw = store.get_range(name, offset, length)
            if raw is None:
                raise Missing(name)
            yield from decode_block(raw, footer["format"])


def read_rows(store, name):
    """Every row as a list. None if missing."""
    try:
        return list(iter_rows(store, name))
    except Missing:
        return None
