# Walkthrough: how the prototype works and why

Read this top to bottom once, then run `python -m archive.demo` and match each
section of its output to the section here.

---

## 0. The brief, sentence by sentence

| The brief says | What that forces | Where it lives |
|---|---|---|
| "Stations report regularly, until they don't" | Silence is data: track last-seen, never store zeros | `query.gaps`, `query.status` |
| "late, out of order, duplicated, or corrected" | Key by event time, version by revision, collapse exact duplicates | `records.py` |
| "one station's history" | Data sorted by station so one station is one small read | `segment.py` footer + ranged GET |
| "the stations that behave [most like a given one]" * | A behaviour fingerprint per station + nearest-neighbour search | `similarity.py` |
| "very little memory" | Every buffer and cache has a byte cap; overload returns 429 | `ingest.ShardBuffer`, `query.FooterCache` |
| "machines that can disappear at any moment" | No durable local state. Object storage is the only truth | `log.py` (everything goes through the store) |
| "Acknowledged data must never be lost" | Acknowledge only after the write is durable AND ownership is confirmed | `LogWriter.commit`, `model_check.py` |
| "keep serving while its data and formats change" | Readers understand old and new formats; switch by compare-and-swap | `segment.SUPPORTED`, `set_write_format`, `upgrade_formats` |

\* The brief's sentence is garbled ("behave I like it the most"). I read it as
similarity search. Say so explicitly in the design conversation. Naming an
ambiguity and choosing an interpretation is part of what they're testing.

---

## 1. The one idea everything rests on

**Object storage is the database.** S3 survives machine loss and is strongly
consistent. It also supports two conditional writes:

- `If-None-Match: *` → "create only if nothing is there" (`put_if_absent`)
- `If-Match: <etag>` → "replace only if nobody changed it since I read it" (`swap`)

With those two operations you get a log and a lock without running etcd,
ZooKeeper or a database. Nodes are just compute with caches, so losing one costs
a pause and never data.

The **head** object (`log.py` docstring) is the shard's single source of truth:
`epoch` (who may write), `floor` (what's compacted), `segments` (what readers
read), `write_format`, `gc`. Every change to it is a compare-and-swap.

---

## 2. Step 1: the commit rule (`log.py`, `LogWriter.commit`)

```
PUT log/<seq> with If-None-Match: *   → write the batch, never overwrite
GET head                              → am I still the owner?
  epoch changed?        → Fenced: stop, do NOT acknowledge
  PUT got 412?          → a stale writer took this number; try seq+1
  floor >= seq?         → our slot was compacted and deleted; not visible, Fenced
  otherwise             → acknowledge seq
```

**Why read the head after the PUT and not before?** If you check first, a
takeover can happen between the check and the write, and you'd acknowledge
something the new owner doesn't know about. Checking after means that if the
epoch is still yours, no takeover happened before your write landed.

**"Deletion can undo a lock."** `If-None-Match: *` means "nothing is here
*now*", not "nothing was *ever* here". Compaction deletes log objects, so
a number can become free again. Compaction publishes the floor *before* it
deletes, so a writer whose PUT landed in a deleted slot will see that floor
when it reads the head.

**Takeover** (`take_over`): raise the epoch by compare-and-swap, then LIST the
log to find the end. Heartbeats decide who *should* take over. Only
compare-and-swap decides who *may*. A paused VM can't be trusted to know how
long it slept, so clocks are never used for safety.

---

## 3. Step 2: messy data (`records.py`)

Every point has a **key** `(station, channel, t)` and a **version**
`(rev, log position)`.

| Arrival | Rule | Code |
|---|---|---|
| Late / out of order | Placed by event time `t`, never by arrival order | `resolve` sorts by `t` |
| Exact duplicate | Same key + rev + value hash → one copy | `dedupe` |
| Correction | Higher `rev` wins; old versions kept | `version()` + `resolve` |
| Original after its correction | Lower rev stored but never wins | same |
| No rev from source | Later log position wins, flagged `weak` | `resolve` |
| Silence | Nothing is stored; a gap is absence | `validate` rejects `v: null` |

**As-of queries.** Every point also has an `arrived` time (stamped by the
gateway). `resolve(rows, as_of=X)` ignores versions that arrived after X. That
answers "what did we *know* on 1 March?" as well as "what *happened*?".
(Arrival time is wall-clock, which is fine for answering a question, but it's
never used for safety.)

**Why keep old versions?** Audits, as-of queries, and repairing a bad
correction all need them.

---

## 4. Step 3: storage (`segment.py`, `log.py`)

**Levels:** L0 log (arrival order) → `compact()` → L1 segments, one per
**event month**, sorted by station → `merge()` → one L2 file per month.

```
segments/2026-01/L2-000000000001-000000000031-a1b2c3d4     ← January, merged
segments/2026-01/L1-000000000040-000000000040-9f8e7d6c     ← a late correction for January
segments/2026-02/L1-000000000032-000000000039-5a4b3c2d     ← February, still filling
```

- **Partitioned by event time, not arrival time.** A correction for January
  that arrives in June goes into `2026-01/`. A query for March reads only
  March's files, so one station's year is exactly 12 block reads
  (`test_one_station_year_is_12_small_reads`).
- **The 5% rule (`plan_merges`).** A sealed month keeps late data as small L1
  "delta" files that readers merge on the fly. The month's L2 is rewritten only
  when deltas reach 5% of its rows, so a trickle of late data doesn't rewrite
  a 500 MB file every hour.

**Segment layout:**
```
[magic][fmt] [st-1 block][st-1 block][st-2 block] … [footer JSON] [footer_len][fmt][magic]
```
The footer maps `station → [[offset, length, rows], …]`. A station's blocks
(≤ 4,096 rows each) sit next to each other, so reading one station is: fetch
the last 64 KB (trailer + footer, usually one GET), then **one** ranged GET
covering that station's blocks. You never download the whole file.

**Streaming compaction (`spill.py`): "very little memory".** Compaction sorts
data that can be far bigger than RAM:
1. Buffer rows (as compact JSON strings) until the memory cap is reached.
2. Sort that chunk and write it to local disk as a "run".
3. At the end, heap-merge all runs, reading each line by line.
4. Write the segment block by block to a local file, then upload it.

Measured (`test_compaction_memory_stays_flat_as_input_doubles`): with a 1 MB
cap, compacting 2× the data uses about the same memory, under half of the
in-RAM version. On 120,000 rows: 68 MB in RAM vs 7.6 MB streaming. Merge also
streams: one block per input file. Local disk is scratch only. If the machine
dies, the job restarts from the log.

**Compression:** format 2 encodes timestamps delta-of-delta (regular reports
→ runs of zeros). The demo shows 8.4:1, **but that's against bloated JSON**,
so it says nothing about the doc's 8:1 against 64-byte binary reports. Don't
quote it as evidence.

**Format changes while serving** (`test_format_change_while_serving`):
1. Ship readers that understand formats 1 and 2 (`SUPPORTED`).
2. `set_write_format(store, 2)`: compare-and-swap on the head. New files use 2.
3. `upgrade_formats()` rewrites old files one at a time.
4. Rollback = flip the field back. A reader that meets an unknown format raises
   `UnknownFormat` instead of misreading bytes.

**Compaction safety:** a new file takes effect only through a compare-and-swap
on the head. Two compactors can both finish, but only one swap wins, and the
loser's file is an orphan. Segment names have a random suffix so they never
collide.

**Garbage collection (`gc`): a new finding, not in the design doc.** A
compactor reads the head, writes its segment, and pauses. gc sees an
unreferenced file and deletes it. The compactor wakes, its swap *succeeds*
(the head hadn't changed), and it deletes the log objects. Now the head points
at a deleted file, and the data is gone. It's the same "deletion can undo a
lock" trap a second time. Fix: gc bumps `head.gc` by compare-and-swap *before*
deleting, so any compactor holding an older head fails its swap. Test:
`test_gc_cannot_delete_a_file_a_paused_compactor_is_about_to_publish`.

---

## 5. Step 4: ingest (`ingest.py`)

```
validate (400) → route by sha256(station) % shards → buffer (byte cap → 429) → commit → ack
```
- **Stable hash:** Python's `hash()` is randomised per process, so it can't route.
- **All-or-nothing submit:** a request is either fully buffered or rejected.
- **Bounded memory:** the 64 MB cap means overload slows stations down
  (they buffer and retry) instead of the OOM killer ending the process.
- **Failure = no ack:** if commit raises (fenced, store down), the batch is
  dropped *unacknowledged*. The station resends, and if the first copy actually
  landed, `dedupe` collapses the duplicate. This is why duplicates are a
  normal, expected case.

---

## 6. Step 5: reads (`query.py`)

`history(store, station, start, end, as_of)`:
1. Read the head.
2. For each segment, read the footer (from `FooterCache`), then one ranged GET.
3. Add log objects above the floor (recent, not yet compacted).
4. `resolve` → one current value per point.

Months outside `[start, end)` are skipped without any request.

**Optimistic read (`log.consistent_read`): a bug found by testing.**
`test_serving_while_changing.py` lets compaction, merge and gc strike at
*every* one of the reader's storage calls. It caught this:
```
1. reader reads head          → "segments: [S1], floor: 2"
2. ── compaction runs ──         folds log/3, log/4 into S2, deletes them
3. reader LISTs log/           → empty
4. reader returns S1 only      → 10 points missing, no error
```
"Retry if a file is missing" doesn't help: a file deleted *before* the LIST
isn't missing, it's simply not listed. Fix: read head → read data → **read head
again; if it changed, start over.** Compaction, merge and gc all change the head
before deleting anything, so any move is detected. Commits never touch the
head, so retries are rare. This is the same pattern as a seqlock in an
operating system kernel.

`FooterCache` is an LRU capped in bytes. Files are written once and never
change, so cache entries never need invalidating. That's a big simplification
immutability buys you.

`status`: `reporting` (≤ 2 cadences), `late` (≤ 60), `silent` (beyond). These
thresholds are my choice. In production they'd be per station.

---

## 7. Step 6: similar stations (`similarity.py`)

**Fingerprint = 128 numbers describing *how* a station behaves:**
- per channel (8 slots × 12): skew, kurtosis, outlier rate, IQR, daily
  Fourier amplitudes (1, 2, 3 cycles/day), yearly amplitude, autocorrelation
  at 1 h / 6 h / 24 h / 168 h
- 28 cross-channel correlations
- 4 reporting features: gap rate, mean gap, longest gap, burstiness

Gaps are skipped, never zero-filled. Fourier amplitudes are computed over
observed hours only. Under 30 days of data → `NotEnoughHistory` instead of a
confident bad answer.

**Two findings from testing (both should go into the design doc):**
1. **Robust scaling (median/MAD, clip at ±4).** With mean/std, one bad
   99 °C correction moved a coastal station next to desert stations, because
   the outlier stretched the scale and flattened the daily rhythm.
2. **Standardise each feature across all stations** before the cosine. Raw,
   coast and desert scored 0.9 alike: the feature that separates them
   (temp↔humidity correlation sign) is 1 of 128 dims and was drowned out by
   features every station shares. The per-dimension mean/std belongs to the
   snapshot, which is another reason vectors from different snapshots
   can't be compared.

**Measured quality** (synthetic, 36 stations): precision@5 ≈ 98%, top-1
correct for 35/36 (the miss is a 0.64 vs 0.64 near-tie). The tests assert a
quality bar, not perfection, because similarity is statistical.

**Search:** flat scan, `vectors @ query`, filtered to the same channel set.
100,000 × 128 float32 = 51.2 MB. Measured here: 1.2 ms p50 / 1.5 ms p99 with
2 threads on an Apple-silicon Mac (the doc's 3.1 / 4.9 ms were on a 2 vCPU
cloud VM). Always say which hardware a number came from.

**Freshness:** `publish()` writes a new snapshot, then moves
`fingerprints/CURRENT` by compare-and-swap. `load()` copies it to local disk
and mmaps the vectors. That's fine because the file is read-only and fits in
memory (Crotty et al.'s acceptable case). A query holding the old `Snapshot`
object keeps working.

---

## 8. Step 7: the model checker (`model_check.py`)

Breadth-first search over every interleaving of a stale writer A, a new owner
B and one compaction C. Each state checks: *every acknowledged batch is
visible.*

```
$ python -m archive.model_check
put_only: 151 states explored, UNSAFE
   1. B takes over: epoch 1 -> 2
   2. B lists log: next seq 2
   3. A PUT seq 2 ok -> ack
   4. C reads head
   5. C reads log up to 2
   6. C swaps head: floor -> 2
   7. C deletes log 1..2
   8. B PUT seq 2 ok -> ack            ← B's data sits below the floor: lost
read_after_put: 437 states explored, safe
```

Two things to correct in the design doc:
- It says **"30 reachable states"**. This model has **437** under the safe rule.
  Use the number your actual model produces, and describe its scope (one stale
  writer, one new owner, one compaction, seq ≤ 4).
- The shortest failure under PUT-only hurts the **new owner**, not just the
  stale writer. Even the rightful owner must check the head after the PUT.

It's still evidence for a small case, not a proof (as the doc says; TLA+ next).

---

## 9. Other corrections and open points for the design doc

1. **Footer cache vs 10 years.** 64 partitions × 12 months × 10 years × 50 KB
   ≈ 384 MB of footers, but the cache is 128 MB. Cold reads of old years add a
   footer round trip: head → footer → block. That's three sequential GETs
   inside the 500 ms p99 target. Plausible, but tight, so say so.
2. **Moving a station to another shard** breaks "log position breaks ties",
   because positions in two shards can't be compared. You need a migration
   barrier (e.g. a routing epoch stored with the position).
3. **Go services + SlateDB:** SlateDB is written in Rust. Pick one: Rust
   services, Go via bindings/FFI, or your own log in Go (this prototype shows
   it's ~60 lines of logic). Expect this question, since the role is Go/Rust.
4. **Raw archive in cold storage** isn't priced, and deep-archive retrieval
   takes hours, which sets your real "rebuild a month" time.
5. **Memory budget table:** say it's for ingest/query nodes. The compactor's
   256 MB merge budget is on a separate node.
6. **Acknowledged ⊆ visible, not equal.** A fenced writer's batch can land
   above the floor and be visible without ever being acknowledged. That's safe
   because the sender resends and `dedupe` collapses the copy.
7. **Readers need an optimistic read.** The doc's reader rule ("a missing log
   object means refetch the head and retry once") is not enough. Add: re-read
   the head after reading, and retry if it changed (section 6).
8. **S3 compatibility is not universal.** MinIO's public images are gone
   from Docker Hub and quay.io, and older MinIO releases (e.g. 2020) predate
   conditional writes. Before choosing any S3-compatible store, run the
   conditional-write probe: create-only returns 412, stale `If-Match` returns 412.
   LocalStack 4.4 and moto 5.2 pass.
9. **Postgres storage claim (measured, see BENCHMARKS.md).** The doc says
   Postgres ~4.8 TB/year vs 420 GB. Measured: Postgres 6.6 TB/year, but the
   prototype's JSON+zlib segments are 4.9 TB/year. The 420 GB needs Gorilla +
   zstd, which is not built. Lead with the operational argument instead, and
   note that for the version-keeping data model Postgres is ~12× bigger.
10. **Flat scan vs pgvector (measured).** numpy exact 0.78 ms vs pgvector exact
   6.2 ms, and HNSW at the same speed only reaches 0.49 recall. This supports
   the doc's "no index" decision.
11. **"What I learned"** in the doc must be in your own words.

---

## 10. What is deliberately NOT built

| Not built | Why it's fine for a prototype | Production |
|---|---|---|
| Real AWS S3 | All 395 tests pass against an S3 API (LocalStack) via `s3store.py` | Point `s3store.py` at AWS |
| Heartbeats / leader detection | Safety never depends on them | Heartbeat object per shard, 30 s staleness |
| HTTP server | Logic is the same | Gateway returning 400/429/200 |
| 64 partitions per shard | One shard = one partition; months are partitioned | Partition key in segment names |
| Gorilla XOR + zstd | zlib + delta-of-delta shows the idea | As in the doc |
| Multipart upload | Segments upload in one streamed PUT | Multipart for files > 5 GB |
| gc grace by LastModified across readers | `grace_s` parameter exists | 7 days |

---

## 11. Questions to be ready for

- *Why not Postgres/TimescaleDB?* Stable disks and nodes don't fit 1 GB spot
  machines that vanish. One row per report ≈ 4.8 TB/year before indexes.
  Reverse if a managed service fits the budget.
- *Why not Raft?* Replicas live on machines that can all vanish together.
  Object storage is already replicated.
- *What if S3 is down?* Writes stop acknowledging, cached reads continue.
  Nothing acknowledged is lost.
- *Why 1 s commits?* PUT cost scales with commit rate: $52/month vs $207 at
  250 ms. Sensors on 1-minute cadence don't need faster.
- *Why flat scan, not HNSW/pgvector?* 51 MB and ~ms. A graph index adds memory
  and rebuild work for no gain at 100k. Reverse at many vectors per station
  (12 M vectors = 6.1 GB).
- *What's your least-sure assumption?* One fingerprint per station. Stations
  change (sensor swap, site move), and the failure is silent: confident,
  useless neighbours. Test: split each station's history in half, and each
  half should find its twin.
