# Measurements

Measured on 6 Nov 2026 on an Apple-silicon MacBook Air. Linux and Postgres ran
in Docker on the same machine. These numbers show the scale; they are not
production figures. Rerun them with the commands shown.

---

## 1. Linux, under hard memory limits

`./scripts/linux_check.sh` (Linux 6.12, Python 3.12, cgroup v2)

| Run | Memory limit | Result |
|---|---|---|
| Full test suite | 256 MB | 395 passed |
| Compact 400,000 rows, streaming (16 MB sort budget) | 128 MB | Finished in 2.5 s, peak RSS 48 MB |
| Same rows, everything in RAM | 128 MB | **Exit 137, OOM-killed by the kernel** |

**What it shows:** "very little memory" holds against a real kernel limit, not
just Python's own accounting. Without the external sort, the same job is killed.

---

## 2. Storage and reads vs Postgres

`./scripts/postgres_compare.sh` (Postgres 17.6, 1 GB container)
Data: 50 stations × 2 days × 1 report/min × 8 channels = 144,000 reports, 1.15 M points.
Values are random walks rounded to 2 decimals.

| Storage | Bytes/report | At doc scale (TB/year) | One station-day read, p50 |
|---|---|---|---|
| Archive segments (JSON + zlib, keeps every version) | 92 | 4.9 | 32 ms |
| Postgres, 1 row per report, 8 columns (the doc's comparison) | 126 | 6.6 | 2 ms |
| Postgres, 1 row per point + rev + arrived (the archive's data model) | 1,088 | 57.2 | 10 ms |
| Raw 64-byte binary report (doc baseline) | 64 | 3.4 | – |

"At doc scale" = 100,000 stations × 525,600 reports/year.

### What this means for the design doc (honest reading)

1. **The doc's storage argument is not proven yet.** The doc says Postgres
   needs ~4.8 TB/year against 420 GB for the archive (about 11×). Measured:
   Postgres is **6.6 TB/year** (the doc underestimated it), but the prototype's
   segments are **4.9 TB/year**, only 1.4× smaller. The doc's 420 GB depends
   on Gorilla + zstd reaching 8:1, and that encoding is **not built**: the
   prototype stores JSON + zlib, which is bigger than raw binary.
2. **For the archive's actual data model, the gap is real.** Keeping every
   version with its revision and arrival time per point (needed for
   corrections and as-of queries) costs Postgres 1,088 B/report vs 92: about
   **12× bigger**.
3. **Postgres reads faster on one machine.** 2 ms vs 32 ms for one
   station-day. The archive's time is Python decoding JSON, not the design,
   but it's a real gap until the format is binary and the reader isn't Python.
4. **So the strongest reasons for the archive are operational, not size:**
   1 GB spot machines that vanish don't suit a database that needs a stable
   disk, and object storage gives durability without replicas. Say this
   instead of "Postgres is 11× bigger".
5. **Not tested:** TimescaleDB native compression, the fairest database
   competitor. If it reaches the doc's compression ratios, the build-vs-buy
   decision should be revisited.

---

## 3. Similarity search vs pgvector

100,000 × 128 float32 vectors, top-10 by cosine, 50 queries for recall.

| Method | p50 ms | p99 ms | Recall@10 | Size | Build |
|---|---|---|---|---|---|
| **numpy flat scan (prototype)** | **0.78** | **1.01** | **1.00 (exact)** | 49 MB | none |
| pgvector exact (no index) | 6.2 | 9.8 | 1.00 | 58 MB table | none |
| pgvector HNSW, ef_search=40 (default) | 0.64 | 0.77 | 0.49 | +82 MB index | 8 s |
| pgvector HNSW, ef_search=200 | 1.51 | 2.10 | 0.66 | +82 MB index | 8 s |
| pgvector HNSW, ef_search=800 | 4.76 | 13.81 | 0.88 | +82 MB index | 8 s |

### What this means

- **The doc's "flat scan, no index" decision is supported by measurement.**
  At 100,000 vectors, the exact flat scan is as fast as HNSW's fastest setting
  and needs no index, no extra 82 MB, no build step, and never misses.
- HNSW only wins when exact scanning gets too slow, i.e. millions of vectors
  (the doc's 12 M "one vector per month" case).
- **Caveat:** uniform random vectors are HNSW's worst case. Real fingerprints
  cluster, so HNSW recall on them would be higher. The exact-vs-exact
  comparison (numpy 0.78 ms vs pgvector 6.2 ms) is not affected by this.
- Container shared memory: Docker's default 64 MB `/dev/shm` made the parallel
  HNSW build fail ("No space left on device"). Postgres in containers needs
  `--shm-size`, a real operational gotcha.
