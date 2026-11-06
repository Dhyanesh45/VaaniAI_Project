# The Archive: a prototype for Vaani's backend design challenge

A log-structured archive for sensor data whose only durable store is object
storage (S3). Machines are disposable, memory is tight, and acknowledged data
is never lost. This repository is the runnable companion to my design write-up:
it backs up the design's riskiest claims with code, tests and measurements.

---

## The challenge

> **The Archive**
>
> A global network of sensor stations has been recording measurements
> for years, and the archive keeps growing. Stations report regularly,
> until they don't. Data can arrive late, out of order, duplicated, or
> corrected long after the fact.
>
> People ask for one station's history, and for the stations that behave
> I like it the most.
>
> The system runs on very little memory, on cheap cloud machines that
> can disappear at any moment. Acknowledged data must never be lost, and
> the system must keep serving while its data and formats change.
>
> We've left out the details on purpose. Decide what matters, choose
> your own numbers, and make the calls a real engineer would have to
> make. Your thinking may touch storage engines, databases, vector
> search, operating systems, concurrency and cloud deployment, but
> follow wherever the problem takes you.
>
> In up to 3 pages, tell us what you'd build and why. Tell us which
> assumption you're least sure of, and what breaks if it's wrong. Some
> of this will be new to you, so tell us what you learned and teach us
> the most interesting part.

The sentence "the stations that behave I like it the most" looks garbled. I
read it as **"the stations that behave most like a given station"**, which is
a similarity search, and built that.

---

## Why this problem interested me

Before this challenge I had read about **Senzomatic**, a European startup that
puts sensors inside wooden buildings to track the condition of the timber:
whether it is degrading or at risk of breaking, so that problems are caught
early and warranties can be managed. This applies mainly where timber
construction is common, such as Europe, the US, Japan and China.

That is very close to this challenge: thousands of sensors reporting for years,
stations going quiet, late or corrected readings, and the question "which
other buildings behave like this one?" It made the problem concrete for me, and
the design choices here (never losing a reading, handling corrections,
comparing stations by behaviour) are the ones such a product would depend on.

---

## What I built

| Step | File | What it shows |
|---|---|---|
| 1 | `archive/log.py` `commit` | The commit rule: write, then confirm ownership, then acknowledge |
| 2 | `archive/records.py` | Late, out-of-order, duplicated and corrected data; "what did we know on date X?" queries |
| 3 | `archive/segment.py`, `log.py`, `spill.py` | Monthly files with a per-station index, compaction under a memory cap, format changes without downtime, safe garbage collection |
| 4 | `archive/ingest.py` | Validation, routing to shards, bounded buffers, `429` when overloaded |
| 5 | `archive/query.py` | One station's history: reads only the months and bytes it needs |
| 6 | `archive/similarity.py` | Stations that behave alike: 128-number fingerprints and a flat cosine search |
| 7 | `archive/model_check.py` | Checks every possible ordering of the commit race |
| – | `archive/store.py`, `archive/s3store.py` | The same storage interface for a local folder and for S3 |

`WALKTHROUGH.md` explains every part and why it is built that way.
`BENCHMARKS.md` has the measurements, including where they disagree with my
original design assumptions.

### Key results

| Claim | Result |
|---|---|
| Acknowledged data is never lost | Safe in all 437 states of the model check and 300 random crash timings; the naive rule loses data in 8 steps |
| Very little memory | On Linux with a 128 MB limit, streaming compaction finishes at 48 MB; the in-memory version is killed by the kernel |
| One station's history | One station's year = 12 small ranged reads |
| Keeps serving while things change | Reads stay complete whenever compaction strikes mid-read (48 timings) |
| Similar stations | ~98% precision@5 on synthetic stations; 0.78 ms over 100,000 stations, about 8× faster than pgvector's exact scan |

Testing found four real flaws in my design, all fixed and covered by tests
(details in `WALKTHROUGH.md`). It also showed that my storage-size argument
against Postgres doesn't hold yet (`BENCHMARKS.md`).

---

## Run it

```bash
./run.sh          # set up, then 395 tests + demo + model check (~10 s)
./run.sh s3       # the same 395 tests against an S3 API (LocalStack in Docker, ~1 min)

./scripts/linux_check.sh        # tests on Linux in 256 MB; compaction in 128 MB
./scripts/postgres_compare.sh   # storage, reads and vector search vs Postgres 17 + pgvector
```

Or by hand:

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q              # tests
.venv/bin/python -m archive.demo           # every requirement in the brief, end to end
.venv/bin/python -m archive.model_check    # every interleaving of the commit race
```

| Test file | Proves |
|---|---|
| `test_commit_rule.py` | Commit rule, including 300 random takeovers |
| `test_records.py` | Rules for late, duplicated and corrected data |
| `test_storage.py` | Segments, formats, merges, two compactors at once, gc safety |
| `test_partitioning_and_memory.py` | Monthly files, 12 reads per station-year, 5% rewrite rule, flat compaction memory |
| `test_serving_while_changing.py` | Reads stay complete when compaction, merge or gc strike at any point |
| `test_ingest_query.py` | Gateway, overload, history, gaps, station status |
| `test_similarity_and_model.py` | Similarity quality, snapshot swap, model check |

---

## Technologies

### Used

| Area | What | Where |
|---|---|---|
| Language | **Python 3** (3.12 on Linux, 3.14 on macOS) | Everything |
| Testing | pytest, randomised fault injection, exhaustive model checking | `tests/`, `archive/model_check.py` |
| Object storage | **S3 API** with conditional writes (`If-None-Match`, `If-Match`), ranged reads; boto3 | `archive/s3store.py` |
| Containers | **Docker**: LocalStack (S3), Postgres, Linux test runs | `run.sh`, `scripts/` |
| Linux | Tests and memory limits on a Linux kernel with cgroup v2 | `scripts/linux_check.sh` |
| Databases | **Postgres 17** for the storage/read comparison; psycopg | `scripts/postgres_compare.py` |
| Vector search | NumPy flat cosine scan; **pgvector** (exact and HNSW) for comparison | `archive/similarity.py`, `BENCHMARKS.md` |
| OS concepts | Atomic `link`/`rename`, `flock`, `mmap`, external merge sort | `archive/store.py`, `archive/spill.py` |
| Version control | **git**, one commit per stage | this repository |

### Not used, and still learning

- **Rust is not used.** The prototype is all Python because it was the fastest
  way to prove the design's logic. I am very keen to learn Rust properly,
  especially for the storage-engine parts of a system like this, where memory
  control and safety matter most.
- **Go is not used either.** The design proposes Go for production services.
- **AWS: still learning and practising.** The code speaks the S3 API and has
  been tested against LocalStack, but it has not been deployed on AWS. I am
  learning how S3 buckets work and how AWS pricing changes with the instance
  type and operating system (for example Ubuntu versus other images).

---

## About me

I have worked with **Python** to develop machine learning models. I have
practical experience with **MySQL**, which made **Postgres** feel familiar, and
I have used **Git, GitLab, Docker and Linux** commands day to day, including
managing and restarting servers.

So far I have worked at a service-based company. What I want now is to build
things from scratch and own them end to end, which is what a startup offers,
and to grow along with the company.

Working on this challenge, I learned how to combine Postgres with Python and
AWS-style object storage. I started learning how S3 buckets work and what AWS
costs depending on the setup. For sensor systems, I also learned why it matters
to keep data as close as possible to where it is produced and used: it reduces
latency and transfer costs, and it helps with where data is allowed to live.
