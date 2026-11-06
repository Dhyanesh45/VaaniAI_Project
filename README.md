# The Archive: prototype

A log-structured sensor archive whose only durable store is object storage.
This is the runnable companion to the design doc. `WALKTHROUGH.md` explains
every part.

    ./run.sh          # set up, then 395 tests + demo + model check on a local folder (~10 s)
    ./run.sh s3       # the same 395 tests against a real S3 API (LocalStack in Docker, ~1 min)

Or by hand:

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    .venv/bin/python -m pytest -q              # tests
    .venv/bin/python -m archive.demo           # every requirement in the brief, end to end
    .venv/bin/python -m archive.model_check    # every interleaving of the commit race

| Step | File | What it shows |
|---|---|---|
| 1 | `archive/log.py` `commit` | The commit rule: PUT, then read head, then acknowledge |
| 2 | `archive/records.py` | Late, out-of-order, duplicate and corrected data; as-of queries |
| 3 | `archive/segment.py`, `log.py`, `spill.py` | Monthly segments with per-station footer, streaming compaction under a memory cap, 5% rewrite rule, format changes, safe gc |
| 4 | `archive/ingest.py` | Validation, shard routing, bounded buffers, 429 on overload |
| 5 | `archive/query.py` | One station's history: month pruning, ranged reads, optimistic reads |
| 6 | `archive/similarity.py` | Fingerprints, flat cosine scan, snapshot swap, mmap |
| 7 | `archive/model_check.py` | Exhaustive check that acknowledged data is never lost |
| — | `archive/store.py`, `archive/s3store.py` | The same storage interface for a local folder and for S3 |

| Test file | Proves |
|---|---|
| `test_commit_rule.py` | Commit rule, incl. 300 random takeovers |
| `test_records.py` | Messy-data rules |
| `test_storage.py` | Segments, formats, merge, two compactors, gc safety |
| `test_partitioning_and_memory.py` | Monthly files, 12 reads per station-year, 5% rule, flat compaction memory |
| `test_serving_while_changing.py` | Reads stay complete when compaction/merge/gc strike at any point |
| `test_ingest_query.py` | Gateway, overload, history, gaps, status |
| `test_similarity_and_model.py` | Similarity quality, snapshot swap, model check |

Set `ARCHIVE_S3_ENDPOINT` to run any test against S3; `run.sh s3` does this.
