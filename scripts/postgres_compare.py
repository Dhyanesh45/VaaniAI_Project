"""Measure the design doc's "why not Postgres / pgvector" claims instead of asserting them.

Needs a Postgres with pgvector (scripts/postgres_compare.sh starts one).

Part A, storage and reads: the same sensor data in
  - the archive's monthly segments
  - Postgres, one row per report (8 channels as columns): the doc's comparison
  - Postgres, one row per point with revision and arrival time: the archive's
    actual data model (keeps versions for corrections and as-of queries)
Part B, similarity search: 100,000 x 128 vectors
  - numpy flat scan (what the prototype uses)
  - pgvector exact scan, and pgvector HNSW index (build time, size, recall)
"""
import argparse
import io
import os
import random
import statistics
import sys
import tempfile
import time

import numpy as np
import psycopg

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from archive.log import LogWriter, compact, init_log, merge, read_head  # noqa: E402
from archive.query import FooterCache, history  # noqa: E402
from archive.store import LocalStore  # noqa: E402

T0 = 1767225600  # 2026-01-01 UTC
CHANNELS = ["temp", "humidity", "pressure", "wind", "rain", "solar", "co2", "pm25"]
REPORTS_PER_YEAR_AT_SCALE = 100_000 * 525_600  # design doc: 100k stations, 1/min


def timeit(fn, n):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t) * 1000)
    out.sort()
    return statistics.median(out), out[int(len(out) * 0.99) - 1 if len(out) >= 100 else -1]


def make_reports(stations, days):
    rng = random.Random(0)
    for s in range(stations):
        level = [rng.uniform(0, 100) for _ in CHANNELS]
        for m in range(days * 1440):
            level = [round(v + rng.gauss(0, 0.2), 2) for v in level]
            yield s, T0 + m * 60, level


def part_a(conn, stations, days):
    print(f"\n## A. Storage and one-station reads: {stations} stations x {days} days x 1/min x 8 channels")
    reports = list(make_reports(stations, days))
    n_rep, n_pts = len(reports), len(reports) * len(CHANNELS)
    print(f"{n_rep:,} reports = {n_pts:,} points")

    # --- the archive
    store = LocalStore(tempfile.mkdtemp(prefix="pgcmp-"))
    init_log(store)
    w = LogWriter(store)
    w.take_over()
    batch = []
    for s, t, vals in reports:
        batch.extend({"station": f"st-{s}", "channel": c, "t": t, "v": v, "rev": 0, "arrived": t}
                     for c, v in zip(CHANNELS, vals))
        if len(batch) >= 20_000:
            w.commit(batch)
            batch = []
    if batch:
        w.commit(batch)
    t = time.perf_counter()
    compact(store)
    merge(store)
    compact_s = time.perf_counter() - t
    seg_bytes = sum(len(store.get(n)) for n in read_head(store)[0]["segments"])

    day = (T0 + 86400 // 2, T0 + 86400 // 2 + 86400) if days > 1 else (T0, T0 + 86400)
    st = f"st-{stations // 2}"
    cold = timeit(lambda: history(store, st, *day, cache=FooterCache()), 20)
    cache = FooterCache()
    warm = timeit(lambda: history(store, st, *day, cache=cache), 20)

    # --- Postgres
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS reports, points")
        cur.execute("""CREATE TABLE reports (station int, t timestamptz, rev smallint,
                       temp real, humidity real, pressure real, wind real,
                       rain real, solar real, co2 real, pm25 real,
                       PRIMARY KEY (station, t, rev))""")
        cur.execute("""CREATE TABLE points (station int, channel smallint, t timestamptz,
                       rev smallint, v double precision, arrived timestamptz,
                       PRIMARY KEY (station, channel, t, rev))""")
        t = time.perf_counter()
        with cur.copy("COPY reports FROM STDIN") as cp:
            for s, ts, vals in reports:
                cp.write_row((s, time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(ts)), 0, *vals))
        rep_load = time.perf_counter() - t
        t = time.perf_counter()
        with cur.copy("COPY points FROM STDIN") as cp:
            for s, ts, vals in reports:
                iso = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(ts))
                for c, v in enumerate(vals):
                    cp.write_row((s, c, iso, 0, v, iso))
        pts_load = time.perf_counter() - t
    conn.commit()
    conn.autocommit = True
    conn.execute("VACUUM ANALYZE reports")
    conn.execute("VACUUM ANALYZE points")
    conn.autocommit = False
    size = lambda tbl: conn.execute(f"SELECT pg_total_relation_size('{tbl}')").fetchone()[0]  # noqa: E731
    rep_bytes, pts_bytes = size("reports"), size("points")

    lo = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(day[0]))
    hi = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(day[1]))
    sid = stations // 2
    q_rep = timeit(lambda: conn.execute(
        "SELECT * FROM reports WHERE station=%s AND t>=%s AND t<%s ORDER BY t", (sid, lo, hi)).fetchall(), 20)
    q_pts = timeit(lambda: conn.execute(
        "SELECT * FROM points WHERE station=%s AND t>=%s AND t<%s ORDER BY channel, t", (sid, lo, hi)).fetchall(), 20)

    raw = n_rep * 64  # the doc's 64-byte binary report
    rows = [
        ("Archive segments (JSON+zlib, all versions)", seg_bytes, f"{compact_s:.1f}s compact+merge",
         f"{cold[0]:.1f} cold / {warm[0]:.1f} warm"),
        ("Postgres, 1 row per report (doc's comparison)", rep_bytes, f"{rep_load:.1f}s COPY", f"{q_rep[0]:.1f}"),
        ("Postgres, 1 row per point + rev + arrived", pts_bytes, f"{pts_load:.1f}s COPY", f"{q_pts[0]:.1f}"),
        ("Doc baseline: raw 64 B/report, uncompressed", raw, "-", "-"),
    ]
    print(f"\n| Storage | Bytes/report | vs raw 64 B | At doc scale (TB/year) | Load | One station-day read, p50 ms |")
    print("|---|---|---|---|---|---|")
    for name, b, load, q in rows:
        per = b / n_rep
        print(f"| {name} | {per:.1f} | {per / 64:.2f}x | {per * REPORTS_PER_YEAR_AT_SCALE / 1e12:.2f} | {load} | {q} |")


def part_b(conn, n, dim, queries):
    print(f"\n## B. Similarity search: {n:,} x {dim} float32 vectors, top-10 cosine")
    rng = np.random.default_rng(0)
    X = rng.standard_normal((n, dim), dtype=np.float32)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    qs = [X[i] for i in rng.integers(0, n, queries)]

    def numpy_top(q):
        s = X @ q
        top = np.argpartition(-s, 10)[:10]
        return set(top[np.argsort(-s[top])].tolist())

    np_t = timeit(lambda: numpy_top(qs[0]), 200)
    truth = [numpy_top(q) for q in qs]

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute("DROP TABLE IF EXISTS items")
        cur.execute(f"CREATE TABLE items (id int PRIMARY KEY, embedding vector({dim}))")
        with cur.copy("COPY items (id, embedding) FROM STDIN") as cp:
            for i, v in enumerate(X):
                cp.write_row((i, "[" + ",".join(f"{x:.6f}" for x in v) + "]"))
    conn.commit()
    conn.autocommit = True
    conn.execute("VACUUM ANALYZE items")
    table_mb = conn.execute("SELECT pg_total_relation_size('items')").fetchone()[0] / 2**20

    lit = lambda q: "[" + ",".join(f"{x:.6f}" for x in q) + "]"  # noqa: E731
    sql = "SELECT id FROM items ORDER BY embedding <=> %s::vector LIMIT 10"

    def pg_run(qi):
        return {r[0] for r in conn.execute(sql, (lit(qs[qi]),)).fetchall()}

    exact_t = timeit(lambda: pg_run(0), 20)
    exact_recall = statistics.mean(len(pg_run(i) & truth[i]) / 10 for i in range(queries))

    conn.execute("SET maintenance_work_mem = '256MB'")
    t = time.perf_counter()
    conn.execute("CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops)")
    build_s = time.perf_counter() - t
    idx_mb = conn.execute("SELECT pg_indexes_size('items')").fetchone()[0] / 2**20
    hnsw = []
    for ef in (40, 200, 800):  # 40 is pgvector's default; higher = more accurate, slower
        conn.execute(f"SET hnsw.ef_search = {ef}")
        hnsw.append((ef, timeit(lambda: pg_run(0), 100),
                     statistics.mean(len(pg_run(i) & truth[i]) / 10 for i in range(queries))))

    print(f"\n| Method | Query p50 ms | Query p99 ms | Recall@10 | Memory / size | Build |")
    print("|---|---|---|---|---|---|")
    print(f"| numpy flat scan (prototype) | {np_t[0]:.2f} | {np_t[1]:.2f} | 1.00 (exact) | {X.nbytes / 2**20:.0f} MB array | none |")
    print(f"| pgvector exact (no index) | {exact_t[0]:.1f} | {exact_t[1]:.1f} | {exact_recall:.2f} | {table_mb:.0f} MB table | none |")
    for ef, (p50, p99), recall in hnsw:
        print(f"| pgvector HNSW (m=16, ef_search={ef}) | {p50:.2f} | {p99:.2f} | {recall:.2f} | +{idx_mb:.0f} MB index | {build_s:.0f}s |")
    print("\nNote: uniform random vectors are a worst case for HNSW (no structure to exploit);"
          " real fingerprints cluster, so recall there would be higher.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("ARCHIVE_PG_DSN",
                                                  "postgresql://postgres:postgres@localhost:5433/postgres"))
    ap.add_argument("--stations", type=int, default=50)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--vectors", type=int, default=100_000)
    ap.add_argument("--queries", type=int, default=50)
    a = ap.parse_args()
    with psycopg.connect(a.dsn) as conn:
        server = conn.execute("SHOW server_version").fetchone()[0]
        print(f"Postgres {server}; client and server on the same machine (Docker on macOS)")
        part_a(conn, a.stations, a.days)
        part_b(conn, a.vectors, 128, a.queries)


if __name__ == "__main__":
    main()
