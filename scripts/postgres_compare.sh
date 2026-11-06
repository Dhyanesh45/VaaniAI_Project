#!/usr/bin/env bash
# Start a throwaway Postgres 17 + pgvector (1 GB memory limit, port 5433) and
# run the comparison. Does not touch any other Postgres on this machine.
set -e
cd "$(dirname "$0")/.."
IMAGE=pgvector/pgvector:0.8.0-pg17
PORT=${PORT:-5433}

.venv/bin/pip install -q -r requirements-bench.txt
if ! docker ps --format '{{.Names}}' | grep -qx archive-pg; then
  docker rm -f archive-pg >/dev/null 2>&1 || true
  # --shm-size: Docker's default 64 MB /dev/shm is too small for a parallel HNSW build
  docker run -d --name archive-pg -p $PORT:5432 -m 1g --shm-size=512m \
    -e POSTGRES_PASSWORD=postgres $IMAGE >/dev/null
  for i in $(seq 1 30); do docker exec archive-pg pg_isready -q -U postgres && break; sleep 1; done
  sleep 1
fi
ARCHIVE_PG_DSN=postgresql://postgres:postgres@localhost:$PORT/postgres \
  .venv/bin/python scripts/postgres_compare.py "$@"
echo "== stop it with: docker rm -f archive-pg"
