#!/usr/bin/env bash
# ./run.sh        set up, then tests + demo + model check on a local folder
# ./run.sh s3     the same test suite against a real S3 API (LocalStack in Docker)
set -e
cd "$(dirname "$0")"

PY=/Library/Frameworks/Python.framework/Versions/3.14/bin/python3
[ -x "$PY" ] || PY=python3

if [ ! -d .venv ]; then
  echo "== creating .venv with $PY"
  "$PY" -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

if [ "$1" = "s3" ]; then
  IMAGE=localstack/localstack:4.4.0
  PORT=4566
  if ! docker ps --format '{{.Names}}' | grep -qx archive-s3; then
    echo "== starting $IMAGE on port $PORT"
    docker rm -f archive-s3 >/dev/null 2>&1 || true
    docker run -d --name archive-s3 -p $PORT:4566 -e SERVICES=s3 $IMAGE >/dev/null
  fi
  for i in $(seq 1 60); do
    curl -sf http://localhost:$PORT/_localstack/health | grep -q '"s3": "\(available\|running\)"' && break
    sleep 1
  done
  echo "== tests against S3 at http://localhost:$PORT"
  ARCHIVE_S3_ENDPOINT=http://localhost:$PORT AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    .venv/bin/python -m pytest -q
  echo "== stop it with: docker rm -f archive-s3"
  exit 0
fi

echo "== tests";        .venv/bin/python -m pytest -q
echo "== demo";         .venv/bin/python -m archive.demo
echo "== model check";  .venv/bin/python -m archive.model_check
