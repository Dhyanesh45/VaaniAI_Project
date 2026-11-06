#!/usr/bin/env bash
# Run the prototype on Linux under hard cgroup memory limits.
#   1. the full test suite in a 256 MB container
#   2. the same compaction twice in a 128 MB container:
#      streaming (16 MB sort budget) must finish; all-in-RAM must be OOM-killed
set -e
cd "$(dirname "$0")/.."
IMAGE=archive-linux:py3.12

docker build -q -t $IMAGE -f scripts/Dockerfile.linux . >/dev/null
RUN="docker run -v $PWD:/app:ro -w /app"

echo "== 1. full test suite on Linux, memory limit 256 MB"
$RUN --rm -m 256m --memory-swap 256m $IMAGE \
  sh -c 'uname -sr; python -m pytest -q -p no:cacheprovider'

ROWS=${ROWS:-400000}
for MODE in streaming in-ram; do
  CAP=$([ $MODE = streaming ] && echo 16 || echo 100000)
  NAME=archive-oom-$MODE
  docker rm -f $NAME >/dev/null 2>&1 || true
  echo
  echo "== 2. compaction of $ROWS rows, $MODE (sort budget ${CAP} MB), memory limit 128 MB"
  set +e
  $RUN --name $NAME -m 128m --memory-swap 128m $IMAGE \
    python scripts/oom_demo.py --rows $ROWS --cap-mb $CAP
  CODE=$?
  set -e
  KILLED=$(docker inspect -f '{{.State.OOMKilled}}' $NAME)
  echo "-> exit code $CODE, OOM-killed by the kernel: $KILLED"
  docker rm -f $NAME >/dev/null
done
