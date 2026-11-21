#!/usr/bin/env bash
# From ShoppingSessions repository root:
#   bash test_and_bench.sh               # builds and runs 36 fixtures
#   bash test_and_bench.sh /tmp/bench-20m # fixtures + 5 benchmark runs
set -euo pipefail

IMAGE=shopping-pipeline
ROOT="$(pwd -P)"
if [[ ! -f cpp/main.cpp || ! -f Dockerfile || ! -f scripts/run_fixtures.py ]]; then
    echo "Run this script from the ShoppingSessions repository root." >&2
    exit 2
fi

# Build with the existing Dockerfile, which already installs Arrow and Parquet.
docker build --platform linux/amd64 -t "$IMAGE" .

# Fixture runner appends input/output paths to the command.
FIXTURE_LOG=$(mktemp)
trap 'rm -f "$FIXTURE_LOG"' EXIT
python3 scripts/run_fixtures.py fixtures -- \
    docker run --rm --network none \
    -v "$ROOT:$ROOT" -v /tmp:/tmp \
    -e NCPU=8 -w "$ROOT" "$IMAGE" /app/pipeline | tee "$FIXTURE_LOG"
if ! grep -q '36/36 fixtures passed' "$FIXTURE_LOG"; then
    echo 'Fixture suite did not report 36/36 passing; benchmark not started.' >&2
    exit 1
fi

if [[ $# -eq 0 ]]; then
    echo "Fixture stage finished. Supply /tmp/bench-20m to run benchmarks."
    exit 0
fi

DATASET="$(realpath "$1")"
if [[ ! -f "$DATASET/manifest.json" ]]; then
    echo "Missing manifest.json in dataset: $DATASET" >&2
    exit 2
fi

RUN_LOG=$(mktemp)
trap 'rm -f "$FIXTURE_LOG" "$RUN_LOG"' EXIT
for i in 1 2 3 4 5; do
    OUT=$(mktemp -d /tmp/shopping-bench-XXXXXXXX)
    echo "Benchmark run $i/5"
    /usr/bin/time -f 'WALL_SECONDS=%e' -o "$RUN_LOG" -a \
      docker run --rm --network none --cpus=8 --memory=24g \
      -v "$DATASET:/data:ro" -v "$OUT:/output" \
      -e NCPU=8 -e MEM_GB=24 \
      "$IMAGE" /app/pipeline /data /output
    rm -rf "$OUT"
done

python3 - "$RUN_LOG" <<'PY'
import statistics
import sys
lines = open(sys.argv[1]).read().splitlines()
times = [float(x.split('=', 1)[1]) for x in lines if x.startswith('WALL_SECONDS=')]
print('Wall seconds:', ', '.join(f'{v:.3f}' for v in times))
print(f'Median wall time: {statistics.median(times):.3f}s')
PY
