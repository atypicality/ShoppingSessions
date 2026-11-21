#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libmimalloc.so.2
exec /app/pipeline "$@"