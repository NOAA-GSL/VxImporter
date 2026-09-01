#!/usr/bin/env bash
set -euo pipefail

# Native binary example (replace placeholder values before running)
# ./vximporter \
#   -conn "${HOME}/credentials" \
#   -file "large_dataset.json" \
#   -workers 16 \
#   -batch-size 1000

# Docker example (mount local JSON array or gzip-compressed JSON file read-only into container)
# Replace placeholder values before running.
docker run --rm \
-v "${HOME}/credentials:/run/config/credentials:ro" \
-v "$(pwd)/large_dataset.json:/data/data.json:ro" \
vximporter:local \
-conn "/run/config/credentials" \
-file "/data/data.json" \
-workers 16 \
-batch-size 1000