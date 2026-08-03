#!/usr/bin/env bash

# Native binary example
./vximporter \
-conn "couchbase://127.0.0.1" \
-user "Administrator" \
-pass "password" \
-bucket "travel-sample" \
-scope "inventory" \
-collection "airline" \
-file "large_dataset.json" \
-workers 16 \
-batch-size 1000

# Docker example (mount local JSON array file read-only into container)
docker run --rm \
-v "$(pwd)/large_dataset.json:/data/data.json:ro" \
vximporter \
-conn "couchbase://host.docker.internal" \
-user "Administrator" \
-pass "password" \
-bucket "travel-sample" \
-scope "inventory" \
-collection "airline" \
-file "/data/data.json" \
-workers 16 \
-batch-size 1000