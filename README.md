# vxImporter

High-throughput Couchbase importer for JSON array files.

The program streams JSON documents from a top-level array, batches records, and upserts them to a target Couchbase collection using a concurrent worker pool.

## Quick Start (60 Seconds)

1. Build the binary:

```bash
go build -o vximporter ./vximporter.go
```

1. Create a sample input file:

```bash
cat > data.json <<'EOF'
[
  {"id":"demo_1","name":"Demo Airline 1"},
  {"id":"demo_2","name":"Demo Airline 2"}
]
EOF
```

1. Run the importer:

```bash
./vximporter \
  -conn "couchbase://127.0.0.1" \
  -user "Administrator" \
  -pass "password" \
  -bucket "travel-sample" \
  -scope "inventory" \
  -collection "airline" \
  -file "data.json"
```

1. Optional Docker path:

```bash
docker build -t vximporter .
docker run --rm \
  -v "$(pwd)/data.json:/data/data.json:ro" \
  vximporter \
  -conn "couchbase://host.docker.internal" \
  -user "Administrator" \
  -pass "password" \
  -bucket "travel-sample" \
  -scope "inventory" \
  -collection "airline" \
  -file "/data/data.json"
```

## Features

- Streams large input files without loading the entire dataset into memory.
- Uses concurrent workers for improved ingestion throughput.
- Executes bulk upsert operations with Couchbase Go SDK v2.
- Uses the `id` field of each document as the Couchbase document key.
- Emits summary metrics (successes, failures, elapsed time, throughput).

## Requirements

- Go 1.25+
- Couchbase Server reachable from your machine or container
- Network access to Couchbase ports from runtime environment

## Project Layout

- `vximporter.go`: Main program and import logic
- `go.mod`: Module and dependency definitions
- `Dockerfile`: Multi-stage container build
- `.dockerignore`: Build context exclusions
- `import.sh`: Native and Docker command examples

## Build

Build native binary:

```bash
go build -o vximporter ./vximporter.go
```

## Run (Native)

```bash
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
```

## Docker

Build image:

```bash
docker build -t vximporter .
```

Run with local file mount:

```bash
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
```

Notes:

- `host.docker.internal` works on Docker Desktop for macOS/Windows.
- On Linux, use your host IP or a user-defined Docker network path to Couchbase.

## CLI Flags

| Flag          | Default                 | Description                          |
| ------------- | ----------------------- | ------------------------------------ |
| `-conn`       | `couchbase://127.0.0.1` | Couchbase connection string          |
| `-user`       | `Administrator`         | Couchbase username                   |
| `-pass`       | `password`              | Couchbase password                   |
| `-bucket`     | `default`               | Target bucket                        |
| `-scope`      | `_default`              | Target scope                         |
| `-collection` | `_default`              | Target collection                    |
| `-file`       | `data.json`             | Input JSON array file path           |
| `-batch-size` | `500`                   | Number of documents per bulk request |
| `-workers`    | `8`                     | Number of concurrent workers         |

## Input File Format

The importer expects a top-level JSON array of objects:

```json
[ 
  {"id":"airline_1","name":"Airline One","country":"USA"},
  {"id":"airline_2","name":"Airline Two","country":"UK"},
  {"id":"airline_3","name":"Airline Three","country":"DE"}
]
```

Document key behavior:

1. `id` is required for every document.
2. The `id` value is used as the Couchbase key.
3. Documents without a valid `id` are counted as failures.

## Runtime Behavior

- Input must be a valid JSON array (`[` ... `]`).
- The file is streamed; the full array is not loaded into memory.
- Invalid JSON structure causes the import to stop with an error.
- Documents without a valid `id` are counted as failures and skipped.
- Bulk operation errors are logged; individual operation errors are counted.
- Final report includes:
  - elapsed time
  - successful docs
  - failed docs
  - throughput (ops/sec)

## Performance Tuning

- Increase `-workers` if Couchbase and network can handle more parallelism.
- Increase `-batch-size` to reduce request overhead, but monitor latency and memory.
- Use smaller values if you see timeouts or high cluster load.
- Start from `-workers 8` and `-batch-size 500`, then tune incrementally.

## Testing

Run unit tests:

```bash
go test ./...
```

Or via Makefile:

```bash
make test
```

Run integration tests (requires a reachable Couchbase target):

```bash
CB_CONN="couchbase://127.0.0.1" \
CB_USER="Administrator" \
CB_PASS="password" \
CB_BUCKET="travel-sample" \
CB_SCOPE="inventory" \
CB_COLLECTION="airline" \
go test -tags integration ./...
```

Or via Makefile:

```bash
make test-integration
```

Integration test notes:

- Integration tests are excluded from default test runs.
- If required env vars are not set, integration tests are skipped.
- The integration test writes and reads one test document in the target collection.
- Integration test file location: `tests/integration/integration_test.go`

## Troubleshooting

Connection failures:

- Verify connection string, credentials, and cluster reachability.
- Confirm bucket/scope/collection exist and credentials are authorized.

File errors:

- Ensure `-file` path exists and is readable.
- For Docker, confirm the host file mount source path is correct.

Import failures:

- Validate the file is a JSON array of JSON objects.
- Ensure every document includes a non-empty `id` field.
- Review logs for bulk operation warnings and per-document errors.
- Lower `-workers` and `-batch-size` if cluster saturation is suspected.

## Security Notes

- Avoid passing production passwords directly in shell history when possible.
- Consider using environment-specific secret management in CI/CD.
- Restrict Couchbase user permissions to only required buckets/scopes/collections.
