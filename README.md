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
cat > /tmp/test.json <<'EOF'
[
  {"id":"demo_1","name":"Demo Airline 1"},
  {"id":"demo_2","name":"Demo Airline 2"}
]
EOF
```

1. Run the importer:

```bash
./vximporter \
  -conn "${HOME}/credentials-test" \
  -file "/tmp/test.json"
```

1. Optional Docker path:

```bash
docker build -t vximporter .
docker run --rm \
  -v "${HOME}/credentials-test:/run/config/credentials:ro" \
  -v "/tmp/data.json:/data/test.json:ro" \
  vximporter \
  -conn "/run/config/credentials" \
  -file "/data/test.json"
```

## Features

- Streams large input files without loading the entire dataset into memory.
- Uses concurrent workers for improved ingestion throughput.
- Executes bulk upsert operations with Couchbase Go SDK v2.
- Uses the `id` field of each document as the Couchbase document key.
- Publishes a shared Couchbase import lock while documents are being written.
- Emits summary metrics (successes, failures, elapsed time, throughput).

## Requirements

- Go 1.25+
- Couchbase Server reachable from your machine or container
- Network access to Couchbase ports from runtime environment

## Project Layout

- `vximporter.go`: Main program and import logic
- `go.mod`: Module and dependency definitions
- `Dockerfile`: Multi-stage container build (entrypoint auto-detects archive vs file mode)
- `entrypoint.sh`: Container entrypoint — reads credentials, detects mode, runs vximporter
- `docker-compose.yml`: Service definition for containerised imports
- `.dockerignore`: Build context exclusions
- `import_file.sh`: Native and Docker single-file import examples
- `import_archive.sh`: Host-side launcher for archive imports

## Build

Build native binary:

```bash
go build -o vximporter ./vximporter.go
```

## Run (Native)

```bash
./vximporter \
  -conn "${HOME}/credentials" \
  -file "test.json" \
  -workers 16 \
  -batch-size 1000
```

## Docker Compose (recommended)

Build the image:

```bash
docker compose build
```

Run a single-file import:

```bash
mkdir -p /tmp/vx-output
export VX_LOAD_DIR=/tmp/vx-empty; export VX_OUTPUT_PATH=/tmp/vx-output \
  docker compose run --rm \
    -v "$(pwd)/large_dataset.json:/data/large_dataset.json:ro" \
    --user "$(id -u):$(id -g)" \
    vximporter
```

`/data/import.json` or `/data/import.json.gz` triggers single-file mode in the entrypoint. Credentials are read from `${HOME}/credentials`.

## Archive Import

`import_archive.sh` is a thin host-side launcher. All archive processing logic runs inside the container via `entrypoint.sh`, which auto-detects the mode:

- **Archive mode**: triggered when `.tar.gz` files are present in `/data/load`. Each tarball is extracted, and any `.json` or `.json.gz` files inside it are imported before the tarball is moved to `/data/output/success-<name>` or `/data/output/failed-<reason>-<name>`.
- **Single-file mode**: triggered when `/data/import.json` or `/data/import.json.gz` is present and no archives are found.

Required flags for `import_archive.sh`:

| Flag | Description                                      |
| ---- | ------------------------------------------------ |
| `-l` | Load directory containing `.tar.gz` archives     |
| `-a` | Archive directory for processed tarballs         |
| `-w` | Number of concurrent import workers (default: 8) |

Credentials are read from `${HOME}/credentials` inside the container (mounted by `import_archive.sh`).

Credentials file format (YAML-style key: value):

```text
cb_host: couchbase://127.0.0.1
cb_user: user
cb_password: pass
cb_bucket: vxdata
cb_scope: _default
cb_collection: test
```

Example:

```bash
./import_archive.sh \
  -l /opt/data/test/load \
  -a /opt/data/test/output \
  -w 8
```

The script can be run from any directory. It executes `docker run` against the local image tag `vximporter:local`.

## Docker

Build image:

```bash
docker build -t vximporter .
```

Run with local file mount:

```bash
docker run --rm \
  -v "${HOME}/credentials:/run/config/credentials:ro" \
  -v "$(pwd)/large_dataset.json:/data/large_dataset.json:ro" \
  -v "$(pwd)/output:/data/output" \
  --tmpfs /data/tmp \
  --user "$(id -u):$(id -g)" \
  vximporter:local
```

Run with explicit vximporter flags (entrypoint passthrough):

```bash
docker run --rm \
  -v "${HOME}/credentials:/run/config/credentials:ro" \
  -v "/tmp/test.json:/data/test.json:ro" \
  vximporter:local \
  -conn "/run/config/credentials" \
  -file "/data/test.json"
```

Run a gzip-compressed JSON file directly:

```bash
docker run --rm \
  -v "${HOME}/credentials:/run/config/credentials:ro" \
  -v "/tmp/test.json.gz:/data/test.json.gz:ro" \
  vximporter:local \
  -conn "/run/config/credentials" \
  -file "/data/test.json.gz"
```

Override the default credentials file (direct passthrough mode):

```bash
docker run --rm \
  -v "/path/to/alt-creds.yaml:/run/config/alt-creds.yaml:ro" \
  -v "/tmp/test.json:/data/test.json:ro" \
  vximporter:local \
  -conn "/run/config/alt-creds.yaml" \
  -file "/data/test.json"
```

Override credentials and set custom archive directories (auto/archive mode):

```bash
CREDENTIALS_FILE="/run/config/alt-creds.yaml" \
VX_LOAD_DIR="/opt/data/test/xfer-test" \
VX_OUTPUT_PATH="/opt/data/test/output" \
VX_WORKERS="8" \
docker compose run --rm \
  -v "/path/to/alt-creds.yaml:/run/config/alt-creds.yaml:ro" \
  --user "$(id -u):$(id -g)" \
  vximporter
```

explicit example:

``` bash
export VX_LOAD_DIR="/opt/data/test/xfer-test"; \
export VX_OUTPUT_PATH="/opt/data/test/output"; \
export VX_WORKERS="8"; \
docker compose run --rm \
-e CREDENTIALS_FILE="/run/config/alt-creds.yaml" \
-v "${HOME}/credentials-test:/run/config/alt-creds.yaml:ro" \
--user "$(id -u):$(id -g)" \
vximporter
```

Notes:

- With no command arguments, the container entrypoint auto-detects archive mode via `${VX_LOAD_DIR:-/data/load}` and single-file mode via `${VX_IMPORT_FILE:-/data/import.json}`.
- Single-file imports may be plain JSON or gzip-compressed JSON. Auto mode checks `${VX_IMPORT_FILE}` first, then `${VX_IMPORT_GZIP_FILE:-${VX_IMPORT_FILE}.gz}`.
- With command arguments, the entrypoint passes them directly to `vximporter`.
- `host.docker.internal` works on Docker Desktop for macOS/Windows.
- On Linux, use your host IP or a user-defined Docker network path to Couchbase.

## CLI Flags

| Flag          | Default     | Description                                                                  |
| ------------- | ----------- | ---------------------------------------------------------------------------- |
| `-conn`       | `""`        | Path to credentials YAML file                                                |
| `-file`       | `data.json` | Input JSON array file path                                                   |
| `-collection` | `""`        | Couchbase collection name; overrides `cb_collection` in the credentials file |
| `-batch-size` | `500`       | Number of documents per bulk request                                         |
| `-workers`    | `8`         | Number of concurrent workers                                                 |

## Environment Variables

| Variable                       | Default    | Description                                                                                                                                      |
| ------------------------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `LOG_LEVEL`                    | `INFO`     | Controls logging verbosity. Supported values: `DEBUG`, `INFO`, `WARN`, `ERROR`. DEBUG logs file names being processed and empty data conditions. |
| `VX_CREDENTIALS_FILE`          | `""`       | Alternative to `-conn` flag; sets the path to credentials YAML file                                                                              |
| `BUCKET_READY_TIMEOUT_SECONDS` | `60`       | Timeout in seconds for waiting for Couchbase bucket to become ready. Useful for slower remote clusters.                                          |
| `DURABILITY_LEVEL`             | `majority` | Durability applied to each document upsert: `none`, `majority`, `majorityAndPersistOnMaster`, or `persistToMajority`.                            |

## Import Lock and Couchbase Readiness

VxImporter coordinates with VxIngest through one lock document in the target
bucket's default scope and `COMMON` collection:

```text
MD:import_lock:COMMON:V01
```

The lock document has this shape:

```json
{
  "id": "MD:import_lock:COMMON:V01",
  "status": "running",
  "updated": 1760000000,
  "job_id": "vximporter:hostname:12345"
}
```

`job_id` combines the host name and process ID so operators can identify the
importer that owns the lock. `updated` is a Unix timestamp in seconds.

### Importer lifecycle

1. VxImporter connects to Couchbase and waits for the target bucket to become
  ready. The wait defaults to 60 seconds and is controlled by
  `BUCKET_READY_TIMEOUT_SECONDS`.
2. It attempts to acquire the lock before starting document writes. A missing
  lock document is created with `status="running"`. An existing `idle`
  document is changed to `running` with a CAS-protected replacement.
3. If the lock already has `status="running"`, the importer waits and polls
  every 10 seconds for the previous import to finish. It waits for up to 30
  minutes. A heartbeat older than 30 minutes is considered stale and can be
  reclaimed with CAS.
4. While importing, VxImporter refreshes `updated` and keeps `status="running"`
  every minute.
5. On normal completion, input failure, or another deferred exit path, the
  heartbeat is stopped and the lock is set to `status="idle"`.

The lock is a coordination marker, not a document-level transaction. It does
not roll back documents already written if an import fails. A process killed
before deferred cleanup can leave a stale `running` document; VxIngest's reader
side has a stale-lock policy, described below. Inspect `job_id` and `updated`
before manually changing a lock.

### Reader wait behavior in VxIngest

VxIngest builders that read documents written by VxImporter check this document
before reading the dataset. They poll every 10 seconds while the status is
`running`. They stop waiting and proceed when:

- the document is absent;
- the status is not `running` (normally `idle`);
- the `updated` timestamp is more than 30 minutes old;
- the lock cannot be read; or
- the total wait reaches 30 minutes.

The reader-side behavior is intentionally fail-open: a lock read error or an
expired/stale lock does not prevent the builder from running. This avoids
blocking ingestion indefinitely when Couchbase is unavailable or an importer
container was terminated. A fresh `running` lock is still honored, so normal
imports give queued importers and VxIngest time to finish before downstream
calculations read the data. If the previous importer remains active beyond the
30-minute wait, the queued importer exits rather than running concurrently.

### Logging Levels

- **DEBUG**: Very verbose. Logs every file name being processed, every document processed, empty data conditions, successful individual document operations, and a pretty-printed list of successfully upserted document IDs.
- **INFO** (default): Logs import start/completion, connection establishment, file processing completion, successes/failures summary, and error conditions.
- **WARN**: Only warnings and error conditions.
- **ERROR**: Only error conditions.

Example with DEBUG logging:

```bash
LOG_LEVEL=DEBUG ./vximporter \
  -conn "${HOME}/credentials" \
  -file "test.json"
```

Example with DEBUG logging in Docker:

```bash
docker run --rm \
  -e LOG_LEVEL=DEBUG \
  -v "${HOME}/credentials:/run/config/credentials:ro" \
  -v "$(pwd)/large_dataset.json:/data/import.json:ro" \
  vximporter:local
```

## Input File Format

The importer expects a top-level JSON array of objects. Input may be plain JSON or gzip-compressed JSON:

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
export CB_CONN="couchbase://127.0.0.1"
export CB_USER="user"
export CB_PASS="pass"
export CB_BUCKET="vxdata"
export CB_SCOPE="_default"
export CB_COLLECTION="test"
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

- Verify the credentials file path, Couchbase host, and cluster reachability.
- Confirm bucket/scope/collection in the credentials file exist and the credentials are authorized.

File errors:

- Ensure `-file` path exists and is readable.
- If the path ends in `.gz`, ensure it decompresses to a top-level JSON array rather than a tar archive or line-delimited JSON.
- For Docker, confirm the host file mount source path is correct.

Import failures:

- Validate the file is a JSON array of JSON objects.
- Ensure every document includes a non-empty `id` field.
- Review logs for bulk operation warnings and per-document errors.
- Lower `-workers` and `-batch-size` if cluster saturation is suspected.

## Security Notes

- Keep the credentials YAML file readable only by its owner.
- Consider using environment-specific secret management in CI/CD.
- Restrict Couchbase user permissions to only required buckets/scopes/collections.
