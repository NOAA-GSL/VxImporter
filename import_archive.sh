#!/usr/bin/env bash
set -euo pipefail
# Host-side launcher: runs the vximporter container to process archives.
# Credentials come from ${HOME}/credentials mounted at /run/config/credentials.

export TZ="UTC0"

function usage {
    echo "Usage: $0 -l load_dir -a archive_dir [-w workers]" >&2
    echo "  -l  directory containing .tar.gz archives to import" >&2
    echo "  -a  archive directory for processed tarballs" >&2
    echo "  -w  concurrent import workers (default: 8)" >&2
    exit 1
}

function strip_trailing_slash {
    local path=$1
    [[ "${path}" != "/" ]] && path=${path%/}
    printf '%s' "${path}"
}

number_of_workers=8

while getopts 'a:l:w:' param; do
    case "${param}" in
        a)
            archive_dir=$(strip_trailing_slash "${OPTARG}")
            if [[ ! -d "${archive_dir}" ]]; then
                echo "ERROR: archive directory ${archive_dir} does not exist" >&2
                usage
            fi
        ;;
        l)
            load_dir=$(strip_trailing_slash "${OPTARG}")
            if [[ ! -d "${load_dir}" ]]; then
                echo "ERROR: load directory ${load_dir} does not exist" >&2
                usage
            fi
        ;;
        w)
            number_of_workers=${OPTARG}
            if ! [[ "${number_of_workers}" =~ ^[1-9][0-9]*$ ]]; then
                echo "ERROR: workers must be a positive integer" >&2
                usage
            fi
        ;;
        *)
        usage ;;
    esac
done

if [[ -z "${archive_dir:-}" ]] || [[ -z "${load_dir:-}" ]]; then
    echo "ERROR: -l and -a are required" >&2
    usage
fi

# Concurrency guard: refuse if more than 10 instances already running.
if command -v pgrep >/dev/null 2>&1; then
    running_jobs=$(pgrep -fc "$(basename "$0")")
else
    running_jobs=$(ps -elf | grep "$(basename "$0")" | grep -v grep | wc -l)
fi
if [[ "${running_jobs}" -gt 10 ]]; then
    echo "Too many jobs running — refusing this one" >&2
    exit 1
fi

# Run the container as the invoking user so file ownership matches the host.
# entrypoint.sh processes all tarballs in load_dir, moves them to archive_dir,
# and exits 0 if any succeeded, 1 if all failed, 2 if nothing to process.
container_exit=0
docker run --rm \
-v "${HOME}/credentials:/run/config/credentials:ro" \
-v "${load_dir}:/data/load" \
-v "${archive_dir}:/data/output" \
--tmpfs /data/tmp \
-e VX_WORKERS="${number_of_workers}" \
-e BUCKET_READY_TIMEOUT_SECONDS="${BUCKET_READY_TIMEOUT_SECONDS:-60}" \
--user "$(id -u):$(id -g)" \
vximporter:local || container_exit=$?

# there are multiple exit codes from the container: 0 = at least one import succeeded, 1 = all failed, 2 = nothing to do
case "${container_exit}" in
    0) echo "Import complete" ;;
    1) echo "Import failed (all archives failed)" >&2 ;;
    2) echo "No archives to process" >&2 ;;
    *) echo "Import failed (container exit ${container_exit})" >&2 ;;
esac

exit "${container_exit}"
