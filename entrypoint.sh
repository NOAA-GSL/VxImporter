#!/usr/bin/env bash
set -euo pipefail
# If args are provided, they are passed directly to vximporter.
# Otherwise auto-detect mode processes .tar.gz archives in /data/load, or a
# single /data/import.json or /data/import.json.gz file. Credentials are read
# from /run/config/credentials.
# Exit 0 = at least one import succeeded. Exit 1 = all failed. Exit 2 = nothing to do.

CREDS="${CREDENTIALS_FILE:-/run/config/credentials}"
LOAD_DIR="${VX_LOAD_DIR:-/data/load}"
OUTPUT_DIR="${VX_OUTPUT_PATH:-/data/output}"
TMP_DIR="${VX_TMP_DIR:-/data/tmp}"
IMPORT_FILE="${VX_IMPORT_FILE:-/data/import.json}"
IMPORT_GZIP_FILE="${VX_IMPORT_GZIP_FILE:-${IMPORT_FILE}.gz}"
WORKERS="${VX_WORKERS:-8}"
BATCH_SIZE="${VX_BATCH_SIZE:-500}"
VXIMPORTER_BIN="${VXIMPORTER_BIN:-/usr/local/bin/vximporter}"

if [[ "$#" -gt 0 ]]; then
    exec "${VXIMPORTER_BIN}" "$@"
fi

get_credential_value() {
    local key=$1 file=$2
    awk -v search_key="${key}" '
    /^[[:space:]]*#/ { next }
    {
      line = $0; sub(/^[[:space:]]+/, "", line)
      if (line ~ ("^" search_key "([[:space:]]*:|[[:space:]])")) {
        sub("^" search_key "[[:space:]]*:?", "", line)
        sub(/^[[:space:]]+/, "", line)
        split(line, parts, /[[:space:]]+/)
        print parts[1]; exit
      }
    }' "${file}"
}

runtime_creds=""
cleanup() {
    if [[ -n "${runtime_creds}" && -f "${runtime_creds}" ]]; then
        rm -f "${runtime_creds}"
    fi
}
trap cleanup EXIT

build_runtime_creds() {
    local cb_host="" cb_user="" cb_pwd="" bucket="" scope="" collection="" timeout=""

    if [[ -f "${CREDS}" ]]; then
        cb_host=$(get_credential_value cb_host "${CREDS}")
        cb_user=$(get_credential_value cb_user "${CREDS}")
        cb_pwd=$(get_credential_value cb_password "${CREDS}")
        bucket=$(get_credential_value cb_bucket "${CREDS}")
        scope=$(get_credential_value cb_scope "${CREDS}")
        collection=$(get_credential_value cb_collection "${CREDS}")
        timeout=$(get_credential_value cb_timeout_seconds "${CREDS}")
    fi

    # CB_* env vars override credentials file values.
    cb_host="${CB_HOST:-${cb_host}}"
    cb_user="${CB_USER:-${cb_user}}"
    cb_pwd="${CB_PASS:-${cb_pwd}}"
    bucket="${CB_BUCKET:-${bucket}}"
    scope="${CB_SCOPE:-${scope}}"
    collection="${CB_COLLECTION:-${collection}}"
    timeout="${CB_TIMEOUT_SECONDS:-${timeout}}"

    for _field in cb_host:"${cb_host}" cb_user:"${cb_user}" cb_password:"${cb_pwd}" cb_bucket:"${bucket}" cb_collection:"${collection}" cb_scope:"${scope}"; do
        _key=${_field%%:*}
        _val=${_field#*:}
        if [[ -z "${_val}" ]]; then
            echo "ERROR: missing required credential field ${_key}; set it in ${CREDS} or via CB_* env vars" >&2
            exit 1
        fi
    done

    runtime_creds=$(mktemp /tmp/vximporter-creds.XXXXXX)
    {
        echo "cb_host: ${cb_host}"
        echo "cb_user: ${cb_user}"
        echo "cb_password: ${cb_pwd}"
        echo "cb_bucket: ${bucket}"
        echo "cb_scope: ${scope}"
        echo "cb_collection: ${collection}"
        if [[ -n "${timeout}" ]]; then
            echo "cb_timeout_seconds: ${timeout}"
        fi
    } >"${runtime_creds}"
    chmod 600 "${runtime_creds}"

    CREDS="${runtime_creds}"
}

build_runtime_creds

run_import() {
    "${VXIMPORTER_BIN}" \
    -conn "${CREDS}" \
    -file "$1" \
    -workers "${WORKERS}" \
    -batch-size "${BATCH_SIZE}"
}

archive_has_unsafe_paths() {
    local archive=$1 entry
    while IFS= read -r entry; do
        [[ -z "${entry}" ]] && continue
        if [[ "${entry}" == /* ]] || [[ "${entry}" == ".." ]] || [[ "${entry}" == ../* ]] || \
        [[ "${entry}" == */../* ]] || [[ "${entry}" == */.. ]]; then
            echo "ERROR: tarball ${archive} contains unsafe path '${entry}'" >&2
            return 0
        fi
    done < <(tar -tzf "${archive}")
    return 1
}

# --- Mode detection ---
mkdir -p "${OUTPUT_DIR}" "${TMP_DIR}"

tar_files=("${LOAD_DIR}"/*.tar.gz)
[[ "${tar_files[0]}" == "${LOAD_DIR}/*.tar.gz" ]] && tar_files=()

if [[ "${#tar_files[@]}" -gt 0 ]]; then
    # Archive mode: extract each tarball, import its JSON files, move tarball to output.
    success_count=0
    failed_count=0

    for f in "${tar_files[@]}"; do
        base_f=$(basename "${f}")
        echo "--- processing ${base_f} ---"

        if archive_has_unsafe_paths "${f}"; then
            mv "${f}" "${OUTPUT_DIR}/failed-unsafe-archive-${base_f}"
            failed_count=$((failed_count + 1))
            continue
        fi

        # busybox mktemp uses -p instead of --tmpdir
        t_dir=$(mktemp -d -p "${TMP_DIR}")
        if ! tar -xzf "${f}" -C "${t_dir}"; then
            echo "ERROR: failed to extract ${f}" >&2
            mv "${f}" "${OUTPUT_DIR}/failed-extract-${base_f}"
            rm -rf "${t_dir}"
            failed_count=$((failed_count + 1))
            continue
        fi

        json_files=()
        while IFS= read -r json_file; do
            json_files+=("${json_file}")
        done < <(find "${t_dir}" -type f \( -name '*.json' -o -name '*.json.gz' \) -print | sort)
        if [[ "${#json_files[@]}" -eq 0 ]]; then
            echo "ERROR: no JSON or JSON.GZ files found in ${f}" >&2
            mv "${f}" "${OUTPUT_DIR}/failed-no-json-${base_f}"
            rm -rf "${t_dir}"
            failed_count=$((failed_count + 1))
            continue
        fi

        import_exit=0
        for json_f in "${json_files[@]}"; do
            run_import "${json_f}" || import_exit=$?
            [[ "${import_exit}" -ne 0 ]] && break
        done
        rm -rf "${t_dir}"

        if [[ "${import_exit}" -ne 0 ]]; then
            echo "import failed for ${f} exit_code:${import_exit}" >&2
            mv "${f}" "${OUTPUT_DIR}/failed-import-${base_f}"
            failed_count=$((failed_count + 1))
        else
            mv "${f}" "${OUTPUT_DIR}/success-${base_f}"
            success_count=$((success_count + 1))
        fi
        echo "--------"
    done

    echo "Archive import complete: ${success_count} succeeded, ${failed_count} failed"
    if [[ "${success_count}" -gt 0 ]]; then exit 0; else exit 1; fi

    elif [[ -f "${IMPORT_FILE}" ]]; then
    # Single-file mode
    run_import "${IMPORT_FILE}"

    elif [[ -f "${IMPORT_GZIP_FILE}" ]]; then
    # Single-file gzip mode
    run_import "${IMPORT_GZIP_FILE}"

else
    echo "Nothing to process: no .tar.gz files in ${LOAD_DIR} and no ${IMPORT_FILE} or ${IMPORT_GZIP_FILE} present" >&2
    exit 2
fi
