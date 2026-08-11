package main

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestEntrypoint_PassthroughArgs(t *testing.T) {
	tempDir := t.TempDir()
	argsFile := filepath.Join(tempDir, "args.txt")
	stubPath := writeImporterStub(t, tempDir, argsFile, "")

	cmd := exec.Command("bash", "entrypoint.sh", "-conn", "/run/config/credentials", "-file", "/data/test.json.gz")
	cmd.Env = append(os.Environ(),
		"VXIMPORTER_BIN="+stubPath,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("entrypoint passthrough failed: %v\n%s", err, output)
	}

	args := readTrimmedFile(t, argsFile)
	if args != "-conn /run/config/credentials -file /data/test.json.gz" {
		t.Fatalf("unexpected passthrough args: %q", args)
	}
}

func TestEntrypoint_AutoDetectSingleGzipFile(t *testing.T) {
	tempDir := t.TempDir()
	argsFile := filepath.Join(tempDir, "args.txt")
	connSnapshot := filepath.Join(tempDir, "conn.yaml")
	stubPath := writeImporterStub(t, tempDir, argsFile, connSnapshot)
	credsPath := writeCredentialsFile(t, tempDir)
	importBase := filepath.Join(tempDir, "import.json")
	importGzipPath := importBase + ".gz"
	writeGzipJSONFile(t, importGzipPath, `[{"id":"gz-doc"}]`)

	cmd := exec.Command("bash", "entrypoint.sh")
	cmd.Env = append(os.Environ(),
		"VXIMPORTER_BIN="+stubPath,
		"CREDENTIALS_FILE="+credsPath,
		"VX_IMPORT_FILE="+importBase,
		"VX_OUTPUT_PATH="+filepath.Join(tempDir, "output"),
		"VX_TMP_DIR="+filepath.Join(tempDir, "tmp"),
		"VX_WORKERS=3",
		"VX_BATCH_SIZE=42",
		"CB_HOST=",
		"CB_USER=",
		"CB_PASS=",
		"CB_BUCKET=",
		"CB_SCOPE=",
		"CB_COLLECTION=",
		"CB_TIMEOUT_SECONDS=",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("entrypoint gzip auto-detect failed: %v\n%s", err, output)
	}

	args := readTrimmedFile(t, argsFile)
	if !strings.Contains(args, "-file "+importGzipPath) {
		t.Fatalf("expected gzip import path in args, got %q", args)
	}
	if !strings.Contains(args, "-workers 3") {
		t.Fatalf("expected workers override in args, got %q", args)
	}
	if !strings.Contains(args, "-batch-size 42") {
		t.Fatalf("expected batch-size override in args, got %q", args)
	}

	connSnapshotContents := readTrimmedFile(t, connSnapshot)
	if !strings.Contains(connSnapshotContents, "cb_host: couchbase://127.0.0.1") {
		t.Fatalf("expected generated credentials file contents, got %q", connSnapshotContents)
	}
	if !strings.Contains(connSnapshotContents, "cb_collection: test") {
		t.Fatalf("expected generated credentials collection, got %q", connSnapshotContents)
	}
}

func TestEntrypoint_AutoDetectArchiveWithGzipJSON(t *testing.T) {
	tempDir := t.TempDir()
	argsFile := filepath.Join(tempDir, "args.txt")
	stubPath := writeImporterStub(t, tempDir, argsFile, "")
	credsPath := writeCredentialsFile(t, tempDir)
	loadDir := filepath.Join(tempDir, "load")
	outputDir := filepath.Join(tempDir, "output")
	tmpDir := filepath.Join(tempDir, "tmp")
	if err := os.MkdirAll(loadDir, 0o755); err != nil {
		t.Fatalf("mkdir load dir: %v", err)
	}
	archivePath := filepath.Join(loadDir, "sample.tar.gz")
	writeTarGzWithFile(t, archivePath, "nested/data.json.gz", gzipJSONPayload(t, `[{"id":"archived"}]`))

	cmd := exec.Command("bash", "entrypoint.sh")
	cmd.Env = append(os.Environ(),
		"VXIMPORTER_BIN="+stubPath,
		"CREDENTIALS_FILE="+credsPath,
		"VX_LOAD_DIR="+loadDir,
		"VX_OUTPUT_PATH="+outputDir,
		"VX_TMP_DIR="+tmpDir,
		"CB_HOST=",
		"CB_USER=",
		"CB_PASS=",
		"CB_BUCKET=",
		"CB_SCOPE=",
		"CB_COLLECTION=",
		"CB_TIMEOUT_SECONDS=",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("entrypoint archive auto-detect failed: %v\n%s", err, output)
	}

	args := readTrimmedFile(t, argsFile)
	if !strings.Contains(args, ".json.gz") {
		t.Fatalf("expected extracted .json.gz path in args, got %q", args)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "success-sample.tar.gz")); err != nil {
		t.Fatalf("expected archive to be moved to success path: %v", err)
	}
}

func writeImporterStub(t *testing.T, tempDir, argsFile, connSnapshot string) string {
	t.Helper()
	stubPath := filepath.Join(tempDir, "stub-vximporter.sh")
	content := "#!/usr/bin/env bash\n" +
		"set -euo pipefail\n" +
		"printf '%s' \"$*\" > \"$STUB_ARGS_FILE\"\n" +
		"for ((i=1; i<=$#; i++)); do\n" +
		"  if [[ \"${!i}\" == '-conn' ]]; then\n" +
		"    next=$((i+1))\n" +
		"    if [[ -n \"${STUB_CONN_SNAPSHOT:-}\" ]]; then cp \"${!next}\" \"$STUB_CONN_SNAPSHOT\"; fi\n" +
		"    break\n" +
		"  fi\n" +
		"done\n"
	if err := os.WriteFile(stubPath, []byte(content), 0o755); err != nil {
		t.Fatalf("write stub importer: %v", err)
	}
	t.Setenv("STUB_ARGS_FILE", argsFile)
	if connSnapshot != "" {
		t.Setenv("STUB_CONN_SNAPSHOT", connSnapshot)
	}
	return stubPath
}

func writeCredentialsFile(t *testing.T, tempDir string) string {
	t.Helper()
	credsPath := filepath.Join(tempDir, "credentials.yaml")
	content := strings.Join([]string{
		"cb_host: couchbase://127.0.0.1",
		"cb_user: user",
		"cb_password: pass",
		"cb_bucket: vxdata",
		"cb_scope: _default",
		"cb_collection: test",
	}, "\n") + "\n"
	if err := os.WriteFile(credsPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write credentials file: %v", err)
	}
	return credsPath
}

func writeGzipJSONFile(t *testing.T, filePath, payload string) {
	t.Helper()
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create gzip file: %v", err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	if _, err := gzipWriter.Write([]byte(payload)); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
}

func gzipJSONPayload(t *testing.T, payload string) []byte {
	t.Helper()
	var builder strings.Builder
	writer := gzip.NewWriter(&builder)
	if _, err := writer.Write([]byte(payload)); err != nil {
		t.Fatalf("gzip payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip payload writer: %v", err)
	}
	return []byte(builder.String())
}

func writeTarGzWithFile(t *testing.T, archivePath, name string, data []byte) {
	t.Helper()
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("create archive: %v", err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	header := &tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}
	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tarWriter.Write(data); err != nil {
		t.Fatalf("write tar file: %v", err)
	}
}

func readTrimmedFile(t *testing.T, filePath string) string {
	t.Helper()
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file %s: %v", filePath, err)
	}
	return strings.TrimSpace(string(data))
}
