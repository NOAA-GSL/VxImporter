package main

import (
	"compress/gzip"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testLogger returns a discarding logger for tests to avoid output pollution.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestExtractDocID_UsesIDFieldFirst(t *testing.T) {
	doc := map[string]interface{}{"id": "abc-123"}
	got, ok := extractDocID(doc)
	if !ok {
		t.Fatalf("expected id field to be accepted")
	}
	if got != "abc-123" {
		t.Fatalf("expected id field, got %q", got)
	}
}

func TestExtractDocID_RejectsMissingID(t *testing.T) {
	doc := map[string]interface{}{"_id": "doc-2"}
	_, ok := extractDocID(doc)
	if ok {
		t.Fatalf("expected missing id to be rejected")
	}
}

func TestExtractDocID_RejectsEmptyID(t *testing.T) {
	doc := map[string]interface{}{"id": "   "}
	_, ok := extractDocID(doc)
	if ok {
		t.Fatalf("expected empty id to be rejected")
	}
}

func TestExtractDocID_AcceptsNumericID(t *testing.T) {
	doc := map[string]interface{}{"id": 12345}
	got, ok := extractDocID(doc)
	if !ok {
		t.Fatalf("expected numeric id to be accepted")
	}
	if got != "12345" {
		t.Fatalf("expected converted numeric id, got %q", got)
	}
}

func TestExtractDocID_RejectsNilID(t *testing.T) {
	doc := map[string]interface{}{"id": nil}
	_, ok := extractDocID(doc)
	if ok {
		t.Fatalf("expected nil id to be rejected")
	}
}

func TestExtractDocID_AcceptsFloat64ID(t *testing.T) {
	// JSON decoder unmarshals numbers as float64; ensure no scientific notation.
	doc := map[string]interface{}{"id": float64(12345)}
	got, ok := extractDocID(doc)
	if !ok {
		t.Fatalf("expected float64 id to be accepted")
	}
	if got != "12345" {
		t.Fatalf("expected \"12345\", got %q", got)
	}
}

func TestEnqueueJSONArrayBatches_StreamsAllDocuments(t *testing.T) {
	input := `[{"id":"a"},{"id":"b"},{"id":"c"}]`
	jobs := make(chan []map[string]interface{}, 4)

	err := enqueueJSONArrayBatches(strings.NewReader(input), "test.json", 2, jobs, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)

	var gotIDs []string
	for batch := range jobs {
		for _, doc := range batch {
			id, ok := extractDocID(doc)
			if !ok {
				t.Fatalf("expected id in streamed document")
			}
			gotIDs = append(gotIDs, id)
		}
	}

	if len(gotIDs) != 3 {
		t.Fatalf("expected 3 documents, got %d", len(gotIDs))
	}
	if gotIDs[0] != "a" || gotIDs[1] != "b" || gotIDs[2] != "c" {
		t.Fatalf("unexpected id order/content: %#v", gotIDs)
	}
}

func TestEnqueueJSONArrayBatches_EmptyArray(t *testing.T) {
	jobs := make(chan []map[string]interface{}, 1)
	err := enqueueJSONArrayBatches(strings.NewReader(`[]`), "test.json", 2, jobs, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)
	if len(jobs) != 0 {
		t.Fatalf("expected no jobs for empty array")
	}
}

func TestEnqueueJSONArrayBatches_TrailingBatchFlushed(t *testing.T) {
	// 3 docs with batchSize 10 — all land in one trailing batch.
	input := `[{"id":"x"},{"id":"y"},{"id":"z"}]`
	jobs := make(chan []map[string]interface{}, 4)

	err := enqueueJSONArrayBatches(strings.NewReader(input), "test.json", 10, jobs, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)

	var total int
	for batch := range jobs {
		total += len(batch)
	}
	if total != 3 {
		t.Fatalf("expected 3 docs, got %d", total)
	}
}

func TestEnqueueJSONArrayBatches_ZeroBatchSizeClamped(t *testing.T) {
	// batchSize <= 0 should be treated as 1, not panic or hang.
	input := `[{"id":"a"},{"id":"b"}]`
	jobs := make(chan []map[string]interface{}, 4)

	err := enqueueJSONArrayBatches(strings.NewReader(input), "test.json", 0, jobs, testLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	close(jobs)

	var total int
	for batch := range jobs {
		total += len(batch)
	}
	if total != 2 {
		t.Fatalf("expected 2 docs, got %d", total)
	}
}

func TestEnqueueJSONArrayBatches_RejectsNonArrayInput(t *testing.T) {
	err := enqueueJSONArrayBatches(strings.NewReader(`{"id":"x"}`), "test.json", 2, make(chan []map[string]interface{}, 1), testLogger())
	if err == nil {
		t.Fatalf("expected error for non-array input")
	}
}

func TestOpenInputReader_GzipJSON(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "input.json.gz")

	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create gzip file: %v", err)
	}

	gzipWriter := gzip.NewWriter(file)
	if _, err := gzipWriter.Write([]byte(`[{"id":"gz-doc"}]`)); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	reader, err := openInputReader(filePath, testLogger())
	if err != nil {
		t.Fatalf("openInputReader returned error: %v", err)
	}
	defer reader.Close()

	jobs := make(chan []map[string]interface{}, 1)
	if err := enqueueJSONArrayBatches(reader, filePath, 10, jobs, testLogger()); err != nil {
		t.Fatalf("unexpected error decoding gz input: %v", err)
	}
	close(jobs)

	total := 0
	for batch := range jobs {
		total += len(batch)
	}
	if total != 1 {
		t.Fatalf("expected 1 document from gz input, got %d", total)
	}
}

func TestParseFlagsFromArgs_Defaults(t *testing.T) {
	t.Setenv("VX_CREDENTIALS_FILE", "/tmp/credentials-default.yaml")
	cfg, err := parseFlagsFromArgs([]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ConnStr != "/tmp/credentials-default.yaml" {
		t.Fatalf("unexpected conn default: %q", cfg.ConnStr)
	}
	if cfg.FilePath != "data.json" {
		t.Fatalf("unexpected file default: %q", cfg.FilePath)
	}
	if cfg.BatchSize != 500 {
		t.Fatalf("unexpected batch-size default: %d", cfg.BatchSize)
	}
	if cfg.NumWorkers != 8 {
		t.Fatalf("unexpected workers default: %d", cfg.NumWorkers)
	}
}

func TestParseFlagsFromArgs_EnvVarDefaults(t *testing.T) {
	t.Setenv("VX_CREDENTIALS_FILE", "/tmp/credentials-env.yaml")
	cfg, err := parseFlagsFromArgs([]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ConnStr != "/tmp/credentials-env.yaml" {
		t.Fatalf("expected VX_CREDENTIALS_FILE to set conn default, got %q", cfg.ConnStr)
	}
}

func TestParseFlagsFromArgs_Overrides(t *testing.T) {
	t.Setenv("VX_CREDENTIALS_FILE", "/tmp/credentials-env.yaml")
	cfg, err := parseFlagsFromArgs([]string{
		"-conn", "/tmp/credentials-override.yaml",
		"-file", "input.json",
		"-batch-size", "1000",
		"-workers", "16",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ConnStr != "/tmp/credentials-override.yaml" {
		t.Fatalf("unexpected conn value: %q", cfg.ConnStr)
	}
	if cfg.FilePath != "input.json" {
		t.Fatalf("unexpected file value: %q", cfg.FilePath)
	}
	if cfg.BatchSize != 1000 {
		t.Fatalf("unexpected batch-size value: %d", cfg.BatchSize)
	}
	if cfg.NumWorkers != 16 {
		t.Fatalf("unexpected workers value: %d", cfg.NumWorkers)
	}
}

func TestParseFlagsFromArgs_MissingConnPathReturnsError(t *testing.T) {
	t.Setenv("VX_CREDENTIALS_FILE", "")
	_, err := parseFlagsFromArgs([]string{})
	if err == nil {
		t.Fatalf("expected error when conn path is missing")
	}
}

func TestParseFlagsFromArgs_NonPositiveWorkersReturnsError(t *testing.T) {
	t.Setenv("VX_CREDENTIALS_FILE", "/tmp/credentials.yaml")
	_, err := parseFlagsFromArgs([]string{"-workers", "0"})
	if err == nil {
		t.Fatalf("expected error when workers <= 0")
	}
}
