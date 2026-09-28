package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
)

// testLogger returns a discarding logger for tests to avoid output pollution.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordedImportLockWriter struct {
	id    string
	value map[string]interface{}
	err   error
}

func (writer *recordedImportLockWriter) Upsert(id string, value interface{}, _ *gocb.UpsertOptions) (*gocb.MutationResult, error) {
	writer.id = id
	writer.value = value.(map[string]interface{})
	return nil, writer.err
}

type recordedImportLockStore struct {
	recordedImportLockWriter
	status string
}

func (store *recordedImportLockStore) Get(_ string, _ *gocb.GetOptions) (*gocb.GetResult, error) {
	return nil, gocb.ErrDocumentNotFound
}

func (store *recordedImportLockStore) Insert(id string, value interface{}, _ *gocb.InsertOptions) (*gocb.MutationResult, error) {
	store.id = id
	store.value = value.(map[string]interface{})
	store.status = store.value["status"].(string)
	return nil, store.err
}

func (store *recordedImportLockStore) Replace(id string, value interface{}, _ *gocb.ReplaceOptions) (*gocb.MutationResult, error) {
	store.id = id
	store.value = value.(map[string]interface{})
	store.status = store.value["status"].(string)
	return nil, store.err
}

func TestAcquireImportLock_InsertsWhenAbsent(t *testing.T) {
	store := &recordedImportLockStore{}
	if err := acquireImportLock(store, "vximporter:test:1"); err != nil {
		t.Fatalf("acquireImportLock returned error: %v", err)
	}
	if store.id != importLockDocID || store.status != "running" {
		t.Fatalf("unexpected lock write: id=%q status=%q", store.id, store.status)
	}
}

func TestImportLockIsAvailable_RejectsActiveStatus(t *testing.T) {
	if importLockIsAvailable("running") {
		t.Fatal("expected running lock to be unavailable")
	}
	if !importLockIsAvailable("idle") {
		t.Fatal("expected idle lock to be available")
	}
}

func TestImportLockIsStale_UsesHeartbeatAge(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	fresh := map[string]interface{}{"status": "running", "updated": float64(now.Unix() - int64(importLockStaleAfter.Seconds()) + 1)}
	stale := map[string]interface{}{"status": "running", "updated": float64(now.Unix() - int64(importLockStaleAfter.Seconds()) - 1)}

	if importLockIsStale(fresh, now) {
		t.Fatal("expected fresh heartbeat to remain active")
	}
	if !importLockIsStale(stale, now) {
		t.Fatal("expected old heartbeat to be stale")
	}
}

func TestUpdateImportLock_WritesSharedLockDocument(t *testing.T) {
	writer := &recordedImportLockWriter{}
	if err := updateImportLock(writer, "running", "vximporter:test:1"); err != nil {
		t.Fatalf("updateImportLock returned error: %v", err)
	}

	if writer.id != importLockDocID {
		t.Fatalf("expected lock ID %q, got %q", importLockDocID, writer.id)
	}
	if writer.value["status"] != "running" {
		t.Fatalf("expected running status, got %q", writer.value["status"])
	}
	if writer.value["job_id"] != "vximporter:test:1" {
		t.Fatalf("unexpected job ID: %q", writer.value["job_id"])
	}
	if updated, ok := writer.value["updated"].(int64); !ok || updated <= 0 {
		t.Fatalf("expected a positive Unix timestamp, got %v", writer.value["updated"])
	}
}

func TestUpdateImportLock_ReturnsUpsertError(t *testing.T) {
	want := fmt.Errorf("couchbase unavailable")
	writer := &recordedImportLockWriter{err: want}
	if err := updateImportLock(writer, "idle", "vximporter:test:1"); err != want {
		t.Fatalf("expected %v, got %v", want, err)
	}
}

func TestLogSuccessfulDocIDs_DebugPrettyPrints(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logSuccessfulDocIDs(logger, []string{"doc-a", "doc-b"}, &out)

	want := "[\n  \"doc-a\",\n  \"doc-b\"\n]\n"
	if out.String() != want {
		t.Fatalf("unexpected pretty printed IDs:\nwant %q\n got %q", want, out.String())
	}
}

func TestLogSuccessfulDocIDs_InfoSuppressesList(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logSuccessfulDocIDs(logger, []string{"doc-a"}, &out)

	if out.Len() != 0 {
		t.Fatalf("expected INFO level to suppress document ID list, got %q", out.String())
	}
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
		"-collection", "override-collection",
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
	if cfg.Collection != "override-collection" {
		t.Fatalf("unexpected collection value: %q", cfg.Collection)
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
