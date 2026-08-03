package main

import (
	"strings"
	"testing"
)

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

func TestEnqueueJSONArrayBatches_StreamsAllDocuments(t *testing.T) {
	input := `[{"id":"a"},{"id":"b"},{"id":"c"}]`
	jobs := make(chan []map[string]interface{}, 4)

	err := enqueueJSONArrayBatches(strings.NewReader(input), 2, jobs)
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

func TestEnqueueJSONArrayBatches_RejectsNonArrayInput(t *testing.T) {
	err := enqueueJSONArrayBatches(strings.NewReader(`{"id":"x"}`), 2, make(chan []map[string]interface{}, 1))
	if err == nil {
		t.Fatalf("expected error for non-array input")
	}
}

func TestParseFlagsFromArgs_Defaults(t *testing.T) {
	cfg := parseFlagsFromArgs([]string{})

	if cfg.ConnStr != "couchbase://127.0.0.1" {
		t.Fatalf("unexpected conn default: %q", cfg.ConnStr)
	}
	if cfg.Username != "Administrator" {
		t.Fatalf("unexpected user default: %q", cfg.Username)
	}
	if cfg.Password != "password" {
		t.Fatalf("unexpected pass default: %q", cfg.Password)
	}
	if cfg.BucketName != "default" {
		t.Fatalf("unexpected bucket default: %q", cfg.BucketName)
	}
	if cfg.ScopeName != "_default" {
		t.Fatalf("unexpected scope default: %q", cfg.ScopeName)
	}
	if cfg.CollectionName != "_default" {
		t.Fatalf("unexpected collection default: %q", cfg.CollectionName)
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

func TestParseFlagsFromArgs_Overrides(t *testing.T) {
	cfg := parseFlagsFromArgs([]string{
		"-conn", "couchbase://cluster.local",
		"-user", "alice",
		"-pass", "secret",
		"-bucket", "travel-sample",
		"-scope", "inventory",
		"-collection", "airline",
		"-file", "input.json",
		"-batch-size", "1000",
		"-workers", "16",
	})

	if cfg.ConnStr != "couchbase://cluster.local" {
		t.Fatalf("unexpected conn value: %q", cfg.ConnStr)
	}
	if cfg.Username != "alice" {
		t.Fatalf("unexpected user value: %q", cfg.Username)
	}
	if cfg.Password != "secret" {
		t.Fatalf("unexpected pass value: %q", cfg.Password)
	}
	if cfg.BucketName != "travel-sample" {
		t.Fatalf("unexpected bucket value: %q", cfg.BucketName)
	}
	if cfg.ScopeName != "inventory" {
		t.Fatalf("unexpected scope value: %q", cfg.ScopeName)
	}
	if cfg.CollectionName != "airline" {
		t.Fatalf("unexpected collection value: %q", cfg.CollectionName)
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
