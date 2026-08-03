package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"
)

// Config contains runtime settings for the importer.
//
// Values are populated from CLI flags in parseFlags.
type Config struct {
	ConnStr        string
	Username       string
	Password       string
	BucketName     string
	ScopeName      string
	CollectionName string
	FilePath       string
	BatchSize      int
	NumWorkers     int
}

// main wires together flag parsing, Couchbase connectivity, file streaming,
// worker scheduling, and final import metrics.
func main() {
	// Parse CLI flags
	cfg := parseFlags()

	// 1. Connect to Couchbase Cluster
	cluster, err := gocb.Connect(cfg.ConnStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		},
		// Optimize timeout & connections for heavy ingestion workloads
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: 10 * time.Second,
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect to cluster: %v", err)
	}
	defer cluster.Close(nil)

	// Obtain handle to target collection
	bucket := cluster.Bucket(cfg.BucketName)
	if err := bucket.WaitUntilReady(5*time.Second, nil); err != nil {
		log.Fatalf("Bucket ready check failed: %v", err)
	}
	collection := bucket.Scope(cfg.ScopeName).Collection(cfg.CollectionName)

	// 2. Open File Stream
	file, err := os.Open(cfg.FilePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	log.Printf("Starting import into '%s.%s.%s' using %d workers (batch size: %d)...",
		cfg.BucketName, cfg.ScopeName, cfg.CollectionName, cfg.NumWorkers, cfg.BatchSize)

	start := time.Now()

	// Buffered job channel keeps workers fed while decoder reads from disk.
	jobs := make(chan []map[string]interface{}, cfg.NumWorkers*2)

	// Counters are aggregated across all workers.
	var totalSuccess uint64
	var totalFailed uint64

	// 3. Start Worker Pool
	var wg sync.WaitGroup
	for w := 1; w <= cfg.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s, f := workerTask(collection, jobs)
			atomic.AddUint64(&totalSuccess, s)
			atomic.AddUint64(&totalFailed, f)
		}(w)
	}

	// 4. Producer: stream JSON array elements into batch jobs.
	if err := enqueueJSONArrayBatches(file, cfg.BatchSize, jobs); err != nil {
		log.Fatalf("Failed while decoding input file: %v", err)
	}
	close(jobs)

	// Wait for all workers to complete
	wg.Wait()

	duration := time.Since(start)
	totalDocs := totalSuccess + totalFailed
	opsPerSec := float64(totalDocs) / duration.Seconds()

	log.Printf("--- Import Complete ---")
	log.Printf("Time Elapsed: %v", duration)
	log.Printf("Successful:   %d docs", totalSuccess)
	log.Printf("Failed:       %d docs", totalFailed)
	log.Printf("Throughput:   %.2f ops/sec", opsPerSec)
}

// workerTask consumes document batches from jobs, enforces id-based keys,
// performs a bulk upsert, and returns per-worker success/failure counters.
func workerTask(collection *gocb.Collection, jobs <-chan []map[string]interface{}) (uint64, uint64) {
	var successCount, failCount uint64

	for batch := range jobs {
		ops := make([]gocb.BulkOp, 0, len(batch))

		// Build upsert operations for documents that contain a valid id field.
		for _, doc := range batch {
			docID, ok := extractDocID(doc)
			if !ok {
				failCount++
				continue
			}

			ops = append(ops, &gocb.UpsertOp{
				ID:    docID,
				Value: doc,
			})
		}

		if len(ops) == 0 {
			continue
		}

		// Execute one network call for all operations in the current batch.
		err := collection.Do(ops, &gocb.BulkOpOptions{
			Context: context.Background(),
		})
		if err != nil {
			log.Printf("Warning: Bulk operation execution error: %v", err)
		}

		// Each operation carries its own result error.
		for _, op := range ops {
			upsertOp := op.(*gocb.UpsertOp)
			if upsertOp.Err != nil {
				failCount++
			} else {
				successCount++
			}
		}
	}

	return successCount, failCount
}

// enqueueJSONArrayBatches validates that the input is a JSON array and streams
// each element into batch jobs without loading the full array in memory.
func enqueueJSONArrayBatches(r io.Reader, batchSize int, jobs chan<- []map[string]interface{}) error {
	if batchSize <= 0 {
		batchSize = 1
	}

	dec := json.NewDecoder(r)

	start, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := start.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("input must be a JSON array")
	}

	batch := make([]map[string]interface{}, 0, batchSize)
	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			return err
		}

		batch = append(batch, doc)
		if len(batch) >= batchSize {
			jobs <- batch
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	end, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok = end.(json.Delim)
	if !ok || delim != ']' {
		return fmt.Errorf("input must end with ]")
	}

	if len(batch) > 0 {
		jobs <- batch
	}

	return nil
}

// extractDocID returns the Couchbase key from the document id field.
//
// The id field is required and must resolve to a non-empty string value.
func extractDocID(doc map[string]interface{}) (string, bool) {
	idVal, exists := doc["id"]
	if !exists || idVal == nil {
		return "", false
	}

	id := strings.TrimSpace(fmt.Sprintf("%v", idVal))
	if id == "" || id == "<nil>" {
		return "", false
	}

	return id, true
}

// parseFlags defines and parses CLI flags into a Config instance.
func parseFlags() *Config {
	return parseFlagsFromArgs(os.Args[1:])
}

// parseFlagsFromArgs defines and parses CLI flags from the provided args.
//
// A dedicated parser function enables deterministic unit tests without mutating
// global process arguments.
func parseFlagsFromArgs(args []string) *Config {
	cfg := &Config{}
	fs := flag.NewFlagSet("vximporter", flag.ContinueOnError)
	fs.StringVar(&cfg.ConnStr, "conn", "couchbase://127.0.0.1", "Couchbase connection string")
	fs.StringVar(&cfg.Username, "user", "Administrator", "Database Username")
	fs.StringVar(&cfg.Password, "pass", "password", "Database Password")
	fs.StringVar(&cfg.BucketName, "bucket", "default", "Target Bucket")
	fs.StringVar(&cfg.ScopeName, "scope", "_default", "Target Scope")
	fs.StringVar(&cfg.CollectionName, "collection", "_default", "Target Collection")
	fs.StringVar(&cfg.FilePath, "file", "data.json", "Path to JSON array file")
	fs.IntVar(&cfg.BatchSize, "batch-size", 500, "Documents per bulk request")
	fs.IntVar(&cfg.NumWorkers, "workers", 8, "Number of concurrent goroutines")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("Failed to parse flags: %v", err)
	}
	return cfg
}
