package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"
	"gopkg.in/yaml.v3"
)

type compositeReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (c compositeReadCloser) Close() error {
	var firstErr error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Credentials holds the Couchbase connection parameters read from the credentials YAML file.
// Cb_timeout_seconds defaults to 3600 when zero or absent.
type Credentials struct {
	Cb_host            string `yaml:"cb_host"`
	Cb_user            string `yaml:"cb_user"`
	Cb_password        string `yaml:"cb_password"`
	Cb_bucket          string `yaml:"cb_bucket"`
	Cb_scope           string `yaml:"cb_scope"`
	Cb_collection      string `yaml:"cb_collection"`
	Cb_timeout_seconds int    `yaml:"cb_timeout_seconds"`
}

// Values are populated from CLI flags in parseFlags.
type Config struct {
	ConnStr    string
	FilePath   string
	BatchSize  int
	NumWorkers int
}

// CbConnection bundles the live Couchbase handles needed for queries.
// vxDBTARGET is the N1QL FROM target in "bucket.scope.collection" form.
type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
	vxDBTARGET string
}

// main wires together flag parsing, Couchbase connectivity, file streaming,
// worker scheduling, and final import metrics.
func main() {
	// Parse CLI flags
	cfg := parseFlags()
	// 1. Connect to Couchbase Cluster
	credentials := getCredentials(cfg.ConnStr)
	cbCon := getDbConnection(credentials)
	defer cbCon.Cluster.Close(nil)

	// Obtain handle to target collection from credentials file values.
	bucketName := credentials.Cb_bucket
	scopeName := credentials.Cb_scope
	collectionName := credentials.Cb_collection
	collection := cbCon.Bucket.Scope(scopeName).Collection(collectionName)

	// 2. Open File Stream
	file, err := openInputReader(cfg.FilePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	log.Printf("Starting import into '%s.%s.%s' using %d workers (batch size: %d)... from filePath: %s",
		bucketName, scopeName, collectionName, cfg.NumWorkers, cfg.BatchSize, cfg.FilePath)

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
	encodeErr := enqueueJSONArrayBatches(file, cfg.BatchSize, jobs)
	close(jobs)

	// Wait for all workers to complete before reporting or exiting.
	wg.Wait()

	if encodeErr != nil {
		log.Fatalf("Failed while decoding input file: %v", encodeErr)
	}

	duration := time.Since(start)
	totalDocs := totalSuccess + totalFailed
	opsPerSec := float64(totalDocs) / duration.Seconds()

	log.Printf("--- Import Complete ---")
	log.Printf("Time Elapsed: %v", duration)
	log.Printf("Successful:   %d docs", totalSuccess)
	log.Printf("Failed:       %d docs", totalFailed)
	log.Printf("Throughput:   %.2f ops/sec", opsPerSec)
}

// openInputReader opens the import file and transparently decompresses gzip input.
func openInputReader(filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		return file, nil
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("open gzip reader: %w", err)
	}

	return compositeReadCloser{
		Reader:  gzipReader,
		closers: []io.Closer{gzipReader, file},
	}, nil
}

// safeIdentRe restricts query parameter substitution to prevent SQL injection.
var safeIdentRe = regexp.MustCompile(`^[a-zA-Z0-9_.:\-/]+$`)

// validateQueryParam fatals if value contains characters outside [a-zA-Z0-9_.:\-/],
// preventing SQL injection via template substitution.
func validateQueryParam(name, value string) {
	if value != "" && !safeIdentRe.MatchString(value) {
		log.Fatalf("query parameter %q contains invalid characters: %q", name, value)
	}
}

// getCredentials reads and YAML-decodes the credentials file.
// It fatals if the file has permissions readable by group or others (mode & 0o077 != 0).
func getCredentials(credentialsFilePath string) Credentials {
	info, err := os.Stat(credentialsFilePath)
	if err != nil {
		log.Fatalf("unable to stat credentials file %q: %v", credentialsFilePath, err)
	}
	// reject credentials files readable by group or others
	if info.Mode().Perm()&0o077 != 0 {
		log.Fatalf("credentials file %q has insecure permissions %04o; chmod 600 to fix", credentialsFilePath, info.Mode().Perm())
	}

	creds := Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		log.Fatalf("unable to read credentials file %q: %v", credentialsFilePath, err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	if creds.Cb_host == "" || creds.Cb_user == "" || creds.Cb_password == "" || creds.Cb_bucket == "" {
		log.Fatal("credentials file missing required fields: cb_host, cb_user, cb_password, cb_bucket")
	}
	if creds.Cb_scope == "" {
		creds.Cb_scope = "_default"
	}
	if creds.Cb_collection == "" {
		creds.Cb_collection = "_default"
	}
	return creds
}

// getDbConnection opens a Couchbase cluster connection using the supplied credentials.
// It waits up to a configurable timeout for the bucket to become ready before returning.
func getDbConnection(cred Credentials) (conn CbConnection) {
	log.Println("getDbConnection()")

	conn = CbConnection{}
	connectionString := cred.Cb_host
	bucketName := cred.Cb_bucket
	collection := cred.Cb_collection
	username := cred.Cb_user
	password := cred.Cb_password
	timeout := cred.Cb_timeout_seconds
	if timeout <= 0 {
		timeout = 3600
	}
	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			QueryTimeout: time.Duration(timeout) * time.Second,
		},
	}

	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		log.Fatal(err)
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.Collection = conn.Bucket.Collection(collection)
	conn.vxDBTARGET = cred.Cb_bucket + "." + cred.Cb_scope + "." + cred.Cb_collection
	validateQueryParam("vxDBTARGET", conn.vxDBTARGET)

	log.Println("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(bucketReadyTimeout(), nil)
	if err != nil {
		log.Fatal(err)
	}

	conn.Scope = conn.Bucket.Scope(cred.Cb_scope)
	return conn
}

// bucketReadyTimeout returns the wait duration for Bucket.WaitUntilReady.
// BUCKET_READY_TIMEOUT_SECONDS can override the default for slower remote clusters.
func bucketReadyTimeout() time.Duration {
	seconds := 60
	raw := strings.TrimSpace(os.Getenv("BUCKET_READY_TIMEOUT_SECONDS"))
	if raw == "" {
		return time.Duration(seconds) * time.Second
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		log.Printf("invalid BUCKET_READY_TIMEOUT_SECONDS=%q, using default %d", raw, seconds)
		return time.Duration(seconds) * time.Second
	}

	return time.Duration(parsed) * time.Second
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
	cfg, err := parseFlagsFromArgs(os.Args[1:])
	if err != nil {
		log.Fatalf("Failed to parse flags: %v", err)
	}
	return cfg
}

// parseFlagsFromArgs defines and parses CLI flags from the provided args.
//
// A dedicated parser function enables deterministic unit tests without mutating
// global process arguments.
func parseFlagsFromArgs(args []string) (*Config, error) {
	cfg := &Config{}
	fs := flag.NewFlagSet("vximporter", flag.ContinueOnError)
	fs.StringVar(&cfg.ConnStr, "conn", os.Getenv("VX_CREDENTIALS_FILE"), "Path to credentials YAML file (env: VX_CREDENTIALS_FILE)")
	fs.StringVar(&cfg.FilePath, "file", "data.json", "Path to JSON array file")
	fs.IntVar(&cfg.BatchSize, "batch-size", 500, "Documents per bulk request")
	fs.IntVar(&cfg.NumWorkers, "workers", 8, "Number of concurrent goroutines")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.ConnStr) == "" {
		return nil, fmt.Errorf("missing credentials file path; set -conn or VX_CREDENTIALS_FILE")
	}
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("invalid -workers value; must be greater than 0")
	}
	return cfg, nil
}
