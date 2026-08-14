package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
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

// initLogger configures the global slog logger based on LOG_LEVEL environment variable.
// Supported levels: DEBUG, INFO (default), WARN, ERROR
func initLogger() *slog.Logger {
	levelStr := strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	if levelStr == "" {
		levelStr = "INFO"
	}

	var level slog.Level
	switch levelStr {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		log.Printf("invalid LOG_LEVEL=%q, using INFO", levelStr)
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewTextHandler(os.Stderr, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
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
	// Initialize logger from LOG_LEVEL environment variable
	logger := initLogger()

	// Parse CLI flags
	cfg := parseFlags()
	logger.Debug("Configuration loaded", "connStr", cfg.ConnStr, "filePath", cfg.FilePath, "batchSize", cfg.BatchSize, "numWorkers", cfg.NumWorkers)

	// 1. Connect to Couchbase Cluster
	credentials := getCredentials(cfg.ConnStr, logger)
	cbCon := getDbConnection(credentials, logger)
	defer cbCon.Cluster.Close(nil)

	// Obtain handle to target collection from credentials file values.
	bucketName := credentials.Cb_bucket
	scopeName := credentials.Cb_scope
	collectionName := credentials.Cb_collection
	collection := cbCon.Bucket.Scope(scopeName).Collection(collectionName)

	// 2. Open File Stream
	file, err := openInputReader(cfg.FilePath, logger)
	if err != nil {
		logger.Error("Failed to open file", "filePath", cfg.FilePath, "error", err)
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	logger.Info("Starting import", "bucket", bucketName, "scope", scopeName, "collection", collectionName, "workers", cfg.NumWorkers, "batchSize", cfg.BatchSize, "filePath", cfg.FilePath)

	start := time.Now()

	// Buffered job channel keeps workers fed while decoder reads from disk.
	jobs := make(chan []map[string]interface{}, cfg.NumWorkers*2)

	// Counters are aggregated across all workers.
	var totalSuccess uint64
	var totalFailed uint64
	var successfulDocIDs []string
	var successfulDocIDsMu sync.Mutex

	// 3. Start Worker Pool
	var wg sync.WaitGroup
	for w := 1; w <= cfg.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s, f, ids := workerTask(collection, jobs, logger)
			atomic.AddUint64(&totalSuccess, s)
			atomic.AddUint64(&totalFailed, f)
			if len(ids) > 0 {
				successfulDocIDsMu.Lock()
				successfulDocIDs = append(successfulDocIDs, ids...)
				successfulDocIDsMu.Unlock()
			}
		}(w)
	}

	// 4. Producer: stream JSON array elements into batch jobs.
	encodeErr := enqueueJSONArrayBatches(file, cfg.FilePath, cfg.BatchSize, jobs, logger)
	close(jobs)

	// Wait for all workers to complete before reporting or exiting.
	wg.Wait()
	logSuccessfulDocIDs(logger, successfulDocIDs, os.Stderr)

	if encodeErr != nil {
		logger.Error("Failed while decoding input file", "filePath", cfg.FilePath, "error", encodeErr)
		log.Fatalf("Failed while decoding input file: %v", encodeErr)
	}

	duration := time.Since(start)
	totalDocs := totalSuccess + totalFailed
	opsPerSec := float64(totalDocs) / duration.Seconds()

	logger.Info("Import Complete", "timeElapsed", duration, "successful", totalSuccess, "failed", totalFailed, "throughput", fmt.Sprintf("%.2f ops/sec", opsPerSec))
}

func logSuccessfulDocIDs(logger *slog.Logger, docIDs []string, out io.Writer) {
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}

	logger.Debug("Successfully upserted document IDs", "count", len(docIDs))
	encodedDocIDs, err := json.MarshalIndent(docIDs, "", "  ")
	if err != nil {
		logger.Debug("Unable to pretty print upserted document IDs", "error", err)
		return
	}
	fmt.Fprintln(out, string(encodedDocIDs))
}

// openInputReader opens the import file and transparently decompresses gzip input.
func openInputReader(filePath string, logger *slog.Logger) (io.ReadCloser, error) {
	logger.Debug("Opening input file", "filePath", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		logger.Error("Unable to open file", "filePath", filePath, "error", err)
		return nil, err
	}

	if !strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		return file, nil
	}

	logger.Debug("File is gzip compressed, initializing decompression", "filePath", filePath)
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		logger.Error("Failed to create gzip reader", "filePath", filePath, "error", err)
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
		slog.Error("Query parameter contains invalid characters", "param", name, "value", value)
		log.Fatalf("query parameter %q contains invalid characters: %q", name, value)
	}
}

// getCredentials reads and YAML-decodes the credentials file.
// It fatals if the file has permissions readable by group or others (mode & 0o077 != 0).
func getCredentials(credentialsFilePath string, logger *slog.Logger) Credentials {
	info, err := os.Stat(credentialsFilePath)
	if err != nil {
		logger.Error("Unable to stat credentials file", "filePath", credentialsFilePath, "error", err)
		log.Fatalf("unable to stat credentials file %q: %v", credentialsFilePath, err)
	}
	// reject credentials files readable by group or others
	if info.Mode().Perm()&0o077 != 0 {
		logger.Error("Credentials file has insecure permissions", "filePath", credentialsFilePath, "permissions", fmt.Sprintf("%04o", info.Mode().Perm()))
		log.Fatalf("credentials file %q has insecure permissions %04o; chmod 600 to fix", credentialsFilePath, info.Mode().Perm())
	}

	creds := Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		logger.Error("Unable to read credentials file", "filePath", credentialsFilePath, "error", err)
		log.Fatalf("unable to read credentials file %q: %v", credentialsFilePath, err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		logger.Error("Failed to unmarshal credentials YAML", "filePath", credentialsFilePath, "error", err)
		log.Fatalf("Unmarshal: %v", err)
	}
	if creds.Cb_host == "" || creds.Cb_user == "" || creds.Cb_password == "" || creds.Cb_bucket == "" {
		logger.Error("Credentials file missing required fields", "filePath", credentialsFilePath, "required", "cb_host, cb_user, cb_password, cb_bucket")
		log.Fatal("credentials file missing required fields: cb_host, cb_user, cb_password, cb_bucket")
	}
	if creds.Cb_scope == "" {
		creds.Cb_scope = "_default"
	}
	if creds.Cb_collection == "" {
		creds.Cb_collection = "_default"
	}
	logger.Debug("Credentials loaded successfully", "bucket", creds.Cb_bucket, "scope", creds.Cb_scope, "collection", creds.Cb_collection)
	return creds
}

// getDbConnection opens a Couchbase cluster connection using the supplied credentials.
// It waits up to a configurable timeout for the bucket to become ready before returning.
func getDbConnection(cred Credentials, logger *slog.Logger) (conn CbConnection) {
	logger.Debug("Initiating database connection")

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

	logger.Debug("Connecting to Couchbase cluster", "host", connectionString, "bucket", bucketName)
	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		logger.Error("Failed to connect to Couchbase cluster", "host", connectionString, "error", err)
		log.Fatal(err)
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.Collection = conn.Bucket.Collection(collection)
	conn.vxDBTARGET = cred.Cb_bucket + "." + cred.Cb_scope + "." + cred.Cb_collection
	validateQueryParam("vxDBTARGET", conn.vxDBTARGET)

	logger.Debug("Waiting for bucket to become ready", "bucket", bucketName)
	err = conn.Bucket.WaitUntilReady(bucketReadyTimeout(), nil)
	if err != nil {
		logger.Error("Bucket failed to become ready", "bucket", bucketName, "error", err)
		log.Fatal(err)
	}

	conn.Scope = conn.Bucket.Scope(cred.Cb_scope)
	logger.Info("Database connection established", "vxDBTARGET", conn.vxDBTARGET)
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
		slog.Warn("Invalid BUCKET_READY_TIMEOUT_SECONDS, using default", "value", raw, "default", seconds)
		return time.Duration(seconds) * time.Second
	}

	return time.Duration(parsed) * time.Second
}

// workerTask consumes document batches from jobs, enforces id-based keys,
// performs a bulk upsert, and returns per-worker success/failure counters.
func workerTask(collection *gocb.Collection, jobs <-chan []map[string]interface{}, logger *slog.Logger) (uint64, uint64, []string) {
	var successCount, failCount uint64
	successfulDocIDs := []string{}

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
			logger.Debug("No valid documents in batch to process", "batchSize", len(batch))
			continue
		}

		// Execute one network call for all operations in the current batch.
		err := collection.Do(ops, &gocb.BulkOpOptions{
			Context: context.Background(),
		})
		if err != nil {
			logger.Error("Bulk operation execution error", "batchSize", len(ops), "error", err)
		}

		// Each operation carries its own result error.
		for _, op := range ops {
			upsertOp := op.(*gocb.UpsertOp)
			if upsertOp.Err != nil {
				logger.Debug("Document upsert failed", "docID", upsertOp.ID, "error", upsertOp.Err)
				failCount++
			} else {
				logger.Debug("Document upserted successfully", "docID", upsertOp.ID)
				successfulDocIDs = append(successfulDocIDs, upsertOp.ID)
				successCount++
			}
		}
	}

	return successCount, failCount, successfulDocIDs
}

// enqueueJSONArrayBatches validates that the input is a JSON array and streams
// each element into batch jobs without loading the full array in memory.
func enqueueJSONArrayBatches(r io.Reader, filePath string, batchSize int, jobs chan<- []map[string]interface{}, logger *slog.Logger) error {
	if batchSize <= 0 {
		batchSize = 1
	}

	dec := json.NewDecoder(r)

	start, err := dec.Token()
	if err != nil {
		logger.Error("Failed to read first token from JSON file", "filePath", filePath, "error", err)
		return err
	}
	delim, ok := start.(json.Delim)
	if !ok || delim != '[' {
		logger.Error("JSON file is malformed - must be a JSON array", "filePath", filePath, "firstToken", fmt.Sprintf("%v", start))
		return fmt.Errorf("input must be a JSON array")
	}

	logger.Debug("Processing file", "filePath", filePath)
	batch := make([]map[string]interface{}, 0, batchSize)
	documentCount := 0
	for dec.More() {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			logger.Error("JSON decode error while processing file", "filePath", filePath, "documentNumber", documentCount, "error", err)
			return err
		}

		documentCount++
		logger.Debug("Processing document from file", "filePath", filePath, "documentNumber", documentCount)

		batch = append(batch, doc)
		if len(batch) >= batchSize {
			jobs <- batch
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	end, err := dec.Token()
	if err != nil {
		logger.Error("Failed to read final token from JSON file", "filePath", filePath, "error", err)
		return err
	}
	delim, ok = end.(json.Delim)
	if !ok || delim != ']' {
		logger.Error("JSON file is malformed - must end with ]", "filePath", filePath, "lastToken", fmt.Sprintf("%v", end))
		return fmt.Errorf("input must end with ]")
	}

	if len(batch) > 0 {
		jobs <- batch
	} else if documentCount == 0 {
		logger.Debug("No data to process in file", "filePath", filePath)
	}

	logger.Info("File processing complete", "filePath", filePath, "totalDocuments", documentCount)
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
