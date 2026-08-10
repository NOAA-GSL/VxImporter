//go:build integration

package integration

import (
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
)

func TestIntegration_UpsertAndGet(t *testing.T) {
	conn := os.Getenv("CB_CONN")
	user := os.Getenv("CB_USER")
	pass := os.Getenv("CB_PASS")
	bucketName := os.Getenv("CB_BUCKET")
	scopeName := os.Getenv("CB_SCOPE")
	collectionName := os.Getenv("CB_COLLECTION")

	if conn == "" || user == "" || pass == "" || bucketName == "" || scopeName == "" || collectionName == "" {
		t.Skip("integration test skipped: set CB_CONN, CB_USER, CB_PASS, CB_BUCKET, CB_SCOPE, CB_COLLECTION")
	}

	cluster, err := gocb.Connect(conn, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: user,
			Password: pass,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{KVTimeout: 10 * time.Second},
	})
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer cluster.Close(nil)

	bucket := cluster.Bucket(bucketName)
	if err := bucket.WaitUntilReady(5*time.Second, nil); err != nil {
		t.Fatalf("bucket ready check failed: %v", err)
	}
	collection := bucket.Scope(scopeName).Collection(collectionName)

	docID := "vximporter-integration-test-doc"
	payload := map[string]interface{}{
		"id":      docID,
		"source":  "integration-test",
		"updated": time.Now().UTC().Format(time.RFC3339Nano),
	}

	_, err = collection.Upsert(docID, payload, nil)
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = collection.Remove(docID, nil)
	})

	res, err := collection.Get(docID, nil)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	var got map[string]interface{}
	if err := res.Content(&got); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got["source"] != "integration-test" {
		t.Fatalf("unexpected source field: %#v", got["source"])
	}
}
