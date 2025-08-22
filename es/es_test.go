package es

import (
	"context"
	"log"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBulkInsert(t *testing.T) {
	es := NewEsClient()
	if err := es.Init(); err != nil {
		t.Fatalf("failed to init es: %v", err)
	}

	var docs []map[string]any
	for i := 0; i < 5; i++ {
		id := primitive.NewObjectID()
		doc := map[string]any{
			"_id":    id.String(),
			"user":   "user_" + id.Hex()[0:6],
			"count":  i,
			"ts":     time.Now(),
			"active": true,
		}
		docs = append(docs, doc)
	}

	if err := es.IndexProcessed(context.Background(), docs, "test-index"); err != nil {
		t.Fatalf("bulk insert failed: %v", err)
	}

	log.Println("bulk insert test finished OK")
}
