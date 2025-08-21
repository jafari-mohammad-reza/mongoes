package es

import (
	"context"
	"log"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBulkInsert(t *testing.T) {
	es := NewEsClient()
	if err := es.Init(); err != nil {
		t.Fatalf("failed to init es: %v", err)
	}

	var docs []bson.Raw
	for i := 0; i < 5; i++ {
		id := primitive.NewObjectID()
		doc := bson.M{
			"_id":    id,
			"user":   "user_" + id.Hex()[0:6],
			"count":  i,
			"ts":     time.Now(),
			"active": true,
		}
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatalf("failed to marshal doc: %v", err)
		}
		docs = append(docs, raw)
	}

	if err := es.IndexProcessed(context.Background(), docs, "test-index"); err != nil {
		t.Fatalf("bulk insert failed: %v", err)
	}

	log.Println("bulk insert test finished OK")
}
