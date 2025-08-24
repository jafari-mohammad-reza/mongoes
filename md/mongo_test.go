package md

import (
	"context"
	"fmt"
	"mongo-es/utils"
	"os"
	"time"

	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"go.mongodb.org/mongo-driver/mongo"
)

type TestRecord struct {
	Name     string `bson:"name"`
	LastName string `bson:"last_name"`
	IsActive bool   `bson:"is_active"`
	Stats    struct {
		Country string `bson:"country"`
	} `bson:"stats"` // we add nested field to be able to work on collections with variation of structures
	CreatedAt time.Time `bson:"created_at"`
}

func TestMdClient(t *testing.T) {
	cfg, err := utils.NewConf()
	if err != nil {
		t.Fatal(err.Error())
	}
	md := NewMdClient(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	utils.Prepare()
	defer func() {
		os.RemoveAll("es-processed")
		os.RemoveAll("md-processed")
	}()
	if err := md.Init(ctx); err != nil {
		t.Fatalf("failed to init md client %s\n", err.Error())
	}
	coll := md.cl.Database("test-db").Collection("users")
	defer coll.Drop(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			insertTestRecs(t, ctx, coll)
			time.Sleep(time.Millisecond * 500)
		}
	}()

	_, errChan, err := md.WatchColl(ctx, "test-db", "users", "")
	if err != nil {
		t.Fatalf("failed watching coll %s\n", err.Error())
		coll.Drop(ctx)
	}
	for err := range errChan {
		t.Fatalf("failed watching coll %s\n", err.Error())
		coll.Drop(ctx)
	}

}
func insertTestRecs(t *testing.T, ctx context.Context, coll *mongo.Collection) {
	testRecords, err := generateTestRecord(ctx, 1000)
	if err != nil {
		t.Fatalf("failed to generate test records %s", err.Error())
	}
	inserted, err := coll.InsertMany(ctx, testRecords)
	if err != nil {
		t.Fatalf("failed to insert test records: %s", err.Error())
	}
	if len(inserted.InsertedIDs) != len(testRecords) {
		t.Fatal("test records and inserted count should match")
	}
}
func TestColls(t *testing.T) {
	cfg, err := utils.NewConf()
	if err != nil {
		t.Fatal(err.Error())
	}
	md := NewMdClient(cfg)
	ctx := t.Context()
	if err := md.Init(ctx); err != nil {
		t.Fatalf("failed to init md client %s\n", err.Error())
	}
	colls, err := md.Colls(ctx, "test-db")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("colls: %v\n", colls)
}

func generateTestRecord(ctx context.Context, count int) ([]any, error) {
	records := make([]any, 0)
	for range count {
		rec := TestRecord{
			Name:     gofakeit.Name(),
			LastName: gofakeit.LastName(),
			IsActive: gofakeit.Bool(),
			Stats: struct {
				Country string "bson:\"country\""
			}{
				Country: gofakeit.Country(),
			},
			CreatedAt: time.Now(),
		}
		records = append(records, rec)
	}
	return records, nil
}
