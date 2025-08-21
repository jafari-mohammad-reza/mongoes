package md

import (
	"context"
	"fmt"
	"mongo-es/utils"
	"os"
	"path"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WatchEvent struct {
	Collection string
	DB         string
}
type CollStats struct {
	Offset int64
}
type MdClient struct {
	cl           *mongo.Client
	watchChan    chan WatchEvent
	collStat     map[string]CollStats
	processFiles map[string]*os.File
}

func NewMdClient() *MdClient {
	return &MdClient{
		watchChan:    make(chan WatchEvent, 1000),
		collStat:     make(map[string]CollStats),
		processFiles: make(map[string]*os.File),
	}
}
func (m *MdClient) Init(ctx context.Context) error {
	uri := utils.Env("MONGO_URL", "mongodb://127.0.0.1:27017")
	md, err := mongo.Connect(ctx, options.Client().
		ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("failed to connect to mongo %s", uri)
	}
	m.cl = md
	return nil
}

func (m *MdClient) Destroy(ctx context.Context) error {
	return m.cl.Disconnect(ctx)
}
func (m *MdClient) Colls(ctx context.Context, db string) ([]string, error) {
	return m.cl.Database(db).ListCollectionNames(ctx, bson.D{})
}
func (m *MdClient) WatchColl(ctx context.Context, db, coll string, batch int64) (chan []bson.Raw, chan error, error) {
	var stat CollStats
	processedChan := make(chan []bson.Raw, 10)
	errorChan := make(chan error, 1)

	collStat, ok := m.collStat[coll]
	if !ok {
		stat = CollStats{
			Offset: 0,
		}
		m.collStat[coll] = stat
	} else {
		stat = collStat
	}

	go func() {
		defer close(processedChan)
		defer close(errorChan)

		for {
			select {
			case <-ctx.Done():
				fmt.Println("watch coll ctx done")
				return
			default:
			}

			cur, err := m.cl.Database(db).Collection(coll).Find(ctx, bson.D{}, &options.FindOptions{Skip: &stat.Offset, Limit: &batch})
			if err != nil {
				errorChan <- fmt.Errorf("failed to skip %d items from %s in %s database: %s", stat.Offset, coll, db, err.Error())
				return
			}

			processed := []bson.Raw{}
			for cur.Next(ctx) {
				item := cur.Current
				processed = append(processed, item)
			}
			cur.Close(ctx)

			stat.Offset += int64(len(processed))
			m.collStat[coll] = stat

			if len(processed) > 0 {
				if err := m.logProcessed(coll, processed); err != nil {
					errorChan <- err
					return
				}

				select {
				case processedChan <- processed:
				case <-ctx.Done():
					return
				}
			}

			if len(processed) == 0 {
				time.Sleep(5 * time.Second)
			} else {
				time.Sleep(time.Second)
			}
		}
	}()

	return processedChan, errorChan, nil
}

func (m *MdClient) logProcessed(coll string, processed []bson.Raw) error {
	file, ok := m.processFiles[coll]
	if !ok {
		f, err := os.OpenFile(path.Join("md-processed", fmt.Sprintf("%s_processed.log", coll)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0655)
		if err != nil {
			return err
		}
		file = f
		m.processFiles[coll] = f
	}
	for i, pr := range processed {
		_, err := fmt.Fprintln(file, pr.String())
		if err != nil {
			return fmt.Errorf("failed to write processed line %d to process file: %s", i, err.Error())
		}
	}
	return nil
}
