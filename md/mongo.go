package md

import (
	"bufio"
	"context"
	"fmt"
	"mongo-es/utils"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
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
	cfg          *utils.Conf
	cl           *mongo.Client
	watchChan    chan WatchEvent
	collStat     map[string]CollStats
	processFiles map[string]*os.File
	mu           sync.Mutex
}

func NewMdClient(cfg *utils.Conf) *MdClient {
	return &MdClient{
		cfg:          cfg,
		watchChan:    make(chan WatchEvent, 1000),
		collStat:     make(map[string]CollStats),
		processFiles: make(map[string]*os.File),
		mu:           sync.Mutex{},
	}
}
func (m *MdClient) Init(ctx context.Context) error {
	url := m.cfg.Mongo.URL
	md, err := mongo.Connect(ctx, options.Client().
		ApplyURI(url))
	if err != nil {
		return fmt.Errorf("failed to connect to mongo %s", url)
	}
	m.cl = md
	if err := m.loadOffsets(ctx); err != nil {
		return fmt.Errorf("failed to load offsets: %s", err.Error())
	}
	return nil
}

func (m *MdClient) loadOffsets(ctx context.Context) error {
	logDir, err := os.ReadDir("md-processed")
	if err != nil {
		return fmt.Errorf("failed to read md-processed dir %s", err.Error())
	}
	for _, entry := range logDir {
		f, err := os.OpenFile(path.Join("md-processed", entry.Name()), os.O_RDONLY, 0655)
		if err != nil {
			return fmt.Errorf("failed to read %s: %s", entry.Name(), err.Error())
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 10*1024*1024) // 10MB max token size

		lineCount := 0
		for scanner.Scan() {
			lineCount++
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanner error: %v", err)
		}
		collName := strings.Split(entry.Name(), "_processed.log")[0]
		m.collStat[collName] = CollStats{
			Offset: int64(lineCount),
		}
	}
	return nil
}

func (m *MdClient) Destroy(ctx context.Context) error {
	return m.cl.Disconnect(ctx)
}
func (m *MdClient) Colls(ctx context.Context, db string) ([]string, error) {
	return m.cl.Database(db).ListCollectionNames(ctx, bson.D{})
}
func (m *MdClient) WatchColl(ctx context.Context, db, coll, sortBy string) (chan []bson.Raw, chan error, error) {
	if sortBy == "" {
		sortBy = "created_at"
	}
	var stat CollStats
	processedChan := make(chan []bson.Raw, 10)
	errorChan := make(chan error, 1)

	collStat, ok := m.collStat[coll]
	if !ok {
		m.mu.Lock()
		stat = CollStats{
			Offset: 0,
		}
		m.collStat[coll] = stat
		m.mu.Unlock()
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
			targetColl := m.cl.Database(db).Collection(coll)
			docCount, err := targetColl.CountDocuments(ctx, bson.D{})
			if err != nil {
				errorChan <- fmt.Errorf("failed to get %s doc counts: %s", coll, err.Error())
				return
			}
			if docCount == collStat.Offset {
				fmt.Printf("%s processed count reached max of %d\n", coll, collStat.Offset)
				return
			}
			allowDiskUse := true
			batchSize := m.cfg.Mongo.GetCollBatch(coll)
			limit := int64(batchSize)
			cur, err := targetColl.Find(ctx, bson.D{}, &options.FindOptions{Sort: bson.M{sortBy: -1}, Skip: &stat.Offset, Limit: &limit, BatchSize: &batchSize, AllowDiskUse: &allowDiskUse})
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

			atomic.AddInt64(&stat.Offset, int64(len(processed)))
			m.mu.Lock()
			m.collStat[coll] = stat
			processedChan <- processed
			m.mu.Unlock()

			if len(processed) > 0 {
				if err := m.logProcessed(coll, processed); err != nil {
					errorChan <- err
					return
				}
			}
			processSleepTimeout := m.cfg.Mongo.BatchTimeoutSec
			time.Sleep(time.Duration(processSleepTimeout) * time.Second)
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
		var doc map[string]any
		if err := bson.Unmarshal(pr, &doc); err != nil {
			return fmt.Errorf("failed to unmarshal doc: %w", err)
		}
		_, err := fmt.Fprintln(file, doc["_id"])
		if err != nil {
			return fmt.Errorf("failed to write processed line %d to process file: %s", i, err.Error())
		}
	}
	return nil
}
