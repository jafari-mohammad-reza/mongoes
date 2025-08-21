package es

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"mongo-es/utils"
	"net/http"
	"strings"
	"time"

	elastic "github.com/elastic/go-elasticsearch/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type EsClient struct {
	client *elastic.Client
}

func NewEsClient() *EsClient {
	return &EsClient{}
}

func (es *EsClient) Init() error {
	cfg := elastic.Config{
		Addresses: []string{
			utils.Env("ELASTIC_ADDR", "http://localhost:9200"),
		},
		Username: utils.Env("ELASTIC_USER", ""),
		Password: utils.Env("ELASTIC_PASSWORD", ""),
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := elastic.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create elastic client: %s", err.Error())
	}
	es.client = client
	return nil
}
func (es *EsClient) IndexProcessed(ctx context.Context, processed []bson.Raw, prefix string) error {
	index := fmt.Sprintf("%s-%s", prefix, time.Now().Format(time.DateOnly))
	var buf bytes.Buffer

	for _, pr := range processed {
		var doc map[string]any
		if err := bson.Unmarshal(pr, &doc); err != nil {
			return fmt.Errorf("failed to unmarshal bson: %w", err)
		}
		idVal, ok := doc["_id"]
		if !ok {
			return fmt.Errorf("document missing _id")
		}
		var docID string
		switch v := idVal.(type) {
		case primitive.ObjectID:
			docID = v.Hex()
		default:
			docID = fmt.Sprintf("%v", v)
		}
		delete(doc, "_id")

		meta := fmt.Appendf(nil,
			`{ "index" : { "_index" : "%s", "_id" : "%s" } }%s`,
			index, docID, "\n",
		)
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal json: %w", err)
		}
		data = append(data, '\n')

		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}
	res, err := es.client.Bulk(
		strings.NewReader(buf.String()),
		es.client.Bulk.WithContext(ctx),
	)
	var bulkRes map[string]any
	if err := json.NewDecoder(res.Body).Decode(&bulkRes); err != nil {
		return fmt.Errorf("decode bulk response: %w", err)
	}
	if bulkRes["errors"].(bool) {
		for _, item := range bulkRes["items"].([]any) {
			it := item.(map[string]any)
			idx := it["index"].(map[string]any)
			if idx["error"] != nil {
				return fmt.Errorf("failed doc %v: %+v\n", idx["_id"], idx["error"])
			}
		}
	}
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk indexing error: %s", res.String())
	}

	log.Printf("Indexed %d docs into %s", len(processed), index)
	return nil
}
