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
			utils.Env("ELASTIC_ADDR", "http://localhost:9092"),
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

	for i, pr := range processed {
		var doc map[string]interface{}
		if err := bson.Unmarshal(pr, &doc); err != nil {
			return fmt.Errorf("failed to unmarshal bson: %w", err)
		}

		meta := fmt.Appendf(nil, `{ "index" : { "_index" : "%s", "_id" : "%d" } }%s`, index, i, "\n")
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
