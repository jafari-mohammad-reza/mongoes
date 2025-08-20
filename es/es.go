package es

import (
	"crypto/tls"
	"fmt"
	"mongo-es/utils"
	"net/http"

	elastic "github.com/elastic/go-elasticsearch/v8"
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
func (es *EsClient) IndexProcessed(processed []string, prefix string) error {
	// for _, pr := range processed {

	// }
	return nil
}
