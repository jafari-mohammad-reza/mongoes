package utils

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/spf13/viper"
)

type Conf struct {
	Mongo   MongoConf   `mapstructure:"mongo"`
	Elastic ElasticConf `mapstructure:"elastic"`
}

type ElasticConf struct {
	Addresses    []string          `mapstructure:"addresses"`
	User         string            `mapstructure:"user"`
	Password     string            `mapstructure:"password"`
	UniqueFields map[string]string `mapstructure:"unique_fields"`
	IndicPeriod  map[string]int    `mapstructure:"indic_period"`
	CollPrefix   map[string]string `mapstructure:"coll_prefix"`
}
type MongoConf struct {
	CollBatch       map[string]int32 `mapstructure:"coll_batch"`
	BatchTimeoutSec int              `mapstructure:"batch_timeout"`
	URL             string           `mapstructure:"url"`
	DB              string           `mapstructure:"db"`
	WhiteList       []string         `mapstructure:"white_list"`
}

type Mappings struct{}

func newV(name string) (*viper.Viper, error) {
	mongoDefaultVals := map[string]any{
		"utl":           "mongodb://localhost:27017",
		"batch_timeout": 10,
		"db":            "test",
		"white_list":    []string{},
	}
	elasticDefaultVals := map[string]any{
		"addresses": []string{
			"http://localhost:9200",
		},
		"unique_fields": make(map[string]string),
		"indic_period":  make(map[string]int),
		"coll_prefix":   make(map[string]string),
	}
	v := viper.New()
	v.SetConfigFile(fmt.Sprintf("%s.yaml", name))
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	fmt.Printf("name: %v\n", name)
	switch name {
	case "config":
		for k, val := range mongoDefaultVals {
			v.Set(fmt.Sprintf("mongo.%s", k), val)
		}
		for k, val := range elasticDefaultVals {
			v.Set(fmt.Sprintf("elastic.%s", k), val)
		}
	}
	if err := v.ReadInConfig(); err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("config not found")
		}
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return nil, errors.New("config not found")
		}
		return nil, fmt.Errorf("failed to read %s: %s", name, err.Error())
	}
	return v, nil
}
func NewConf() (*Conf, error) {
	v, err := newV("config")
	if err != nil {
		if err.Error() == "config not found" {
			cfg := Conf{
				Mongo: MongoConf{
					URL:             "mongodb://localhost:27017",
					BatchTimeoutSec: 10,
					DB:              "test",
					CollBatch:       make(map[string]int32),
					WhiteList:       []string{},
				},
				Elastic: ElasticConf{
					Addresses:    []string{"http://localhost:9200"},
					User:         "",
					Password:     "",
					UniqueFields: make(map[string]string),
					IndicPeriod:  make(map[string]int),
					CollPrefix:   make(map[string]string),
				},
			}
			return &cfg, nil
		}
		return nil, err
	}
	var cfg Conf
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
func (c *MongoConf) GetCollBatch(coll string) int32 {
	if field, exists := c.CollBatch[coll]; exists {
		return field
	}
	return 100
}
func (c *MongoConf) IsWhiteListed(coll string) bool {
	return slices.Contains(c.WhiteList, coll)
}
func (c *ElasticConf) GetUniqueField(prefix string) string {
	if field, exists := c.UniqueFields[prefix]; exists {
		return field
	}
	return "_id"
}
func (c *ElasticConf) GetIndicPeriod(indic string) int {
	if field, exists := c.IndicPeriod[indic]; exists {
		return field
	}
	return 24
}
func (c *ElasticConf) GetCollPrefix(coll string) string {
	if field, exists := c.CollPrefix[coll]; exists {
		return field
	}
	return coll
}
func LoadMappings() (*Mappings, error) {
	v, err := newV("mappings")
	if err != nil {
		return nil, err
	}
	var mappings Mappings
	if err := v.Unmarshal(&mappings); err != nil {
		return nil, err
	}
	return &mappings, nil
}
