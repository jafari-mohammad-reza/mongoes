package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type Mapper struct {
	Collections map[string]map[string]any
}

func NewMapper() (*Mapper, error) {
	mp := &Mapper{
		Collections: make(map[string]map[string]any),
	}

	mapDir, err := os.ReadDir("mappers")
	if err != nil {
		if os.IsNotExist(err) {
			return mp, nil
		}
		return nil, err
	}

	for _, m := range mapDir {
		mname := m.Name()
		coll := strings.TrimSuffix(mname, ".json")

		mb, err := os.ReadFile(path.Join("mappers", mname))
		if err != nil {
			return nil, err
		}

		var raw map[string]any
		if err := json.Unmarshal(mb, &raw); err != nil {
			return nil, err
		}

		flattened := make(map[string]any)
		flatten("", raw, flattened)
		mp.Collections[coll] = flattened
	}

	return mp, nil
}
func (m *Mapper) ProcessedMapper(coll string, processed []bson.Raw) ([]map[string]any, error) {
	maps := m.Collections[coll]
	docs := []map[string]any{}
	for _, item := range processed {
		var doc map[string]any
		if err := bson.Unmarshal(item, &doc); err != nil {
			return nil, fmt.Errorf("failed to unmarshal doc: %w", err)
		}
		flattened := make(map[string]any)
		flatten("", doc, flattened)
		for key, value := range flattened {
			if newKey, ok := maps[key]; ok {
				delete(flattened, key)
				flattened[newKey.(string)] = value
			}
		}
		docs = append(docs, flattened)
	}

	return docs, nil
}

func flatten(prefix string, in map[string]any, out map[string]any) {
	for k, v := range in {
		key := k

		switch val := v.(type) {
		case string:
			out[key] = val
		case map[string]any:
			flatten(key, val, out)
		default:
		}
	}
}
