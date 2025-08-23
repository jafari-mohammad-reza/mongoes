package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type Mapper struct {
	Collections map[string]map[string]any
	Indices     map[string]map[string]any
}

func NewMapper() (*Mapper, error) {
	mp := &Mapper{
		Collections: make(map[string]map[string]any),
		Indices:     make(map[string]map[string]any),
	}

	// Load MongoDB mappers
	if err := mp.loadMappersFromDir("mappers", mp.Collections); err != nil {
		return nil, fmt.Errorf("failed to load mongo mappers: %w", err)
	}

	// Load Elasticsearch mappers
	if err := mp.loadMappersFromDir("es-mappers", mp.Indices); err != nil {
		return nil, fmt.Errorf("failed to load elastic mappers: %w", err)
	}

	return mp, nil
}

func (mp *Mapper) loadMappersFromDir(dirPath string, targetMap map[string]map[string]any) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read directory %s: %w", dirPath, err)
	}

	for _, entry := range entries {
		filename := entry.Name()

		if !strings.HasSuffix(filename, ".json") || entry.IsDir() {
			continue
		}

		key := strings.TrimSuffix(filename, ".json")
		filePath := filepath.Join(dirPath, filename)

		fileBytes, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", filePath, err)
		}

		var rawMapping map[string]any
		if err := json.Unmarshal(fileBytes, &rawMapping); err != nil {
			return fmt.Errorf("failed to parse JSON in file %s: %w", filePath, err)
		}

		flattened := make(map[string]any)
		flatten("", rawMapping, flattened)
		targetMap[key] = flattened
	}

	return nil
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

func (m *Mapper) EsMapper(indic string, processed []map[string]any) ([]map[string]any, error) {
	maps, exists := m.Indices[indic]
	if !exists {
		return processed, nil
	}

	docs := make([]map[string]any, 0, len(processed))

	for _, item := range processed {
		flattened := make(map[string]any)
		flatten("", item, flattened)
		mapped := make(map[string]any)

		for key, value := range flattened {
			if newKey, ok := maps[key]; ok {
				mapped[newKey.(string)] = value
			}
		}

		docs = append(docs, mapped)
	}
	return docs, nil
}

func flatten(prefix string, in map[string]any, out map[string]any) {
	for k, v := range in {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}

		switch val := v.(type) {
		case map[string]any:
			flatten(key, val, out)
		case []any:
			out[key] = val
		default:
			out[key] = val
		}
	}
}
