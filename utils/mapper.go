package utils

import (
	"encoding/json"
	"fmt"

	"os"
	"path/filepath"
	"reflect"
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
		if Env("INDIC_FLAT_MAP", "") != "" {
			indicFlatMapEnv := Env("INDIC_FLAT_MAP", "")
			for fm := range strings.SplitSeq(indicFlatMapEnv, ",") {
				parts := strings.Split(fm, ":")
				ind := parts[0]
				field := parts[1]
				if ind == indic {
					fmt.Printf("field: %v\n", field)
					field = strings.ReplaceAll(field, "\"", "")
					flatmap, ok := toMapSliceLoose(flattened[field])
					if !ok {
						continue
					}
					for k, v := range flatObjectMap(flatmap) {
						flattened[fmt.Sprintf("%s.%s", field, k)] = v
					}
				}

			}
		}

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

func toMapSliceLoose(v any) ([]map[string]any, bool) {
	if v == nil {
		return nil, false
	}

	// Fast path: bson.A
	if a, ok := v.(bson.A); ok {
		return toMapSliceLoose([]any(a))
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}

	out := make([]map[string]any, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()

		// Common fast paths
		switch t := elem.(type) {
		case map[string]any:
			out = append(out, t)
			continue
		case bson.M:
			out = append(out, map[string]any(t))
			continue
		case bson.D:
			m := make(map[string]any, len(t))
			for _, e := range t {
				m[e.Key] = e.Value
			}
			out = append(out, m)
			continue
		}

		// Generic: accept any map with any key type; stringify keys.
		ev := reflect.ValueOf(elem)
		if ev.Kind() == reflect.Map {
			m := make(map[string]any, ev.Len())
			for _, k := range ev.MapKeys() {
				// stringify key (handles interface{}, numbers, etc.)
				m[fmt.Sprint(k.Interface())] = ev.MapIndex(k).Interface()
			}
			out = append(out, m)
			continue
		}

		// Unknown element → fail
		return nil, false
	}
	return out, true
}
func flatObjectMap(in []map[string]any) map[string][]any {
	out := make(map[string][]any)
	for _, m := range in {
		for k, v := range m {
			out[k] = append(out[k], v)
		}
	}
	return out
}
