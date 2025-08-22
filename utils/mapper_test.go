package utils_test

import (
	"mongo-es/utils"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestProcessedMapper(t *testing.T) {

	m := &utils.Mapper{
		Collections: map[string]map[string]any{
			"users": {
				"name":          "first_name",
				"last_name":     "last_name",
				"stats.country": "user_country",
			},
		},
	}

	doc := bson.D{
		{"name", "Alice"},
		{"last_name", "Smith"},
		{"stats", bson.D{{"country", "USA"}}},
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}

	results, err := m.ProcessedMapper("users", []bson.Raw{raw})
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	res := results[0]
	expected := map[string]any{
		"first_name":   "Alice",
		"last_name":    "Smith",
		"user_country": "USA",
	}

	for k, v := range expected {
		if res[k] != v {
			t.Errorf("expected %s = %v, got %v", k, v, res[k])
		}
	}
}
