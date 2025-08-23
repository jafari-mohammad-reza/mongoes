package utils

import (
	"fmt"

	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestProcessedMapper(t *testing.T) {

	m := &Mapper{
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

func TestFlatten_Basic(t *testing.T) {
	in := map[string]any{
		"message": map[string]any{
			"a": "b",
			"c": "d",
		},
		"pps": []any{
			map[string]any{"x": 1},
			map[string]any{"y": 2},
		},
		"count": 3,
	}
	got := make(map[string]any)
	flatten("", in, got)

	expect := map[string]any{
		"message.a": "b",
		"message.c": "d",
		"pps": []any{
			map[string]any{"x": 1},
			map[string]any{"y": 2},
		},
		"count": 3,
	}

	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("flatten mismatch\n got:  %#v\n want: %#v", got, expect)
	}
}

func TestToMapSliceLoose_SupportedVariants(t *testing.T) {
	t.Run("slice of map[string]any", func(t *testing.T) {
		in := []map[string]any{
			{"a": 1}, {"b": 2},
		}
		got, ok := toMapSliceLoose(in)
		if !ok {
			t.Fatal("expected ok=true")
		}
		if !reflect.DeepEqual(got, in) {
			t.Fatalf("got %#v want %#v", got, in)
		}
	})

	t.Run("bson.A of bson.M", func(t *testing.T) {
		in := bson.A{
			bson.M{"a": 1},
			bson.M{"b": 2},
		}
		got, ok := toMapSliceLoose(in)
		if !ok {
			t.Fatal("expected ok=true")
		}
		want := []map[string]any{{"a": 1}, {"b": 2}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v want %#v", got, want)
		}
	})

	t.Run("slice of bson.D", func(t *testing.T) {
		in := []bson.D{
			{{Key: "a", Value: 1}},
			{{Key: "b", Value: 2}},
		}
		got, ok := toMapSliceLoose(in)
		if !ok {
			t.Fatal("expected ok=true")
		}
		want := []map[string]any{{"a": 1}, {"b": 2}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v want %#v", got, want)
		}
	})

	t.Run("generic map key type stringified", func(t *testing.T) {

		in := []any{
			map[any]any{"a": 1, 2: "numKey"},
		}
		got, ok := toMapSliceLoose(in)
		if !ok {
			t.Fatal("expected ok=true")
		}

		want := []map[string]any{{"a": 1, "2": "numKey"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v want %#v", got, want)
		}
	})
}

func TestToMapSliceLoose_FailCases(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if got, ok := toMapSliceLoose(nil); ok || got != nil {
			t.Fatalf("expected (nil,false), got (%#v,%v)", got, ok)
		}
	})
	t.Run("not a slice", func(t *testing.T) {
		if got, ok := toMapSliceLoose(map[string]any{"a": 1}); ok || got != nil {
			t.Fatalf("expected (nil,false), got (%#v,%v)", got, ok)
		}
	})
	t.Run("slice with non-map element", func(t *testing.T) {
		if got, ok := toMapSliceLoose([]any{123}); ok || got != nil {
			t.Fatalf("expected (nil,false), got (%#v,%v)", got, ok)
		}
	})
}

func TestFlatObjectMap(t *testing.T) {
	in := []map[string]any{
		{"a": 1, "c": 2},
		{"a": 3},
		{"b": "x"},
	}
	got := flatObjectMap(in)
	want := map[string][]any{
		"a": {1, 3},
		"c": {2},
		"b": {"x"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestEndToEnd_Smoke(t *testing.T) {
	doc := map[string]any{
		"message": map[string]any{
			"date":  "2025-08-21T14:51:43.000Z",
			"value": 188070.36,
		},
		"series": bson.A{
			bson.M{"date": "2025-08-21T14:51:43.000Z", "value": 1.0},
			bson.M{"date": "2025-08-21T14:51:47.000Z", "value": 2.0},
		},
	}
	flat := map[string]any{}
	flatten("", doc, flat)

	raw := flat["series"]
	objs, ok := toMapSliceLoose(raw)
	if !ok {
		t.Fatalf("series should be array of objects, got %T", raw)
	}
	agg := flatObjectMap(objs)
	if len(agg["date"]) != 2 || len(agg["value"]) != 2 {
		t.Fatalf("unexpected agg: %#v", agg)
	}

	if flat["message.date"] != "2025-08-21T14:51:43.000Z" {
		t.Fatalf("message.date missing: %#v", flat)
	}
	if fmt.Sprintf("%.2f", flat["message.value"]) != "188070.36" {
		t.Fatalf("message.value missing/wrong: %#v", flat)
	}
}
