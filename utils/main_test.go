package utils

import (
	"os"
	"testing"
)

func TestPrepare(t *testing.T) {
	Prepare()
	stat, err := os.Stat("es-processed")
	if err != nil {
		t.Fatal(err)
	}
	if !stat.IsDir() {
		t.Fatalf("es-processed should be dir")
	}
	stat, err = os.Stat("md-processed")
	if err != nil {
		t.Fatal(err)
	}
	if !stat.IsDir() {
		t.Fatalf("md-processed should be dir")
	}
	defer func() {
		os.RemoveAll("es-processed")
		os.RemoveAll("md-processed")
	}()
}
