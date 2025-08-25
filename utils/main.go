package utils

import (
	"fmt"
	"os"
)

func Prepare() {
	// TODO: make this cleaner by saving in single parent dir

	dirs := []string{
		"processed/md-processed",
		"processed/es-processed",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("error creating %s: %s", dir, err.Error())
			os.Exit(1)
		}
	}
}
