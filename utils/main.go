package utils

import (
	"fmt"
	"os"
)

func Prepare() {
	dirs := []string{
		"md-processed",
		"es-processed",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("error creating %s: %s", dir, err.Error())
			os.Exit(1)
		}
	}
}
func Env(name, df string) string {
	stored := os.Getenv(name)
	if stored == "" {
		return df
	}
	return stored
}
