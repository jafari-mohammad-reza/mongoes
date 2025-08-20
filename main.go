package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	ctx := context.Background()
	prepare()
	mc := NewMdClient()
	if err := mc.Init(ctx); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
}
func prepare() {
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
func env(name, df string) string {
	stored := os.Getenv(name)
	if stored == "" {
		return df
	}
	return stored
}
