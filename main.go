package main

import (
	"context"
	"fmt"
	"mongo-es/es"
	"mongo-es/md"
	"mongo-es/utils"
	"os"
)

func main() {
	ctx := context.Background()
	cfg, err := utils.NewConf()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	utils.Prepare()
	mc := md.NewMdClient(cfg)
	esc := es.NewEsClient(cfg)
	if err := esc.Init(); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
	fmt.Println("elastic initialized")
	if err := mc.Init(ctx); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
	fmt.Println("mongodb initialized.")
	db := cfg.Mongo.DB
	colls, err := mc.Colls(ctx, db)
	if err != nil {
		fmt.Printf("failed to get %s collections %s\n", db, err.Error())
		os.Exit(1)
	}
	mapper, err := utils.NewMapper()
	if err != nil {
		fmt.Printf("failed to create mapper: %s\n", err.Error())
		os.Exit(1)
	}

	for _, coll := range colls {
		if !cfg.Mongo.IsWhiteListed(coll) {
			fmt.Printf("ignoring %s\n", coll)
			continue
		}
		go func() {
			if err != nil {
				fmt.Printf("failed to get batch size: %s", err.Error())
				return
			}
			prCh, errCh, err := mc.WatchColl(ctx, db, coll, "")
			if err != nil {
				fmt.Printf("failed to get %s changes: %s", coll, err.Error())
				return
			}
			for {
				select {
				case processed, ok := <-prCh:
					if !ok {
						fmt.Printf("Channel closed for collection %s, stopping processing", coll)
						return
					}
					prefix := cfg.Elastic.GetCollPrefix(coll)
					if !ok {
						prefix = coll
					}
					processedMap, err := mapper.ProcessedMapper(coll, processed)
					if err != nil {
						errCh <- err
					}
					esProcessedMap, err := mapper.EsMapper(prefix, processedMap)
					if err != nil {
						errCh <- err
					}
					if err := esc.IndexProcessed(ctx, esProcessedMap, prefix); err != nil {
						errCh <- err
					}
				case err, ok := <-errCh:
					if !ok {
						fmt.Printf("Channel closed for collection %s, stopping processing", coll)
						return
					}
					fmt.Printf("failed to get %s changes: %s", coll, err.Error())
					os.Exit(1)
				}
			}
		}()
	}
	select {}
}
