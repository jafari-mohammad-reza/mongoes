package main

import (
	"context"
	"fmt"
	"mongo-es/es"
	"mongo-es/md"
	"mongo-es/utils"
	"os"
	"strings"
)

func main() {
	ctx := context.Background()
	utils.Prepare()
	mc := md.NewMdClient()
	esc := es.NewEsClient()
	if err := esc.Init(); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
	if err := mc.Init(ctx); err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}
	db := utils.Env("MONGO_DB", "new-iri")
	colls, err := mc.Colls(ctx, db)
	if err != nil {
		fmt.Printf("failed to get %s collections %s\n", db, err.Error())
		os.Exit(1)
	}
	esColl := make(map[string]string, len(colls)) // mongo collections to elastic index names
	if utils.Env("ES_COLL", "") != "" {
		esc := utils.Env("ES_COLL", "")
		cns := strings.SplitSeq(esc, ",")
		for cn := range cns {
			pair := strings.Split(cn, ":")
			if len(pair) != 2 {
				continue
			}
			esColl[pair[0]] = pair[1]
		}
	}

	for _, coll := range colls {
		go func() {
			prCh, errCh, err := mc.WatchColl(ctx, db, coll, 500)
			if err != nil {
				fmt.Printf("failed to get %s changes: %s", coll, err.Error())
				os.Exit(1)
			}
			select {
			case processed := <-prCh:
				prefix, ok := esColl[coll]
				if !ok {
					prefix = coll
				}
				if err := esc.IndexProcessed(ctx, processed, prefix); err != nil {
					errCh <- err
				}
			case err := <-errCh:
				fmt.Printf("failed to get %s changes: %s", coll, err.Error())
				os.Exit(1)
			default:
			}
		}()
	}
	select {}
}
