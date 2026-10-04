// Scan demonstrates independent finite pages with incremental consumption.
package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	weir "github.com/batchstream/weir-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	address := flag.String("address", "127.0.0.1:7447", "Weir initialization listener")
	storeName := flag.String("store", "mongo", "logical Store")
	resource := flag.String("resource", "weir_m1/records", "relative collection or index")
	pageSize := flag.Uint("page-size", 128, "documents per page, 1 to 256")
	filterText := flag.String("filter", "", "native filter object as JSON (Mongo Extended JSON or Search query)")
	include := flag.String("include", "", "comma-separated field paths to include")
	exclude := flag.String("exclude", "", "comma-separated field paths to exclude")
	flag.Parse()
	if *pageSize < 1 || *pageSize > 256 {
		return fmt.Errorf("page-size must be between 1 and 256")
	}
	if *include != "" && *exclude != "" {
		return fmt.Errorf("select include or exclude projection")
	}
	request := &weir.ScanRequest{Resource: *resource, PageSize: uint32(*pageSize)}
	if *filterText != "" {
		filter := &weir.Document{ContentType: "application/json", Data: []byte(*filterText)}
		if *storeName == "mongo" {
			var object bson.D
			if err := bson.UnmarshalExtJSON(filter.Data, false, &object); err != nil {
				return err
			}
			data, err := bson.Marshal(object)
			if err != nil {
				return err
			}
			filter.ContentType, filter.Data = "application/bson", data
		}
		request.Filter = filter
	}
	if *include != "" {
		request.Projection = &weir.Projection{Mode: weir.ProjectionInclude, Fields: strings.Split(*include, ",")}
	} else if *exclude != "" {
		request.Projection = &weir.Projection{Mode: weir.ProjectionExclude, Fields: strings.Split(*exclude, ",")}
	}
	initialize, initializeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initializeCancel()
	openOptions := weir.OpenOptions{Seed: *address, Stores: []string{*storeName}}
	client, err := weir.Open(initialize, openOptions)
	if err != nil {
		return err
	}
	defer client.Close()
	options := weir.ScanOptions{StoreName: *storeName, Request: request}
	options.Consume = func(ctx context.Context, document *weir.Document) error {
		fmt.Printf("document: media=%s bytes=%d\n", document.ContentType, len(document.Data))
		// Process and discard each document. Retrying a page can repeat documents,
		// so a persistent consumer should commit output and its checkpoint together.
		return ctx.Err()
	}
	for page := uint64(1); ; page++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		end, err := client.Scan(ctx, options)
		cancel()
		if err != nil {
			return fmt.Errorf("page %d incomplete; previous checkpoint retained: %w", page, err)
		}
		if end.Failure != nil {
			return fmt.Errorf("page %d failed: %v", page, end.Failure)
		}
		fmt.Printf("page=%d documents=%d exhausted=%t\n", page, end.DocumentCount, end.Exhausted)
		if end.Exhausted {
			return nil
		}
		// Scan exposes this token only after ScanEnd and final gRPC OK.
		// The next RPC may be handled by a different Weir instance.
		request.ContinuationToken = end.NextContinuationToken
	}
}
