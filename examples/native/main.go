// Native sends one bounded backend request and consumes its response incrementally.
// Native backend errors remain native response data, not normalized write outcomes.
package main

import (
	"context"
	"flag"
	"fmt"
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
	store := flag.String("store", "mongo", "mongo or search")
	database := flag.String("database", "weir_m1", "configured Mongo database")
	index := flag.String("index", "weir_m2_example", "configured Search index")
	flag.Parse()

	request := &weir.NativeRequest{}
	switch *store {
	case "mongo":
		request.Resource = weir.EncodeSegment(*database) + "/records"
		command := bson.D{{Key: "count", Value: "records"}}
		var err error
		request.MongoDBCommand, err = bson.Marshal(command)
		if err != nil {
			return err
		}
	case "search":
		request.Resource = weir.EncodeSegment(*index)
		request.SearchHTTP = &weir.SearchHTTPRequest{Method: "GET", Path: "/_doc/example"}
	default:
		return fmt.Errorf("unsupported store")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	openOptions := weir.OpenOptions{Seed: *address, Stores: []string{*store}}
	client, err := weir.Open(ctx, openOptions)
	if err != nil {
		return err
	}
	defer client.Close()
	total := 0
	opts := weir.NativeOptions{StoreName: *store, Request: request}
	opts.Consume = func(_ context.Context, response *weir.NativeResponse, data []byte) error {
		fmt.Printf("HTTP=%v content-type=%s chunk=%d bytes\n", response.Http, response.BodyContentType, len(data))
		total += len(data)
		// Consume and discard each chunk instead of retaining the whole response.
		return nil
	}
	result, err := client.Native(ctx, opts)
	if result != nil && result.Completion == weir.NativeNotStarted {
		return fmt.Errorf("native request was not started: %v; RPC error=%v", result.Failure, err)
	}
	if err != nil {
		return fmt.Errorf("native response failed; retained evidence=%v: %w", result, err)
	}
	if result == nil || result.Completion != weir.NativeResponseComplete {
		return fmt.Errorf("native response incomplete: %v; reconcile any possible writes before retrying", result)
	}
	fmt.Printf("complete native response: HTTP=%v content-type=%s %d bytes\n", result.Response.Http, result.Response.BodyContentType, total)
	// A complete HTTP/BSON response can describe a backend error. Inspect that
	// response separately before deciding whether the business operation succeeded.
	return nil
}
