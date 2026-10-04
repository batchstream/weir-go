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
	var body []byte
	switch *store {
	case "mongo":
		request.Resource = weir.EncodeSegment(*database) + "/records"
		request.Descriptor = &weir.Document{ContentType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		request.BodyContentType = "application/bson"
		command := bson.D{{Key: "count", Value: "records"}}
		var err error
		body, err = bson.Marshal(command)
		if err != nil {
			return err
		}
	case "search":
		request.Resource = weir.EncodeSegment(*index)
		descriptor := &weir.SearchHTTPRequest{Method: "GET", Path: "/_doc/example"}
		encoded, err := weir.SearchHTTPDescriptor(descriptor)
		if err != nil {
			return err
		}
		request.Descriptor = encoded
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
	request.Body = body
	total := 0
	opts := weir.NativeOptions{StoreName: *store, Request: request}
	opts.Consume = func(_ context.Context, event *weir.Event) error {
		if head := event.Head; head != nil {
			fmt.Printf("metadata=%v media=%s\n", head.Metadata, head.BodyContentType)
		}
		total += len(event.Chunk)
		// Consume native bytes here without collecting the entire response.
		return nil
	}
	terminal, err := client.Native(ctx, opts)
	if err != nil {
		return fmt.Errorf("native response incomplete; effects indeterminate: %w", err)
	}
	if terminal == nil || terminal.Completion != weir.NativeResponseComplete {
		return fmt.Errorf("native exchange: %v; effects indeterminate", terminal)
	}
	fmt.Printf("complete native response: %d bytes\n", total)
	return nil
}
