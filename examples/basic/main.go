// Basic demonstrates one finite Execute batch with incremental input and consumption.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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
	storeName := flag.String("store", "mongo", "logical Store: mongo or search")
	database := flag.String("database", "weir_m1", "pre-created MongoDB database")
	index := flag.String("index", "weir_m2_example", "pre-created Search index")
	count := flag.Int("count", 100, "finite number of read requests")
	flag.Parse()
	if *storeName != "mongo" && *storeName != "search" || *count < 1 {
		return fmt.Errorf("invalid Store or count")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	openOptions := weir.OpenOptions{Seed: *address, Stores: []string{*storeName}}
	client, err := weir.Open(ctx, openOptions)
	if err != nil {
		return err
	}
	defer client.Close()
	target := weir.EncodeSegment(*database) + "/records/s:example"
	record := bson.D{{Key: "_id", Value: "example"}, {Key: "n", Value: int32(1)}}
	data, err := bson.Marshal(record)
	if err != nil {
		return err
	}
	document := &weir.Document{MediaType: "application/bson", Data: data}
	if *storeName == "search" {
		target = weir.EncodeSegment(*index) + "/s:example"
		document = &weir.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	}
	request := &weir.WriteRequest{Resource: target, Document: document}
	opts := weir.WriteOptions{StoreName: *storeName, Request: request}
	result, err := client.Put(ctx, opts)
	if err != nil {
		if result != nil {
			return fmt.Errorf("write RPC incomplete; backend evidence=%v: %w", result, err)
		}
		return fmt.Errorf("write result unavailable; effects indeterminate: %w", err)
	}
	if result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
		return fmt.Errorf("write: %v", result)
	}
	produced := 0
	batch := weir.ExecuteOptions{StoreName: *storeName}
	batch.Produce = func(context.Context) (*weir.Command, error) {
		if produced == *count {
			return nil, io.EOF
		}
		produced++
		read := &weir.ReadRequest{Resource: target}
		return weir.NewReadCommand(read), nil
	}
	batch.Consume = func(_ context.Context, id uint64, event *weir.Event) error {
		read := event.GetResult().GetRead()
		if read.GetFailure() != nil {
			return fmt.Errorf("read %d: %v", id, read.GetFailure())
		}
		fmt.Printf("id=%d record=%d bytes missing=%t\n", id, len(read.GetDocument().GetData()), read.GetMissing())
		// Consume and discard here: retaining Events would require the full batch memory.
		return nil
	}
	if err := client.Execute(ctx, batch); err != nil {
		return err
	}
	fmt.Printf("completed %d reads with all request ends and final gRPC OK\n", *count)
	return nil
}
