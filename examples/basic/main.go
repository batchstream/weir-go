// Basic demonstrates typed finite read and mutation batches addressed to one Store.
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
	request := &weir.MutateRequest{Resource: target, Action: weir.MutationPut, Document: document}
	opts := weir.MutateOptions{StoreName: *storeName, Requests: []*weir.MutateRequest{request}}
	results, err := client.Mutate(ctx, opts)
	if err != nil {
		if len(results) != 0 && results[0] != nil {
			return fmt.Errorf("write RPC incomplete; backend evidence=%v: %w", results[0], err)
		}
		return fmt.Errorf("write result unavailable; effects indeterminate: %w", err)
	}
	if results[0].GetOutcome() != weir.MutationApplied || results[0].GetFailure() != nil {
		return fmt.Errorf("write: %v", results[0])
	}
	batch := weir.ReadOptions{StoreName: *storeName}
	for range *count {
		read := &weir.ReadRequest{Resource: target}
		batch.Requests = append(batch.Requests, read)
	}
	reads, err := client.Read(ctx, batch)
	if err != nil {
		return fmt.Errorf("read RPC incomplete: %w", err)
	}
	for index, read := range reads {
		if read.GetFailure() != nil {
			return fmt.Errorf("read %d: %v", index, read.GetFailure())
		}
		fmt.Printf("index=%d record=%d bytes missing=%t\n", index, len(read.GetDocument().GetData()), read.GetMissing())
	}
	fmt.Printf("completed %d reads with all request ends and final gRPC OK\n", *count)
	return nil
}
