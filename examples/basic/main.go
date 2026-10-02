// Basic demonstrates one finite Execute batch with incremental input and consumption.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
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
	target := protocol.EncodeSegment(*database) + "/records/s:example"
	record := bson.D{{Key: "_id", Value: "example"}, {Key: "n", Value: int32(1)}}
	data, err := bson.Marshal(record)
	if err != nil {
		return err
	}
	document := &pb.Document{MediaType: "application/bson", Data: data}
	if *storeName == "search" {
		target = protocol.EncodeSegment(*index) + "/s:example"
		document = &pb.Document{MediaType: "application/json", Data: []byte(`{"n":1}`)}
	}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: target, Action: action}
	variant := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: variant}
	opts := weir.RecordOptions{StoreName: *storeName, Call: call}
	result, err := client.Record(ctx, opts)
	if err != nil {
		if result != nil {
			return fmt.Errorf("write RPC incomplete; backend evidence=%v: %w", result, err)
		}
		return fmt.Errorf("write result unavailable; effects indeterminate: %w", err)
	}
	if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
		return fmt.Errorf("write: %v", result)
	}
	produced := 0
	batch := weir.Options{StoreName: *storeName}
	batch.Produce = func(context.Context) (*pb.Call, error) {
		if produced == *count {
			return nil, io.EOF
		}
		produced++
		read := &pb.ReadRequest{Resource: target}
		variant := &pb.Call_Read{Read: read}
		call := &pb.Call{Version: 1, Operation: variant}
		return call, nil
	}
	batch.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
		read := event.GetResult().GetRead()
		if read.GetFailure() != nil {
			return fmt.Errorf("read %d: %v", id, read.GetFailure())
		}
		fmt.Printf("id=%d record=%d bytes missing=%t\n", id, len(read.GetDocument().GetData()), read.GetMissing() != nil)
		// Consume and discard here: retaining Events would require the full batch memory.
		return nil
	}
	if err := client.Execute(ctx, batch); err != nil {
		return err
	}
	fmt.Printf("completed %d reads with all request ends and final gRPC OK\n", *count)
	return nil
}
