package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
)

func main() {
	address := flag.String("address", "127.0.0.1:7447", "Weir initialization listener")
	resource := flag.String("resource", "weir://mongo/weir_acceptance/records/s:example", "record URI")
	flag.Parse()
	store, segments, err := protocol.ParseResource(*resource)
	if err != nil || len(segments) == 0 {
		panic("resource must identify a record")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := weir.OpenOptions{Seed: *address, Stores: []string{store}}
	client, err := weir.Open(ctx, options)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	relative := strings.TrimPrefix(*resource, "weir://"+store+"/")
	request := &pb.ReadRequest{Resource: relative}
	variant := &pb.Call_Read{Read: request}
	call := &pb.Call{Version: 1, Operation: variant}
	recordOptions := weir.RecordOptions{StoreName: store, Call: call}
	result, err := client.Record(ctx, recordOptions)
	if err != nil {
		panic(err)
	}
	read := result.GetRead()
	if read.GetFailure() != nil {
		panic(read.GetFailure())
	}
	if read.GetMissing() != nil {
		fmt.Println("missing")
		return
	}
	document := read.GetDocument()
	fmt.Printf("%s: %d bytes\n", document.MediaType, len(document.Data))
}
