package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	weir "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir/api/weir/v1"
)

func main() {
	address := flag.String("address", "127.0.0.1:7447", "isolated Weir application listener")
	resource := flag.String("resource", "weir://mongo/weir_acceptance/records/s:example", "record URI")
	flag.Parse()
	opts := weir.Options{Plaintext: true}
	client, err := weir.New(*address, opts)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.ReadRequest{Resource: *resource}
	result, err := client.Read(ctx, req)
	if err != nil {
		panic(err)
	}
	if result.GetMissing() != nil {
		fmt.Println("missing")
		return
	}
	doc := result.GetDocument()
	fmt.Printf("%s: %d bytes\n", doc.MediaType, len(doc.Data))
}
