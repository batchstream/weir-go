package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	weir "github.com/batchstream/weir-go"
)

func main() {
	address := flag.String("address", "127.0.0.1:7447", "Weir initialization listener")
	store := flag.String("store", "mongo", "logical Store")
	resource := flag.String("resource", "weir_acceptance/records/s:example", "relative record resource")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := weir.OpenOptions{Seed: *address, Stores: []string{*store}}
	client, err := weir.Open(ctx, options)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	request := &weir.ReadRequest{Resource: *resource}
	readOptions := weir.ReadOptions{StoreName: *store, Request: request}
	read, err := client.Read(ctx, readOptions)
	if err != nil {
		panic(err)
	}
	if read.GetFailure() != nil {
		panic(read.GetFailure())
	}
	if read.GetMissing() {
		fmt.Println("missing")
		return
	}
	document := read.GetDocument()
	fmt.Printf("%s: %d bytes\n", document.MediaType, len(document.Data))
}
