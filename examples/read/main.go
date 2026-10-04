package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	weir "github.com/batchstream/weir-go"
)

func main() {
	address := flag.String("address", "127.0.0.1:7447", "Weir initialization listener")
	store := flag.String("store", "mongo", "logical Store")
	resources := flag.String("resources", "weir_acceptance/records/s:example,weir_acceptance/records/s:another", "comma-separated relative record resources in one Store")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := weir.OpenOptions{Seed: *address, Stores: []string{*store}}
	client, err := weir.Open(ctx, options)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	readOptions := weir.ReadOptions{StoreName: *store}
	for _, resource := range strings.Split(*resources, ",") {
		request := &weir.ReadRequest{Resource: resource}
		readOptions.Requests = append(readOptions.Requests, request)
	}
	reads, err := client.Read(ctx, readOptions)
	if err != nil {
		panic(err)
	}
	for index, read := range reads {
		if read.Failure != nil {
			panic(read.Failure)
		}
		if read.Missing {
			fmt.Println(index, "missing")
			continue
		}
		document := read.Document
		fmt.Printf("%d %s: %d bytes\n", index, document.ContentType, len(document.Data))
	}
}
