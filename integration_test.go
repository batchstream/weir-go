//go:build integration

package weir_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Only an explicit tag and operator-provided endpoint enable backend I/O. Each
// test owns one generated record in a pre-created disposable collection/index.
func TestMongoLifecycle(t *testing.T)  { testLifecycle(t, "mongo") }
func TestSearchLifecycle(t *testing.T) { testLifecycle(t, "search") }

func testLifecycle(t *testing.T, backend string) {
	t.Helper()
	prefix := "WEIR_" + strings.ToUpper(backend)
	address := os.Getenv("WEIR_ADDRESS")
	store := os.Getenv(prefix + "_STORE")
	collection := os.Getenv(prefix + "_RESOURCE")
	if address == "" || store == "" || collection == "" {
		t.Skip("requires WEIR_ADDRESS, " + prefix + "_STORE and " + prefix + "_RESOURCE")
	}
	if !protocol.ValidStoreName(store) {
		t.Fatal("invalid Store name", store)
	}
	if _, err := protocol.ParseRelativeResource(collection); err != nil {
		t.Fatal("invalid relative collection/index", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	options := weir.OpenOptions{Seed: address, Stores: []string{store}}
	client, err := weir.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id := fmt.Sprintf("sdk-%s-%d", backend, time.Now().UnixNano())
	builder := mutationBuilder{t: t, backend: backend, resource: collection + "/s:" + id, id: id}
	write := weir.WriteOptions{StoreName: store, Request: builder.write(1)}
	result, err := client.Create(ctx, write)
	applied(t, result, err)
	deleted := false
	t.Cleanup(func() {
		if deleted {
			return
		}
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupClient, err := weir.Open(cleanup, options)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanupClient.Close()
		request := &weir.DeleteRequest{Resource: builder.resource}
		remove := weir.DeleteOptions{StoreName: store, Request: request}
		result, err := cleanupClient.Delete(cleanup, remove)
		if err != nil || result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
			t.Error("owned record cleanup:", result, err)
		}
	})
	duplicate, err := client.Create(ctx, write)
	if err != nil || duplicate.GetOutcome() != weir.MutationNotApplied || duplicate.GetFailure().GetCode() != weir.FailurePreconditionFailed {
		t.Fatalf("duplicate create: %v %v", duplicate, err)
	}
	write.Request = builder.write(4)
	result, err = client.Replace(ctx, write)
	applied(t, result, err)
	result, err = client.Put(ctx, write)
	applied(t, result, err)
	transform := weir.AtomicTransformOptions{StoreName: store, Request: builder.transform(4)}
	result, err = client.AtomicTransform(ctx, transform)
	applied(t, result, err)
	read := &weir.ReadRequest{Resource: builder.resource}
	readOptions := weir.ReadOneOptions{StoreName: store, Request: read}
	readResult, err := client.ReadOne(ctx, readOptions)
	if err != nil || readResult == nil || readResult.Document == nil || number(t, backend, readResult.Document.GetData()) != 4 {
		t.Fatalf("persisted read: %v %v", readResult, err)
	}
	write.Request = builder.write(5)
	result, err = client.Replace(ctx, write)
	applied(t, result, err)
	reads := weir.ReadOptions{StoreName: store, Requests: []*weir.ReadRequest{read, read}}
	batchResults, err := client.Read(ctx, reads)
	if err != nil || len(batchResults) != 2 {
		t.Fatal("batch read failed", batchResults, err)
	}
	for _, item := range batchResults {
		if number(t, backend, item.Document.GetData()) != 5 {
			t.Fatal("read-after-write batch failed")
		}
	}
	selector := &weir.Document{MediaType: "application/json", Data: []byte(`{"query":{"ids":{"values":["` + id + `"]}}}`)}
	if backend == "mongo" {
		filter := bson.D{{Key: "_id", Value: id}}
		query := bson.D{{Key: "filter", Value: filter}}
		selector = bsonDocument(t, query)
	}
	scan := &weir.ScanRequest{Resource: collection, Selector: selector, PageSize: 1}
	count := 0
	scanOptions := weir.ScanOptions{StoreName: store, Request: scan}
	scanOptions.Consume = func(context.Context, *weir.Document) error { count++; return nil }
	deadline := time.Now().Add(10 * time.Second)
	for {
		count = 0
		end, err := client.Scan(ctx, scanOptions)
		if err != nil || end.GetFailure() != nil {
			t.Fatal("Scan:", end, err)
		}
		if count == 1 {
			break
		}
		if count > 1 || time.Now().After(deadline) {
			t.Fatalf("Scan count=%d", count)
		}
		time.Sleep(100 * time.Millisecond) // Explicit read-only Search visibility poll.
	}
	native := nativeRequest(t, backend, collection, id)
	var body []byte
	var head *weir.NativeHead
	nativeOptions := weir.NativeOptions{StoreName: store, Request: native}
	nativeOptions.Consume = func(_ context.Context, event *weir.Event) error {
		if event.Head != nil {
			head = event.Head
		}
		if len(body)+len(event.Chunk) > 1<<20 {
			return errors.New("fixture response too large")
		}
		body = append(body, event.Chunk...)
		return nil
	}
	end, err := client.Native(ctx, nativeOptions)
	if err != nil || end.GetCompletion() != weir.NativeResponseComplete {
		t.Fatalf("Native: %v %v", end, err)
	}
	if backend == "mongo" {
		raw := bson.Raw(body)
		if raw.Lookup("n").AsInt64() != 1 || raw.Lookup("ok").AsFloat64() != 1 {
			t.Fatal("native count mismatch")
		}
	} else {
		metadata, err := weir.DecodeSearchHTTPResponse(head.GetMetadata())
		if err != nil {
			t.Fatal(err)
		}
		if metadata.StatusCode != 200 {
			t.Fatalf("native HTTP status %d", metadata.StatusCode)
		}
	}
	deleteRequest := &weir.DeleteRequest{Resource: builder.resource}
	deleteOptions := weir.DeleteOptions{StoreName: store, Request: deleteRequest}
	result, err = client.Delete(ctx, deleteOptions)
	applied(t, result, err)
	deleted = true
	readResult, err = client.ReadOne(ctx, readOptions)
	if err != nil || readResult == nil || !readResult.Missing {
		t.Fatalf("deleted read: %v %v", readResult, err)
	}
	t.Logf("%s: Create, duplicate precondition, Replace, Put, AtomicTransform, Read, ordered streamed read-after-write, Scan, native Execute and Delete verified", backend)
}

func applied(t *testing.T, result *weir.MutationResult, err error) {
	t.Helper()
	if err != nil || result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
		t.Fatalf("mutation acknowledgement: %v %v", result, err)
	}
}
func bsonDocument(t *testing.T, value bson.D) *weir.Document {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &weir.Document{MediaType: "application/bson", Data: data}
	return document
}

type mutationBuilder struct {
	t                     *testing.T
	backend, resource, id string
}

func (b mutationBuilder) write(n int) *weir.WriteRequest {
	b.t.Helper()
	document := &weir.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if b.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: b.id}, {Key: "n", Value: n}}
		document = bsonDocument(b.t, value)
	}
	request := &weir.WriteRequest{Resource: b.resource, Document: document}
	return request
}
func (b mutationBuilder) transform(n int) *weir.AtomicTransformRequest {
	b.t.Helper()
	expression := &weir.Document{MediaType: "application/vnd.weir.search-update.v1+json", Data: []byte(fmt.Sprintf(`{"doc":{"n":%d}}`, n))}
	if b.backend == "mongo" {
		fields := bson.D{{Key: "n", Value: n}}
		update := bson.D{{Key: "$set", Value: fields}}
		expression = bsonDocument(b.t, update)
		expression.MediaType = "application/vnd.weir.mongodb-update.v1+bson"
	}
	request := &weir.AtomicTransformRequest{Resource: b.resource, BackendExpression: expression}
	return request
}
func number(t *testing.T, backend string, data []byte) int64 {
	t.Helper()
	if backend == "mongo" {
		return bson.Raw(data).Lookup("n").AsInt64()
	}
	var value struct {
		N int64 `json:"n"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value.N
}
func nativeRequest(t *testing.T, backend, collection, id string) *weir.NativeRequest {
	t.Helper()
	request := &weir.NativeRequest{Resource: collection}
	if backend == "mongo" {
		parts := strings.Split(collection, "/")
		query := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		request.Body = bsonDocument(t, command).Data
		request.Descriptor = &weir.Document{MediaType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		request.BodyMediaType = "application/bson"
	} else {
		descriptor := &weir.SearchHTTPRequest{Method: "GET", Path: "/_doc/" + id}
		encoded, err := weir.SearchHTTPDescriptor(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		request.Descriptor = encoded
	}
	return request
}
