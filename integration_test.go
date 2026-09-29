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
	spb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

// These tests only contact a server when both the build tag and explicit endpoint
// are provided. Each test owns one new record; databases/indexes must already exist.
func TestMongoLifecycle(t *testing.T)  { testLifecycle(t, "mongo", "WEIR_MONGO_RESOURCE") }
func TestSearchLifecycle(t *testing.T) { testLifecycle(t, "search", "WEIR_SEARCH_RESOURCE") }

func testLifecycle(t *testing.T, backend, variable string) {
	t.Helper()
	address, collection := os.Getenv("WEIR_ADDRESS"), os.Getenv(variable)
	if address == "" || collection == "" {
		t.Skip("requires WEIR_ADDRESS and " + variable)
	}
	opts := weir.Options{Plaintext: true, Timeout: 10 * time.Second}
	client, err := weir.New(address, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	id := fmt.Sprintf("sdk-%s-%d", backend, time.Now().UnixNano())
	resource := collection + "/s:" + id
	store := strings.SplitN(strings.TrimPrefix(collection, "weir://"), "/", 2)[0]
	builder := mutationBuilder{t: t, backend: backend, resource: resource, id: id}
	create := builder.mutation("create", 1)
	if _, err := client.Mutate(ctx, create); err != nil {
		t.Fatal(err)
	}
	deleted := false
	t.Cleanup(func() {
		if deleted {
			return
		}
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// Run before deferred Close by owning a separate connection for cleanup.
		cleanupClient, err := weir.New(address, opts)
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanupClient.Close()
		_, err = cleanupClient.Mutate(cleanup, builder.mutation("delete", 0))
		if err != nil {
			t.Error("owned record cleanup:", err)
		}
	})
	duplicate, err := client.Mutate(ctx, create)
	var failure *weir.FailureError
	if !errors.As(err, &failure) || failure.Failure.Code != pb.FailureCode_PRECONDITION_FAILED || duplicate.Outcome != pb.MutationOutcome_NOT_APPLIED {
		t.Fatalf("duplicate create: %v %v", duplicate, err)
	}
	for _, action := range []string{"replace", "put", "transform"} {
		if _, err := client.Mutate(ctx, builder.mutation(action, 4)); err != nil {
			t.Fatalf("%s: %v", action, err)
		}
	}
	read := &pb.ReadRequest{Resource: resource}
	result, err := client.Read(ctx, read)
	if err != nil || result.GetDocument() == nil {
		t.Fatalf("Read: %v %v", result, err)
	}
	if number(t, backend, result.GetDocument().Data) != 4 {
		t.Fatal("mutation not persisted")
	}
	operations := []weir.Operation{{Read: read}, {Mutate: builder.mutation("replace", 5)}, {Read: read}}
	results, err := client.Bulk(ctx, "weir://"+store, operations)
	if err != nil || len(results) != 3 {
		t.Fatalf("Bulk: %v %v", results, err)
	}
	if number(t, backend, results[2].GetRead().GetDocument().GetData()) != 5 {
		t.Fatal("Bulk ordering/read-after-write failed")
	}
	selector := &pb.Document{MediaType: weir.MediaTypeJSON, Data: []byte(`{"query":{"ids":{"values":["` + id + `"]}}}`)}
	if backend == "mongo" {
		filter := bson.D{{Key: "_id", Value: id}}
		query := bson.D{{Key: "filter", Value: filter}}
		selector = bsonDocument(t, query)
	}
	scan := &pb.ScanRequest{Resource: collection, Selector: selector, FetchItemsHint: 1}
	deadline := time.Now().Add(10 * time.Second)
	for {
		count := 0
		_, err := client.Scan(ctx, scan, func(doc *pb.Document) error { count++; return nil })
		if err != nil {
			t.Fatal("Scan:", err)
		}
		if count == 1 {
			break
		}
		if count > 1 || time.Now().After(deadline) {
			t.Fatalf("Scan count=%d", count)
		}
		// Search visibility follows the backend refresh interval. This explicit
		// test poll is read-only; the SDK itself never retries a call.
		time.Sleep(100 * time.Millisecond)
	}
	request := native(t, backend, collection, id)
	var body []byte
	nativeResult, err := client.Native(ctx, request, func(chunk []byte) error {
		if len(body)+len(chunk) > 1<<20 {
			return errors.New("fixture response too large")
		}
		body = append(body, chunk...)
		return nil
	})
	if err != nil || nativeResult.End.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
		t.Fatalf("Native: %v %v", nativeResult, err)
	}
	if backend == "mongo" {
		raw := bson.Raw(body)
		if raw.Lookup("n").AsInt64() != 1 {
			t.Fatal("Native count mismatch")
		}
	} else {
		metadata := &spb.Response{}
		if err := proto.Unmarshal(nativeResult.Head.Metadata.Data, metadata); err != nil {
			t.Fatal(err)
		}
		if metadata.StatusCode != 200 {
			t.Fatalf("Native HTTP status %d", metadata.StatusCode)
		}
	}
	if _, err := client.Mutate(ctx, builder.mutation("delete", 0)); err != nil {
		t.Fatal(err)
	}
	deleted = true
	result, err = client.Read(ctx, read)
	if err != nil || result.GetMissing() == nil {
		t.Fatalf("deleted Read: %v %v", result, err)
	}
	t.Logf("%s: Create, duplicate precondition, Replace, Put, AtomicTransform, Read, ordered Bulk, Scan, Native, Delete verified", backend)
}

func bsonDocument(t *testing.T, value bson.D) *pb.Document {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	doc := &pb.Document{MediaType: weir.MediaTypeBSON, Data: data}
	return doc
}

type mutationBuilder struct {
	t                     *testing.T
	backend, resource, id string
}

func (b mutationBuilder) mutation(action string, n int) *pb.MutateRequest {
	t := b.t
	t.Helper()
	doc := &pb.Document{MediaType: weir.MediaTypeJSON, Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if b.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: b.id}, {Key: "n", Value: n}}
		doc = bsonDocument(t, value)
	}
	req := &pb.MutateRequest{Resource: b.resource}
	switch action {
	case "create":
		req.Action = &pb.MutateRequest_Create{Create: doc}
	case "replace":
		req.Action = &pb.MutateRequest_Replace{Replace: doc}
	case "put":
		req.Action = &pb.MutateRequest_Put{Put: doc}
	case "delete":
		empty := &pb.Empty{}
		req.Action = &pb.MutateRequest_Delete{Delete: empty}
	case "transform":
		expression := &pb.Document{MediaType: "application/vnd.weir.search-update.v1+json", Data: []byte(fmt.Sprintf(`{"doc":{"n":%d}}`, n))}
		if b.backend == "mongo" {
			fields := bson.D{{Key: "n", Value: n}}
			update := bson.D{{Key: "$set", Value: fields}}
			expression = bsonDocument(t, update)
			expression.MediaType = "application/vnd.weir.mongodb-update.v1+bson"
		}
		form := &pb.Transform_BackendExpression{BackendExpression: expression}
		transform := &pb.Transform{Form: form}
		req.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	}
	return req
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

func native(t *testing.T, backend, collection, id string) weir.NativeRequest {
	t.Helper()
	open := &pb.NativeOpen{Resource: collection}
	req := weir.NativeRequest{Open: open}
	if backend == "mongo" {
		parts := strings.Split(collection, "/")
		query := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		req.Body = bsonDocument(t, command).Data
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		open.BodyMediaType = weir.MediaTypeBSON
	} else {
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/" + id}
		raw, err := proto.Marshal(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.search-http.v1+protobuf", Data: raw}
	}
	return req
}
