//go:build integration

package weir_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	spb "github.com/batchstream/weir-protocol/api/weir/search/v1"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

// Only an explicit tag and operator-provided endpoint enable backend I/O. Each
// test owns one generated record in a pre-created disposable collection/index.
func TestMongoLifecycle(t *testing.T)  { testLifecycle(t, "mongo", "WEIR_MONGO_RESOURCE") }
func TestSearchLifecycle(t *testing.T) { testLifecycle(t, "search", "WEIR_SEARCH_RESOURCE") }

func testLifecycle(t *testing.T, backend, variable string) {
	t.Helper()
	address, collection := os.Getenv("WEIR_ADDRESS"), os.Getenv(variable)
	if address == "" || collection == "" {
		t.Skip("requires WEIR_ADDRESS and " + variable)
	}
	store, _, err := protocol.ParseResource(collection)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	collection = strings.TrimPrefix(collection, "weir://"+store+"/")
	options := weir.OpenOptions{Seed: address, Stores: []string{store}}
	client, err := weir.Open(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id := fmt.Sprintf("sdk-%s-%d", backend, time.Now().UnixNano())
	builder := mutationBuilder{t: t, backend: backend, resource: collection + "/s:" + id, id: id}
	recordOptions := weir.RecordOptions{StoreName: store, Call: builder.call("create", 1)}
	result, err := client.Record(ctx, recordOptions)
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
		remove := weir.RecordOptions{StoreName: store, Call: builder.call("delete", 0)}
		result, err := cleanupClient.Record(cleanup, remove)
		if err != nil || result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
			t.Error("owned record cleanup:", result, err)
		}
	})
	duplicate, err := client.Record(ctx, recordOptions)
	if err != nil || duplicate.GetMutation().GetOutcome() != pb.MutationOutcome_NOT_APPLIED || duplicate.GetMutation().GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED {
		t.Fatalf("duplicate create: %v %v", duplicate, err)
	}
	for _, action := range []string{"replace", "put", "transform"} {
		recordOptions.Call = builder.call(action, 4)
		result, err = client.Record(ctx, recordOptions)
		applied(t, result, err)
	}
	read := &pb.ReadRequest{Resource: builder.resource}
	readVariant := &pb.Call_Read{Read: read}
	readCall := &pb.Call{Version: 1, Operation: readVariant}
	readOptions := weir.RecordOptions{StoreName: store, Call: readCall}
	result, err = client.Record(ctx, readOptions)
	if err != nil || result.GetRead().GetDocument() == nil || number(t, backend, result.GetRead().GetDocument().GetData()) != 4 {
		t.Fatalf("persisted read: %v %v", result, err)
	}
	// Execute can have several requests in flight. This application explicitly
	// waits for Complete before sending dependent reads and mutations.
	calls := []*pb.Call{readCall, builder.call("replace", 5), readCall}
	results := make([]*pb.Result, len(calls))
	completed := make(chan uint64, 1)
	produced := 0
	batch := weir.Options{StoreName: store}
	batch.Produce = func(ctx context.Context) (*pb.Call, error) {
		if produced > 0 {
			select {
			case <-completed:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if produced == len(calls) {
			return nil, io.EOF
		}
		call := calls[produced]
		produced++
		return call, nil
	}
	batch.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
		results[id-1] = event.GetResult()
		return nil
	}
	batch.Complete = func(ctx context.Context, id uint64) error {
		select {
		case completed <- id:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := client.Execute(ctx, batch); err != nil {
		t.Fatal("finite mixed Execute:", err)
	}
	applied(t, results[1], nil)
	if number(t, backend, results[2].GetRead().GetDocument().GetData()) != 5 {
		t.Fatal("Complete-gated read-after-write failed")
	}
	selector := &pb.Document{MediaType: "application/json", Data: []byte(`{"query":{"ids":{"values":["` + id + `"]}}}`)}
	if backend == "mongo" {
		filter := bson.D{{Key: "_id", Value: id}}
		query := bson.D{{Key: "filter", Value: filter}}
		selector = bsonDocument(t, query)
	}
	scan := &pb.ScanRequest{Resource: collection, Selector: selector, PageSize: 1}
	count := 0
	scanOptions := weir.ScanPageOptions{StoreName: store, Request: scan}
	scanOptions.Consume = func(context.Context, *pb.Document) error { count++; return nil }
	deadline := time.Now().Add(10 * time.Second)
	for {
		count = 0
		end, err := client.ScanPage(ctx, scanOptions)
		if err != nil || end.GetFailure() != nil {
			t.Fatal("ScanPage:", end, err)
		}
		if count == 1 {
			break
		}
		if count > 1 || time.Now().After(deadline) {
			t.Fatalf("ScanPage count=%d", count)
		}
		time.Sleep(100 * time.Millisecond) // Explicit read-only Search visibility poll.
	}
	native := nativeCall(t, backend, collection, id)
	nativeVariant := &pb.Call_Native{Native: native}
	nativeRequest := &pb.Call{Version: 1, Operation: nativeVariant}
	producedNative := false
	var body []byte
	var head *pb.NativeHead
	var end *pb.NativeEnd
	nativeOptions := weir.Options{StoreName: store}
	nativeOptions.Produce = func(context.Context) (*pb.Call, error) {
		if producedNative {
			return nil, io.EOF
		}
		producedNative = true
		return nativeRequest, nil
	}
	nativeOptions.Consume = func(_ context.Context, _ uint64, event *pb.Event) error {
		if value := event.GetHead(); value != nil {
			head = value
		}
		if len(body)+len(event.GetChunk()) > 1<<20 {
			return errors.New("fixture response too large")
		}
		body = append(body, event.GetChunk()...)
		if value := event.GetNativeEnd(); value != nil {
			end = value
		}
		return nil
	}
	if err := client.Execute(ctx, nativeOptions); err != nil || end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE {
		t.Fatalf("native Execute: %v %v", end, err)
	}
	if backend == "mongo" {
		raw := bson.Raw(body)
		if raw.Lookup("n").AsInt64() != 1 || raw.Lookup("ok").AsFloat64() != 1 {
			t.Fatal("native count mismatch")
		}
	} else {
		metadata := &spb.Response{}
		if err := proto.Unmarshal(head.GetMetadata().GetData(), metadata); err != nil {
			t.Fatal(err)
		}
		if metadata.StatusCode != 200 {
			t.Fatalf("native HTTP status %d", metadata.StatusCode)
		}
	}
	recordOptions.Call = builder.call("delete", 0)
	result, err = client.Record(ctx, recordOptions)
	applied(t, result, err)
	deleted = true
	result, err = client.Record(ctx, readOptions)
	if err != nil || result.GetRead().GetMissing() == nil {
		t.Fatalf("deleted read: %v %v", result, err)
	}
	t.Logf("%s: Create, duplicate precondition, Replace, Put, AtomicTransform, Read, Complete-gated mixed Execute, ScanPage, native Execute and Delete verified", backend)
}

func applied(t *testing.T, result *pb.Result, err error) {
	t.Helper()
	if err != nil || result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
		t.Fatalf("mutation acknowledgement: %v %v", result, err)
	}
}
func bsonDocument(t *testing.T, value bson.D) *pb.Document {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: "application/bson", Data: data}
	return document
}

type mutationBuilder struct {
	t                     *testing.T
	backend, resource, id string
}

func (b mutationBuilder) call(action string, n int) *pb.Call {
	t := b.t
	t.Helper()
	document := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	if b.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: b.id}, {Key: "n", Value: n}}
		document = bsonDocument(t, value)
	}
	request := &pb.MutateRequest{Resource: b.resource}
	switch action {
	case "create":
		request.Action = &pb.MutateRequest_Create{Create: document}
	case "replace":
		request.Action = &pb.MutateRequest_Replace{Replace: document}
	case "put":
		request.Action = &pb.MutateRequest_Put{Put: document}
	case "delete":
		empty := &pb.Empty{}
		request.Action = &pb.MutateRequest_Delete{Delete: empty}
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
		request.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	default:
		t.Fatal("unknown mutation action", action)
	}
	variant := &pb.Call_Mutate{Mutate: request}
	call := &pb.Call{Version: 1, Operation: variant}
	return call
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
func nativeCall(t *testing.T, backend, collection, id string) *pb.NativeCall {
	t.Helper()
	open := &pb.NativeOpen{Resource: collection}
	call := &pb.NativeCall{Open: open}
	if backend == "mongo" {
		parts := strings.Split(collection, "/")
		query := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		call.Body = bsonDocument(t, command).Data
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.mongodb-command.v1+protobuf"}
		open.BodyMediaType = "application/bson"
	} else {
		descriptor := &spb.Request{Method: "GET", Path: "/_doc/" + id}
		raw, err := proto.Marshal(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		open.Descriptor_ = &pb.Document{MediaType: "application/vnd.weir.search-http.v1+protobuf", Data: raw}
	}
	return call
}
