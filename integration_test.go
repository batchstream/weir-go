//go:build integration

package weir_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func TestMongoStreaming(t *testing.T)  { testStreaming(t, "mongo") }
func TestSearchStreaming(t *testing.T) { testStreaming(t, "search") }

type integrationTarget struct {
	address    string
	store      string
	collection string
}

func integrationFixture(t *testing.T, backend string) integrationTarget {
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
	target := integrationTarget{address: address, store: store, collection: collection}
	return target
}

func testLifecycle(t *testing.T, backend string) {
	t.Helper()
	target := integrationFixture(t, backend)
	store, collection := target.store, target.collection
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := weir.New(target.address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id := fmt.Sprintf("sdk-%s-%d", backend, time.Now().UnixNano())
	builder := mutationBuilder{t: t, backend: backend, resource: collection + "/s:" + id, id: id}
	write := weir.WriteOptions{StoreName: store, Request: builder.write(1)}
	deleted := false
	t.Cleanup(func() {
		if deleted {
			return
		}
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupClient, err := weir.New(target.address)
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
	result, err := client.Create(ctx, write)
	applied(t, result, err)
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
	native := nativeRequest(t, backend, collection, id)
	var body []byte
	nativeOptions := weir.NativeOptions{StoreName: store, Request: native}
	nativeOptions.Consume = func(_ context.Context, _ *weir.NativeResponse, data []byte) error {
		if len(body)+len(data) > 1<<20 {
			return errors.New("fixture response too large")
		}
		body = append(body, data...)
		return nil
	}
	resultNative, err := client.Native(ctx, nativeOptions)
	if err != nil || resultNative == nil || resultNative.Completion != weir.NativeResponseComplete {
		t.Fatalf("Native: %v %v", resultNative, err)
	}
	if backend == "mongo" {
		raw := bson.Raw(body)
		if raw.Lookup("n").AsInt64() != 1 || raw.Lookup("ok").AsFloat64() != 1 {
			t.Fatal("native count mismatch")
		}
	} else {
		metadata, err := weir.ParseHTTPNativeResponse(resultNative.Response)
		if err != nil || metadata.StatusCode != http.StatusOK {
			t.Fatalf("native HTTP metadata: %v %v", metadata, err)
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
	t.Logf("%s: Create, duplicate precondition, Replace, Put, AtomicTransform, Read, ordered streamed read-after-write, native Execute and Delete verified", backend)
}

func testStreaming(t *testing.T, backend string) {
	t.Helper()
	target := integrationFixture(t, backend)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	client, err := weir.New(target.address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	idPrefix := fmt.Sprintf("sdk-stream-%s-%d", backend, time.Now().UnixNano())
	builders := make([]mutationBuilder, 3)
	ids := make([]string, len(builders))
	for index := range builders {
		ids[index] = fmt.Sprintf("%s-%d", idPrefix, index)
		builders[index] = mutationBuilder{t: t, backend: backend, resource: target.collection + "/s:" + ids[index], id: ids[index]}
	}
	// Register every owned key before sending: an unacknowledged create may apply.
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupClient, err := weir.New(target.address)
		if err != nil {
			t.Error("record cleanup client:", err)
			return
		}
		defer cleanupClient.Close()
		for _, builder := range builders {
			request := &weir.DeleteRequest{Resource: builder.resource}
			options := weir.DeleteOptions{StoreName: target.store, Request: request}
			result, err := cleanupClient.Delete(cleanup, options)
			if err != nil || result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
				t.Error("owned record cleanup:", result, err)
			}
		}
	})
	first := builders[0].write(0)
	updated := builders[0].write(10)
	second := builders[1].write(1)
	third := builders[2].write(2)
	create := &weir.MutateRequest{Resource: first.Resource, Action: weir.MutationCreate, Document: first.Document}
	put := &weir.MutateRequest{Resource: updated.Resource, Action: weir.MutationPut, Document: updated.Document}
	createSecond := &weir.MutateRequest{Resource: second.Resource, Action: weir.MutationCreate, Document: second.Document}
	createThird := &weir.MutateRequest{Resource: third.Resource, Action: weir.MutationCreate, Document: third.Document}
	mutations := []*weir.MutateRequest{create, create, put, createSecond, createThird}
	position, confirmed := 0, 0
	mutate := weir.MutateStreamOptions{StoreName: target.store}
	mutate.Next = func(context.Context) (*weir.MutateRequest, error) {
		if position == len(mutations) {
			return nil, io.EOF
		}
		request := mutations[position]
		position++
		return request, nil
	}
	mutate.Consume = func(_ context.Context, index uint64, result *weir.MutationResult) error {
		confirmed++
		if index != uint64(confirmed) {
			return fmt.Errorf("mutation ordinal %d, expected %d", index, confirmed)
		}
		if index == 2 {
			if result.GetOutcome() != weir.MutationNotApplied || result.GetFailure().GetCode() != weir.FailurePreconditionFailed {
				return fmt.Errorf("duplicate create evidence: %v", result)
			}
			return nil
		}
		if result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
			return fmt.Errorf("mutation %d: %v", index, result)
		}
		return nil
	}
	if err := client.MutateStream(ctx, mutate); err != nil || confirmed != len(mutations) {
		t.Fatalf("stream mutation acknowledgements=%d: %v", confirmed, err)
	}
	lua := &weir.LuaTransform{Source: []byte(`return function(current, incoming)
    current.n = current.n + 7
    return current
end`)}
	transform := &weir.AtomicTransformRequest{Resource: first.Resource, Lua: lua}
	transformOptions := weir.AtomicTransformOptions{StoreName: target.store, Request: transform}
	result, err := client.AtomicTransform(ctx, transformOptions)
	applied(t, result, err)
	resources := []string{third.Resource, first.Resource, target.collection + "/s:" + idPrefix + "-missing", second.Resource, first.Resource}
	expected := []int64{2, 17, -1, 1, 17}
	position, confirmed = 0, 0
	read := weir.ReadStreamOptions{StoreName: target.store}
	read.Next = func(context.Context) (*weir.ReadRequest, error) {
		if position == len(resources) {
			return nil, io.EOF
		}
		request := &weir.ReadRequest{Resource: resources[position]}
		position++
		return request, nil
	}
	read.Consume = func(_ context.Context, index uint64, result *weir.ReadResult) error {
		confirmed++
		if index != uint64(confirmed) || result == nil || result.Failure != nil {
			return fmt.Errorf("read %d: %v", index, result)
		}
		if expected[index-1] == -1 {
			if !result.Missing || result.Document != nil {
				return fmt.Errorf("read %d did not confirm missing: %v", index, result)
			}
		} else if result.Missing || result.Document == nil || number(t, backend, result.Document.Data) != expected[index-1] {
			return fmt.Errorf("read %d did not preserve same-key mutation order: %v", index, result)
		}
		return nil
	}
	if err := client.ReadStream(ctx, read); err != nil || confirmed != len(resources) {
		t.Fatalf("stream read acknowledgements=%d: %v", confirmed, err)
	}
	t.Logf("%s: stream same-key order, duplicate-create continuation, Lua readback and repeated/missing streamed reads verified", backend)
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
	document := &weir.Document{ContentType: "application/bson", Data: data}
	return document
}

type mutationBuilder struct {
	t                     *testing.T
	backend, resource, id string
}

func (b mutationBuilder) write(n int) *weir.WriteRequest {
	b.t.Helper()
	document := &weir.Document{ContentType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d,"unprojected":true}`, n))}
	if b.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: b.id}, {Key: "n", Value: n}, {Key: "unprojected", Value: true}}
		document = bsonDocument(b.t, value)
	}
	request := &weir.WriteRequest{Resource: b.resource, Document: document}
	return request
}
func (b mutationBuilder) transform(n int) *weir.AtomicTransformRequest {
	b.t.Helper()
	expression := &weir.Document{ContentType: "application/vnd.weir.search-update.v1+json", Data: []byte(fmt.Sprintf(`{"doc":{"n":%d}}`, n))}
	if b.backend == "mongo" {
		fields := bson.D{{Key: "n", Value: n}}
		update := bson.D{{Key: "$set", Value: fields}}
		expression = bsonDocument(b.t, update)
		expression.ContentType = "application/vnd.weir.mongodb-update.v1+bson"
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
	if backend == "mongo" {
		parts, err := protocol.ParseRelativeResource(collection)
		if err != nil || len(parts) != 2 {
			t.Fatal("Mongo integration resource requires database/collection", collection, err)
		}
		query := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "count", Value: parts[1]}, {Key: "query", Value: query}}
		document := bsonDocument(t, command)
		request := &weir.NativeRequest{Resource: protocol.EncodeSegment(parts[0]), Request: document}
		return request
	}
	httpRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/_doc/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request, err := weir.NewHTTPNativeRequest(collection, httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	return request
}
