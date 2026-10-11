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

func TestMongoStreamingAndPagination(t *testing.T)  { testStreamingAndPagination(t, "mongo") }
func TestSearchStreamingAndPagination(t *testing.T) { testStreamingAndPagination(t, "search") }

type integrationTarget struct {
	options    weir.OpenOptions
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
	options := weir.OpenOptions{Seed: address, Stores: []string{store}}
	target := integrationTarget{options: options, store: store, collection: collection}
	return target
}

func testLifecycle(t *testing.T, backend string) {
	t.Helper()
	target := integrationFixture(t, backend)
	store, collection := target.store, target.collection
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	options := target.options
	client, err := weir.Open(ctx, options)
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
	filter := &weir.Document{ContentType: "application/json", Data: []byte(`{"ids":{"values":["` + id + `"]}}`)}
	if backend == "mongo" {
		query := bson.D{{Key: "_id", Value: id}}
		filter = bsonDocument(t, query)
	}
	scan := &weir.ScanRequest{Resource: collection, Filter: filter, PageSize: 1}
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
	t.Logf("%s: Create, duplicate precondition, Replace, Put, AtomicTransform, Read, ordered streamed read-after-write, Scan, native Execute and Delete verified", backend)
}

func testStreamingAndPagination(t *testing.T, backend string) {
	t.Helper()
	target := integrationFixture(t, backend)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	client, err := weir.Open(ctx, target.options)
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
		cleanupClient, err := weir.Open(cleanup, target.options)
		if err != nil {
			t.Error("open record cleanup:", err)
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
	nativeFilter := map[string]any{"ids": map[string]any{"values": ids}}
	filterData, err := json.Marshal(nativeFilter)
	if err != nil {
		t.Fatal(err)
	}
	filter := &weir.Document{ContentType: "application/json", Data: filterData}
	if backend == "mongo" {
		condition := bson.D{{Key: "$in", Value: ids}}
		query := bson.D{{Key: "_id", Value: condition}}
		filter = bsonDocument(t, query)
	}
	projection := &weir.Projection{Mode: weir.ProjectionInclude, Fields: []string{"n"}}
	scan := &weir.ScanRequest{Resource: target.collection, Filter: filter, Projection: projection, PageSize: 1}
	scanOptions := weir.ScanOptions{StoreName: target.store, Request: scan}
	seen := make(map[int64]bool)
	scanOptions.Consume = func(_ context.Context, document *weir.Document) error {
		if backend == "mongo" {
			if bson.Raw(document.Data).Lookup("unprojected").Type != 0 {
				return errors.New("Mongo projection retained an unselected field")
			}
		} else {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(document.Data, &fields); err != nil {
				return err
			}
			if _, exists := fields["unprojected"]; exists {
				return errors.New("Search projection retained an unselected field")
			}
		}
		value := number(t, backend, document.Data)
		if value != 1 && value != 2 && value != 17 || seen[value] {
			return fmt.Errorf("unexpected or duplicate traversal value %d", value)
		}
		seen[value] = true
		return nil
	}
	// Wait until every owned record is visible before Search pins a snapshot.
	scan.PageSize = uint32(len(builders))
	visibilityDeadline := time.Now().Add(10 * time.Second)
	var end *weir.ScanEnd
	for {
		clear(seen)
		end, err = client.Scan(ctx, scanOptions)
		if err != nil || end.GetFailure() != nil {
			t.Fatal("initial Scan:", end, err)
		}
		if len(seen) == len(builders) {
			break
		}
		if time.Now().After(visibilityDeadline) {
			t.Fatal("created documents did not become visible to Scan")
		}
		time.Sleep(100 * time.Millisecond)
	}
	clear(seen)
	scan.PageSize = 1
	end, err = client.Scan(ctx, scanOptions)
	if err != nil || end.GetFailure() != nil {
		t.Fatal("first bounded Scan:", end, err)
	}
	if end.Exhausted || len(end.NextContinuationToken) == 0 || end.DocumentCount != 1 {
		t.Fatal("first bounded page has no continuation:", end)
	}
	// Reopening removes any SDK state that could accidentally make tokens work.
	continuationClient, err := weir.Open(ctx, target.options)
	if err != nil {
		t.Fatal(err)
	}
	defer continuationClient.Close()
	scan.PageSize = 2
	for page := 0; !end.Exhausted; page++ {
		if page >= len(builders) {
			t.Fatal("Scan did not terminate within its owned record count")
		}
		scan.ContinuationToken = end.NextContinuationToken
		end, err = continuationClient.Scan(ctx, scanOptions)
		if err != nil || end.GetFailure() != nil {
			t.Fatal("continued Scan:", end, err)
		}
	}
	if len(seen) != len(builders) || len(end.NextContinuationToken) != 0 {
		t.Fatalf("completed traversal count=%d, end=%v", len(seen), end)
	}
	t.Logf("%s: stream same-key order, duplicate-create continuation, Lua readback, repeated/missing streamed reads and projected checkpoint continuation on a reopened client verified", backend)
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
		parts := strings.Split(collection, "/")
		query := bson.D{{Key: "_id", Value: id}}
		command := bson.D{{Key: "count", Value: parts[len(parts)-1]}, {Key: "query", Value: query}}
		document := bsonDocument(t, command)
		request := &weir.NativeRequest{Resource: collection, Request: document}
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
