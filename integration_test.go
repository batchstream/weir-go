//go:build integration

package weir_test

import (
	"context"
	"encoding/json"
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
	remove := &weir.MutateRequest{Resource: builder.resource, Action: weir.MutationDelete}
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
		result, err := mutateOne(cleanup, cleanupClient, store, remove)
		if err != nil || result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
			t.Error("owned record cleanup:", result, err)
		}
	})
	create := builder.write(weir.MutationCreate, 1)
	result, err := mutateOne(ctx, client, store, create)
	applied(t, result, err)
	duplicate, err := mutateOne(ctx, client, store, create)
	if err != nil || duplicate.GetOutcome() != weir.MutationNotApplied || duplicate.GetFailure().GetCode() != weir.FailurePreconditionFailed {
		t.Fatalf("duplicate create: %v %v", duplicate, err)
	}
	result, err = mutateOne(ctx, client, store, builder.write(weir.MutationReplace, 4))
	applied(t, result, err)
	result, err = mutateOne(ctx, client, store, builder.write(weir.MutationPut, 4))
	applied(t, result, err)
	result, err = mutateOne(ctx, client, store, builder.transform(4))
	applied(t, result, err)
	read := &weir.ReadRequest{Resource: builder.resource}
	readResult, err := readOne(ctx, client, store, read)
	if err != nil || readResult == nil || readResult.Document == nil || number(t, backend, readResult.Document.GetData()) != 4 {
		t.Fatalf("persisted read: %v %v", readResult, err)
	}
	result, err = mutateOne(ctx, client, store, builder.write(weir.MutationReplace, 5))
	applied(t, result, err)
	batchResults, err := client.Read(ctx, store, weir.WithRequests(read, read))
	if err != nil || len(batchResults) != 2 {
		t.Fatal("batch read failed", batchResults, err)
	}
	for _, item := range batchResults {
		if number(t, backend, item.Document.GetData()) != 5 {
			t.Fatal("read-after-write batch failed")
		}
	}
	native := nativeRequest(t, backend, collection, id)
	resultNative, err := client.Native(ctx, store, native)
	if err != nil || resultNative == nil || resultNative.Completion != weir.NativeResponseComplete {
		t.Fatalf("Native: %v %v", resultNative, err)
	}
	if backend == "mongo" {
		raw := bson.Raw(resultNative.Body)
		if raw.Lookup("n").AsInt64() != 1 || raw.Lookup("ok").AsFloat64() != 1 {
			t.Fatal("native count mismatch")
		}
	} else {
		metadata, err := weir.ParseHTTPNativeResponse(resultNative.Response)
		if err != nil || metadata.StatusCode != http.StatusOK {
			t.Fatalf("native HTTP metadata: %v %v", metadata, err)
		}
	}
	result, err = mutateOne(ctx, client, store, remove)
	applied(t, result, err)
	deleted = true
	readResult, err = readOne(ctx, client, store, read)
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
			remove := &weir.MutateRequest{Resource: builder.resource, Action: weir.MutationDelete}
			result, err := mutateOne(cleanup, cleanupClient, target.store, remove)
			if err != nil || result.GetOutcome() != weir.MutationApplied || result.GetFailure() != nil {
				t.Error("owned record cleanup:", result, err)
			}
		}
	})
	create := builders[0].write(weir.MutationCreate, 0)
	// The rest follow from a producer, in order after the requests.
	produced := []*weir.MutateRequest{builders[0].write(weir.MutationPut, 10), builders[1].write(weir.MutationCreate, 1), builders[2].write(weir.MutationCreate, 2)}
	total := 2 + len(produced)
	// Results arrive as records complete; each index appears once.
	position, confirmed := 0, 0
	seen := make(map[int]bool)
	nextMutation := func(context.Context) (*weir.MutateRequest, error) {
		if position == len(produced) {
			return nil, io.EOF
		}
		request := produced[position]
		position++
		return request, nil
	}
	consumeMutation := func(_ context.Context, index int, result *weir.MutationResult) error {
		confirmed++
		if index >= total || seen[index] {
			return fmt.Errorf("unexpected mutation index %d", index)
		}
		seen[index] = true
		if index == 1 {
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
	_, err = client.Mutate(ctx, target.store, weir.WithRequests(create, create), weir.WithNext(nextMutation), weir.WithMutateConsume(consumeMutation))
	if err != nil || confirmed != total {
		t.Fatalf("stream mutation acknowledgements=%d: %v", confirmed, err)
	}
	lua := &weir.LuaTransform{Source: []byte(`return function(current, incoming)
    current.n = current.n + 7
    return current
end`)}
	transform := &weir.MutateRequest{Resource: create.Resource, Action: weir.MutationAtomicTransform, Lua: lua}
	result, err := mutateOne(ctx, client, target.store, transform)
	applied(t, result, err)
	resources := []string{builders[2].resource, create.Resource, target.collection + "/s:" + idPrefix + "-missing", builders[1].resource, create.Resource}
	expected := []int64{2, 17, -1, 1, 17}
	position, confirmed = 0, 0
	clear(seen)
	nextRead := func(context.Context) (*weir.ReadRequest, error) {
		if position == len(resources) {
			return nil, io.EOF
		}
		request := &weir.ReadRequest{Resource: resources[position]}
		position++
		return request, nil
	}
	consumeRead := func(_ context.Context, index int, result *weir.ReadResult) error {
		confirmed++
		if index >= len(resources) || seen[index] || result == nil || result.Failure != nil {
			return fmt.Errorf("read %d: %v", index, result)
		}
		seen[index] = true
		if expected[index] == -1 {
			if !result.Missing || result.Document != nil {
				return fmt.Errorf("read %d did not confirm missing: %v", index, result)
			}
		} else if result.Missing || result.Document == nil || number(t, backend, result.Document.Data) != expected[index] {
			return fmt.Errorf("read %d did not preserve same-key mutation order: %v", index, result)
		}
		return nil
	}
	_, err = client.Read(ctx, target.store, weir.WithNext(nextRead), weir.WithReadConsume(consumeRead))
	if err != nil || confirmed != len(resources) {
		t.Fatalf("stream read acknowledgements=%d: %v", confirmed, err)
	}
	t.Logf("%s: stream same-key order, duplicate-create continuation, Lua readback and repeated/missing streamed reads verified", backend)
}

func mutateOne(ctx context.Context, client *weir.Client, store string, request *weir.MutateRequest) (*weir.MutationResult, error) {
	results, err := client.Mutate(ctx, store, weir.WithRequests(request))
	return results[0], err
}

func readOne(ctx context.Context, client *weir.Client, store string, request *weir.ReadRequest) (*weir.ReadResult, error) {
	results, err := client.Read(ctx, store, weir.WithRequests(request))
	return results[0], err
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

func (b mutationBuilder) write(action weir.MutationAction, n int) *weir.MutateRequest {
	b.t.Helper()
	document := &weir.Document{ContentType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d,"unprojected":true}`, n))}
	if b.backend == "mongo" {
		value := bson.D{{Key: "_id", Value: b.id}, {Key: "n", Value: n}, {Key: "unprojected", Value: true}}
		document = bsonDocument(b.t, value)
	}
	request := &weir.MutateRequest{Resource: b.resource, Action: action, Document: document}
	return request
}
func (b mutationBuilder) transform(n int) *weir.MutateRequest {
	b.t.Helper()
	expression := &weir.Document{ContentType: "application/vnd.weir.search-update.v1+json", Data: []byte(fmt.Sprintf(`{"doc":{"n":%d}}`, n))}
	if b.backend == "mongo" {
		fields := bson.D{{Key: "n", Value: n}}
		update := bson.D{{Key: "$set", Value: fields}}
		expression = bsonDocument(b.t, update)
		expression.ContentType = "application/vnd.weir.mongodb-update.v1+bson"
	}
	request := &weir.MutateRequest{Resource: b.resource, Action: weir.MutationAtomicTransform, BackendExpression: expression}
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
