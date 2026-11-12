package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	weir "github.com/batchstream/weir-go"
)

// Index holds documents of type T, encoded as JSON, by string ID. Name is an
// index or a single-index alias, which must exist before the first write.
type Index[T any] struct {
	Cluster Cluster
	Name    string
	// QueryVisible makes each write return only once later searches see it.
	QueryVisible bool
}

// Hit is one document a search matched.
type Hit[T any] struct {
	ID       string            `json:"_id"`
	Score    float64           `json:"_score"`
	Sort     []json.RawMessage `json:"sort"`
	Document *T                `json:"_source"`
}

// Get returns the document with the ID, or weir.ErrNotFound.
func (i Index[T]) Get(ctx context.Context, id string) (*T, error) {
	documents, err := i.GetMany(ctx, id)
	if err != nil {
		return nil, err
	}
	if documents[0] == nil {
		return nil, fmt.Errorf("search: %s %s: %w", i.Name, id, weir.ErrNotFound)
	}
	return documents[0], nil
}

// GetMany returns the documents with the IDs in one call, in order, with nil
// for each missing one.
func (i Index[T]) GetMany(ctx context.Context, ids ...string) ([]*T, error) {
	resources := make([]string, len(ids))
	for n, id := range ids {
		resources[n] = i.resource(id)
	}
	documents, err := i.Cluster.target().Read(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	values := make([]*T, len(documents))
	for n, document := range documents {
		if document == nil {
			continue
		}
		value := new(T)
		if err := json.Unmarshal(document.GetData(), value); err != nil {
			return nil, fmt.Errorf("search: decode %s %s: %w", i.Name, ids[n], err)
		}
		values[n] = value
	}
	return values, nil
}

// Create writes a document that must not exist. An existing one fails it with
// FailurePreconditionFailed.
func (i Index[T]) Create(ctx context.Context, id string, document *T) error {
	return i.write(ctx, weir.MutationCreate, id, document)
}

// Put creates or replaces the document.
func (i Index[T]) Put(ctx context.Context, id string, document *T) error {
	return i.write(ctx, weir.MutationPut, id, document)
}

// Replace replaces a document that must exist. A missing one fails it with
// FailurePreconditionFailed.
func (i Index[T]) Replace(ctx context.Context, id string, document *T) error {
	return i.write(ctx, weir.MutationReplace, id, document)
}

// Delete deletes the document. Deleting a missing document succeeds.
func (i Index[T]) Delete(ctx context.Context, id string) error {
	request := &weir.MutateRequest{Resource: i.resource(id), Action: weir.MutationDelete}
	return i.mutate(ctx, request)
}

// Update merges fields, a JSON object, into a document that must exist. A
// []byte or json.RawMessage holds the object's JSON as it is; any other value
// is encoded as JSON. A missing document fails it with
// FailurePreconditionFailed.
func (i Index[T]) Update(ctx context.Context, id string, fields any) error {
	if raw, ok := fields.([]byte); ok {
		fields = json.RawMessage(raw)
	}
	expression := struct {
		Doc any `json:"doc"`
	}{Doc: fields}
	data, err := json.Marshal(expression)
	if err != nil {
		return fmt.Errorf("search: encode update: %w", err)
	}
	document := &weir.Document{ContentType: updateContentType, Data: data}
	request := &weir.MutateRequest{Resource: i.resource(id), Action: weir.MutationAtomicTransform, BackendExpression: document}
	return i.mutate(ctx, request)
}

// Transform runs a Lua program atomically against the document. The program
// returns one function, called as function(current, incoming), where current
// is the stored document or nil and incoming is input, a []byte or
// json.RawMessage holding JSON or another value encoded as JSON, or nil when
// input is nil. Its result replaces the document, and weir.reject(message)
// fails it with FailurePreconditionFailed. See the Lua guide for the rest.
func (i Index[T]) Transform(ctx context.Context, id string, program string, input any) error {
	lua := &weir.LuaTransform{Source: []byte(program)}
	if input != nil {
		data, err := encodeBody(input)
		if err != nil {
			return fmt.Errorf("search: encode transform input: %w", err)
		}
		lua.Input = &weir.Document{ContentType: jsonContentType, Data: data}
	}
	request := &weir.MutateRequest{Resource: i.resource(id), Action: weir.MutationAtomicTransform, Lua: lua}
	return i.mutate(ctx, request)
}

// Search sends a search request body, such as {"query": ..., "size": 10}, to
// the index and returns its hits. A status of 300 or more is a *StatusError.
// Sort holds a hit's sort values exactly, to pass back as search_after.
func (i Index[T]) Search(ctx context.Context, body any) ([]Hit[T], error) {
	response, err := i.Do(ctx, http.MethodPost, "/_search", body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		failed := &StatusError{StatusCode: response.StatusCode, Body: response.Body}
		return nil, failed
	}
	var result struct {
		Hits struct {
			Hits []Hit[T] `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(response.Body, &result); err != nil {
		return nil, fmt.Errorf("search: decode %s hits: %w", i.Name, err)
	}
	return result.Hits.Hits, nil
}

// Do sends one HTTP request to the index and returns its response, whatever
// its status. The path starts at the index, such as /_search or /_refresh, and
// may carry a query string. Bodies and errors are as for Cluster.Do.
func (i Index[T]) Do(ctx context.Context, method, path string, body any) (*Response, error) {
	if i.Name == "" {
		return nil, errors.New("search: Index has no Name")
	}
	request := httpRequest{resource: weir.EncodeSegment(i.Name), method: method, path: path, body: body}
	return i.Cluster.send(ctx, request)
}

func (i Index[T]) write(ctx context.Context, action weir.MutationAction, id string, document *T) error {
	if document == nil {
		return errors.New("search: document is nil")
	}
	data, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("search: encode %s %s: %w", i.Name, id, err)
	}
	encoded := &weir.Document{ContentType: jsonContentType, Data: data}
	request := &weir.MutateRequest{Resource: i.resource(id), Action: action, Document: encoded}
	return i.mutate(ctx, request)
}

func (i Index[T]) mutate(ctx context.Context, request *weir.MutateRequest) error {
	err := i.Cluster.target().Mutate(ctx, request, i.QueryVisible)
	if err != nil {
		return fmt.Errorf("search: %s: %w", request.Resource, err)
	}
	return nil
}

// resource addresses one document: index/s:id.
func (i Index[T]) resource(id string) string {
	resource := weir.EncodeSegment(i.Name) + "/" + weir.EncodeSegment("s:"+id)
	return resource
}
