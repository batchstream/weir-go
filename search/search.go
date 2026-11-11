// Package search reads and writes Elasticsearch and OpenSearch documents
// through a Weir Store whose backend is search.
//
// An Index reads and writes documents by ID and runs searches. A Cluster
// sends any other HTTP request:
//
//	cluster := search.Cluster{Client: client, Store: "search"}
//	products := search.Index[Product]{Cluster: cluster, Name: "products"}
//
//	lamp := &Product{Name: "Lamp"}
//	err := products.Put(ctx, "p1", lamp)
//	product, err := products.Get(ctx, "p1")
//
//	match := map[string]any{"match": map[string]any{"name": "lamp"}}
//	query := map[string]any{"query": match, "size": 10}
//	hits, err := products.Search(ctx, query)
//
//	health, err := cluster.Do(ctx, http.MethodGet, "/_cluster/health", nil)
//
// Set a deadline on every context. A failed record is a *weir.Error: check it
// with weir.CodeOf, and with errors.Is against weir.ErrNotFound and
// weir.ErrInterrupted. The package keeps no state of its own; Cluster and
// Index are plain values that are safe to copy and share.
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/internal/record"
)

const (
	jsonContentType   = "application/json"
	updateContentType = "application/vnd.weir.search-update.v1+json"
)

// Cluster is an Elasticsearch or OpenSearch cluster behind a Weir Store.
type Cluster struct {
	Client *weir.Client
	// Store names a Weir Store whose backend is search.
	Store string
}

// Response is a complete HTTP response.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// StatusError is a response whose status is 300 or more, from a method that
// expects success.
type StatusError struct {
	StatusCode int
	Body       []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("search: status %d: %s", e.StatusCode, e.Body)
}

// Do sends one HTTP request to the cluster and returns its response, whatever
// its status. The path starts at the cluster root, such as /_cluster/health,
// and may carry a query string. A []byte or json.RawMessage body is sent as it
// is, any other non-nil body as JSON. A request is sent again only when a
// server refused it before it began. One a lost connection or an unavailable
// backend cut short reports weir.ErrInterrupted and may have run; send it
// again only when that is safe, as for a read.
func (c Cluster) Do(ctx context.Context, method, path string, body any) (*Response, error) {
	request := httpRequest{method: method, path: path, body: body}
	return c.send(ctx, request)
}

// httpRequest is one request of Do, relative to resource.
type httpRequest struct {
	resource string
	method   string
	path     string
	body     any
}

func (c Cluster) send(ctx context.Context, request httpRequest) (*Response, error) {
	data, err := encodeBody(request.body)
	if err != nil {
		return nil, fmt.Errorf("search: encode body: %w", err)
	}
	path := request.path
	if path == "" {
		path = "/"
	}
	var body io.Reader
	if data != nil {
		body = bytes.NewReader(data)
	}
	message, err := http.NewRequestWithContext(ctx, request.method, path, body)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	if data != nil {
		message.Header.Set("Content-Type", jsonContentType)
	}
	native, err := weir.NewHTTPNativeRequest(request.resource, message)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	result, err := c.target().Native(ctx, native)
	if err != nil {
		return nil, fmt.Errorf("search: %s %s: %w", request.method, path, err)
	}
	head, err := weir.ParseHTTPNativeResponse(result.Response)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	response := &Response{StatusCode: head.StatusCode, Header: head.Headers, Body: result.Body}
	return response, nil
}

func (c Cluster) target() record.Target {
	target := record.Target{Client: c.Client, Store: c.Store}
	return target
}

// encodeBody returns the bytes of a request body, nil for none.
func encodeBody(body any) ([]byte, error) {
	switch value := body.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case json.RawMessage:
		return value, nil
	}
	return json.Marshal(body)
}
