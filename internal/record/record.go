// Package record sends the single-record calls of the mongodb and search
// packages.
package record

import (
	"context"
	"errors"
	"fmt"

	weir "github.com/batchstream/weir-go"
)

// Target is one Store of a Client.
type Target struct {
	Client *weir.Client
	Store  string
}

var errNoClient = errors.New("weir: no Client")

// Mutate sends one mutation and returns nil only for APPLIED without a
// Failure. With visible, success also confirms that later queries see it.
func (t Target) Mutate(ctx context.Context, request *weir.MutateRequest, visible bool) error {
	if t.Client == nil {
		return errNoClient
	}
	options := []weir.MutateOption{weir.WithRequests(request)}
	if visible {
		options = append(options, weir.WithQueryVisible())
	}
	results, err := t.Client.Mutate(ctx, t.Store, options...)
	if err != nil {
		return err
	}
	if len(results) != 1 {
		return fmt.Errorf("weir: %d results for one mutation", len(results))
	}
	return weir.MutationErr(results[0])
}

// Read reads resources in one call and returns their documents in order, nil
// for a missing record. A failed read fails the whole call.
func (t Target) Read(ctx context.Context, resources []string) ([]*weir.Document, error) {
	if t.Client == nil {
		return nil, errNoClient
	}
	documents := make([]*weir.Document, len(resources))
	if len(resources) == 0 {
		return documents, nil
	}
	requests := make([]*weir.ReadRequest, len(resources))
	for i, resource := range resources {
		requests[i] = &weir.ReadRequest{Resource: resource}
	}
	results, err := t.Client.Read(ctx, t.Store, weir.WithRequests(requests...))
	if err != nil {
		return nil, err
	}
	if len(results) != len(resources) {
		return nil, fmt.Errorf("weir: %d results for %d reads", len(results), len(resources))
	}
	for i, result := range results {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w", resources[i], err)
		}
		documents[i] = result.Document
	}
	return documents, nil
}

// Native sends one Native request and returns its complete result.
func (t Target) Native(ctx context.Context, request *weir.NativeRequest) (*weir.NativeResult, error) {
	if t.Client == nil {
		return nil, errNoClient
	}
	result, err := t.Client.Native(ctx, t.Store, request)
	if err != nil {
		return nil, err
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
