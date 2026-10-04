package weir

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCachedDiscoveryFailurePreservesClassificationAndCause(t *testing.T) {
	causes := []error{
		status.Error(codes.FailedPrecondition, "conflicting owners"),
		status.Error(codes.NotFound, "requested Store not found"),
		status.Error(codes.Unavailable, "discovery unavailable"),
		context.Canceled,
		context.DeadlineExceeded,
	}
	for _, cause := range causes {
		entry := &storeChannel{expires: time.Now().Add(-time.Second), err: cause}
		client := &Client{stores: map[string]*storeChannel{"records": entry}}
		request := &ReadRequest{Resource: "collection/id"}
		options := ReadOneOptions{StoreName: "records", Request: request}
		result, err := client.ReadOne(t.Context(), options)
		if result != nil || !errors.Is(err, cause) || status.Code(err) != status.Code(cause) {
			t.Fatal("cached discovery cause or classification lost", result, err, cause)
		}
	}
	entry := &storeChannel{expires: time.Now().Add(-time.Second)}
	client := &Client{stores: map[string]*storeChannel{"records": entry}}
	if _, err := client.storeClient("records"); status.Code(err) != codes.Unavailable {
		t.Fatal("expired lease without a cause must be unavailable", err)
	}
}

func TestScanRejectsInvalidProjectionBeforeRPC(t *testing.T) {
	for _, projection := range []*Projection{
		{},
		{Mode: ProjectionInclude, Fields: []string{"name", "name"}},
		{Mode: ProjectionExclude, Fields: []string{"name", "name.first"}},
	} {
		peer := &clientTestPeer{}
		client := clientTestConnection(t, peer)
		request := &ScanRequest{Resource: "records", Projection: projection}
		options := ScanOptions{StoreName: "records", Request: request}
		options.Consume = func(context.Context, *Document) error { return nil }
		if result, err := Scan(t.Context(), client, options); result != nil || err == nil {
			t.Fatal("invalid projection was not rejected", result, err)
		}
		if peer.streams.Load() != 0 {
			t.Fatal("invalid projection opened an RPC")
		}
	}
}

func TestNativeMongoResponseMatchesRequestedBackend(t *testing.T) {
	for _, mode := range []string{"native_normal", "native_wrong_metadata"} {
		peer := &clientTestPeer{mode: mode}
		client := clientTestConnection(t, peer)
		request := &NativeRequest{Resource: "database", MongoDBCommand: []byte{1}}
		options := NativeOptions{StoreName: "records", Request: request}
		consumed := false
		options.Consume = func(_ context.Context, response *NativeResponse, _ []byte) error {
			if response.Http != nil {
				t.Fatal("HTTP metadata exposed as a MongoDB response")
			}
			consumed = true
			return nil
		}
		result, err := Native(t.Context(), client, options)
		if mode == "native_normal" {
			if err != nil || result == nil || result.Completion != NativeResponseComplete || !consumed || result.Response.Http != nil {
				t.Fatal("valid MongoDB response rejected", result, err)
			}
		} else if err == nil || result != nil || consumed {
			t.Fatal("mismatched backend metadata accepted", result, err)
		}
	}
}
