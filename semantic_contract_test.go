package weir

import (
	"context"
	"testing"
)

func TestRequestExtensionsRejectBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestConnection(t, peer)
	read := clientTestReadRequest()
	addResponseFields(read)
	readOptions := ReadOneOptions{StoreName: "records", Request: read}
	if result, err := ReadOne(t.Context(), client, readOptions); result != nil || err == nil {
		t.Fatal("unknown request option was silently ignored", result, err)
	}
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	addResponseFields(document)
	write := &WriteRequest{Resource: "records/s:key", Document: document}
	writeOptions := WriteOptions{StoreName: "records", Request: write}
	if result, err := Put(t.Context(), client, writeOptions); result != nil || err == nil {
		t.Fatal("unknown document option was silently ignored", result, err)
	}
	if peer.streams.Load() != 0 {
		t.Fatal("unsupported request extensions opened an RPC")
	}
}

func TestNativeAcceptsNewStoreFormatsWithoutSchemaSelection(t *testing.T) {
	for _, mode := range []string{"native_normal", "native_no_metadata"} {
		peer := &clientTestPeer{mode: mode}
		client := clientTestConnection(t, peer)
		document := &Document{ContentType: "application/vnd.future-store.command"}
		request := &NativeRequest{Resource: "anything", Request: document}
		options := NativeOptions{StoreName: "future-store", Request: request}
		consumed := false
		options.Consume = func(_ context.Context, response *NativeResponse, _ []byte) error {
			if mode == "native_normal" && response.Metadata.ContentType != "application/vnd.example.response" {
				t.Fatal("unknown metadata format rejected", response)
			}
			consumed = true
			return nil
		}
		result, err := Native(t.Context(), client, options)
		if err != nil || result == nil || result.Completion != NativeResponseComplete || !consumed || peer.received.Load() != 1 {
			t.Fatal("new Store format needs no SDK or protocol backend selection", result, err)
		}
	}
}
