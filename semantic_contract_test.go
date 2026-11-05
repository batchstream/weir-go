package weir

import (
	"context"
	"testing"
)

func TestRequestExtensionsRejectBeforeRPC(t *testing.T) {
	peer := &clientTestPeer{}
	client := clientTestClient(t, peer)
	read := clientTestReadRequest()
	addResponseFields(read)
	if result, err := readOne(t.Context(), client, read); result != nil || err == nil {
		t.Fatal("unknown request option was silently ignored", result, err)
	}
	document := &Document{ContentType: "application/json", Data: []byte(`{}`)}
	addResponseFields(document)
	write := &MutateRequest{Resource: "records/s:key", Action: MutationPut, Document: document}
	if result, err := mutateOne(t.Context(), client, write); result != nil || err == nil {
		t.Fatal("unknown document option was silently ignored", result, err)
	}
	if peer.streams.Load() != 0 {
		t.Fatal("unsupported request extensions opened an RPC")
	}
}

func TestNativeAcceptsNewStoreFormatsWithoutSchemaSelection(t *testing.T) {
	for _, mode := range []string{"native_normal", "native_no_metadata"} {
		peer := &clientTestPeer{mode: mode}
		client := clientTestClient(t, peer)
		document := &Document{ContentType: "application/vnd.future-store.command"}
		request := &NativeRequest{Resource: "anything", Request: document}
		consumed := false
		chunks := func(_ context.Context, response *NativeResponse, _ []byte) error {
			if mode == "native_normal" && response.Metadata.ContentType != "application/vnd.example.response" {
				t.Error("unknown metadata format rejected", response)
			}
			consumed = true
			return nil
		}
		result, err := client.Native(t.Context(), "future-store", request, WithChunks(chunks))
		if err != nil || result == nil || result.Completion != NativeResponseComplete || !consumed || peer.received.Load() != 1 {
			t.Fatal("new Store format needs no SDK or protocol backend selection", result, err)
		}
	}
}
