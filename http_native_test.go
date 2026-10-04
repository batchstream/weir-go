package weir

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
)

func TestHTTPNativeRequestUsesBoundedOriginFormAndExactLength(t *testing.T) {
	for _, incoming := range []bool{false, true} {
		t.Run(strconv.FormatBool(incoming), func(t *testing.T) {
			body := io.NopCloser(strings.NewReader(`{"n":1}`))
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/_doc/id?refresh=true", body)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Custom", "adapter decides")
			if incoming {
				request.TransferEncoding = []string{"chunked"}
				request.Close = true
			}
			native, err := NewHTTPNativeRequest("entries", request)
			if err != nil {
				t.Fatal(err)
			}
			if native.Request.ContentType != "application/http" || native.Resource != "entries" {
				t.Fatal("wrong opaque request envelope", native)
			}
			reader := bufio.NewReader(bytes.NewReader(native.Request.Data))
			parsed, err := http.ReadRequest(reader)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(parsed.Body)
			parsed.Body.Close()
			if err != nil || string(data) != `{"n":1}` || parsed.RequestURI != "/_doc/id?refresh=true" || parsed.Proto != "HTTP/1.1" || parsed.ContentLength != int64(len(data)) || len(parsed.TransferEncoding) != 0 || parsed.Close || parsed.UserAgent() != "" || parsed.Header.Get("X-Custom") != "adapter decides" {
				t.Fatal("HTTP helper changed payload/options or added framing", parsed, string(data), err)
			}
			if request.Header.Get("User-Agent") != "" || incoming && (len(request.TransferEncoding) != 1 || !request.Close) {
				t.Fatal("HTTP helper mutated caller metadata", request)
			}
		})
	}
}

func TestHTTPNativeRequestPreservesExplicitUserAgent(t *testing.T) {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://ignored.example/_doc/id", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("User-Agent", "application supplied")
	native, err := NewHTTPNativeRequest("entries", request)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(bytes.NewReader(native.Request.Data))
	parsed, err := http.ReadRequest(reader)
	if err != nil || parsed.UserAgent() != "application supplied" || parsed.RequestURI != "/_doc/id" {
		t.Fatal("explicit application options changed", parsed, err)
	}
}

type boundedHTTPBody struct {
	read   int
	closed bool
}

func (b *boundedHTTPBody) Read(data []byte) (int, error) {
	b.read += len(data)
	clear(data)
	return len(data), nil
}

func (b *boundedHTTPBody) Close() error {
	b.closed = true
	return nil
}

func TestHTTPNativeRequestStopsOversizedBodiesAndHeaders(t *testing.T) {
	body := &boundedHTTPBody{}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/query", body)
	if err != nil {
		t.Fatal(err)
	}
	if native, err := NewHTTPNativeRequest("entries", request); err == nil || native != nil || !body.closed || body.read != protocol.MaxNativeRequestBytes+1 {
		t.Fatal("unbounded input was not stopped and closed", native, err, body)
	}
	request, err = http.NewRequestWithContext(t.Context(), http.MethodGet, "/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Large", strings.Repeat("x", protocol.MaxNativeMetadataBytes))
	if native, err := NewHTTPNativeRequest("entries", request); err == nil || native != nil {
		t.Fatal("oversized HTTP header block accepted", native, err)
	}
}

func TestHTTPNativeRequestRejectsUnrepresentableTrailers(t *testing.T) {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Trailer = http.Header{"X-End": []string{"trailer"}}
	if native, err := NewHTTPNativeRequest("entries", request); err == nil || native != nil {
		t.Fatal("trailers cannot be discarded while serializing a complete body", native, err)
	}
}

func TestHTTPNativeResponseParsesHeaderOnlyMetadata(t *testing.T) {
	metadata := &Document{ContentType: "application/http", Data: []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 100\r\nContent-Type: application/json\r\nX-Example: one\r\nX-Example: two\r\n\r\n")}
	response := &NativeResponse{Metadata: metadata, BodyContentType: "application/json"}
	parsed, err := ParseHTTPNativeResponse(response)
	if err != nil || parsed.StatusCode != 404 || parsed.Headers.Get("Content-Length") != "100" || len(parsed.Headers.Values("X-Example")) != 2 {
		t.Fatal("body-free HTTP metadata lost status or headers", parsed, err)
	}
}

func TestHTTPNativeResponseRejectsMalformedMetadataFraming(t *testing.T) {
	for _, data := range []string{
		"",
		"HTTP/1.1 200 OK\r\n",
		"HTTP/1.1 200 OK\nX-Example: value\n\r\n\r\n",
		"HTTP/1.1 200 OK\r\nX-Example: value\r\n continued\r\n\r\n",
		"HTTP/1.1 200 OK\r\n\r\nbody\r\n\r\n",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
		"HTTP/1.0 200 OK\r\ntransfer-encoding: chunked\r\n\r\n",
		"HTTP/1.1 200 OK\r\nTrailer: X-End\r\n\r\n",
		"HTTP/1.1 000 Invalid\r\n\r\n",
		"HTTP/1.1 099 Invalid\r\n\r\n",
		"HTTP/1.1 600 Invalid\r\n\r\n",
		"HTTP/1.1 999 Invalid\r\n\r\n",
		"HTTP/1.1 200 OK\r\nX-Large: " + strings.Repeat("x", protocol.MaxNativeMetadataBytes) + "\r\n\r\n",
	} {
		metadata := &Document{ContentType: "application/http", Data: []byte(data)}
		response := &NativeResponse{Metadata: metadata}
		if parsed, err := ParseHTTPNativeResponse(response); err == nil || parsed != nil {
			t.Fatal("invalid HTTP metadata accepted", parsed, err)
		}
	}
	metadata := &Document{ContentType: "application/vnd.future-store.response", Data: []byte("HTTP/1.1 200 OK\r\n\r\n")}
	response := &NativeResponse{Metadata: metadata}
	if parsed, err := ParseHTTPNativeResponse(response); err == nil || parsed != nil {
		t.Fatal("HTTP convenience interpreted another format", parsed, err)
	}
}
