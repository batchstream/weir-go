package weir

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
)

// HTTPNativeResponse describes an application/http metadata document. Its body
// belongs to the Native chunk consumer and is never retained by this helper.
type HTTPNativeResponse struct {
	StatusCode int
	Headers    http.Header
}

// NewHTTPNativeRequest serializes a standard HTTP/1.1 request using an origin-form
// request target. It consumes and closes request.Body. The owning adapter decides
// which methods, paths and headers it supports; Host does not select a Store.
func NewHTTPNativeRequest(resource string, request *http.Request) (*NativeRequest, error) {
	if request == nil || request.URL == nil || request.URL.Opaque != "" || !strings.HasPrefix(request.URL.RequestURI(), "/") {
		return nil, errors.New("Native HTTP requires an origin-form request target")
	}
	if _, err := protocol.ParseRelativeResource(resource); err != nil {
		return nil, err
	}
	if len(request.Trailer) != 0 || request.Header.Get("Trailer") != "" {
		return nil, errors.New("Native HTTP requires a complete body without trailers")
	}
	copy := request.Clone(request.Context())
	copy.Proto, copy.ProtoMajor, copy.ProtoMinor = "HTTP/1.1", 1, 1
	copy.RequestURI = ""
	copy.TransferEncoding = nil
	copy.Close = false
	if copy.Header == nil {
		copy.Header = make(http.Header)
	}
	if _, supplied := copy.Header["User-Agent"]; !supplied {
		copy.Header["User-Agent"] = []string{""}
	}
	if copy.Body != nil && copy.Body != http.NoBody {
		data, err := io.ReadAll(io.LimitReader(copy.Body, protocol.MaxNativeRequestBytes+1))
		closeErr := copy.Body.Close()
		if err != nil || closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		if len(data) > protocol.MaxNativeRequestBytes {
			return nil, errors.New("Native HTTP request exceeds its byte bound")
		}
		copy.Body = http.NoBody
		if len(data) != 0 {
			copy.Body = io.NopCloser(bytes.NewReader(data))
		}
		copy.ContentLength = int64(len(data))
		copy.GetBody = nil
	}
	if copy.Host == "" && copy.URL.Host == "" {
		copy.Host = "weir.invalid"
	}
	writer := nativeHTTPWriter{}
	if err := copy.Write(&writer); err != nil {
		return nil, err
	}
	document := &Document{ContentType: "application/http", Data: writer.data.Bytes()}
	native := &NativeRequest{Resource: resource, Request: document}
	return native, nil
}

// ParseHTTPNativeResponse parses a complete HTTP status line and header block
// from Native response metadata. No body bytes may follow the terminating CRLF.
// Other native formats stay opaque and do not require this optional helper.
func ParseHTTPNativeResponse(response *NativeResponse) (*HTTPNativeResponse, error) {
	if response == nil || response.Metadata == nil || response.Metadata.ContentType != "application/http" || len(response.Metadata.Data) > protocol.MaxNativeMetadataBytes {
		return nil, errors.New("Native response requires bounded application/http metadata")
	}
	data := response.Metadata.Data
	if bytes.Index(data, []byte("\r\n\r\n")) != len(data)-4 || len(data) < 4 {
		return nil, errors.New("Native HTTP metadata requires a complete header block")
	}
	lineStart := 0
	for index, value := range data {
		if value == '\r' && (index+1 == len(data) || data[index+1] != '\n') || value == '\n' && (index == 0 || data[index-1] != '\r') {
			return nil, errors.New("Native HTTP metadata requires CRLF line endings")
		}
		if index > 0 && data[index-1] == '\n' && (value == ' ' || value == '\t') {
			return nil, errors.New("Native HTTP metadata contains a folded header")
		}
		if value == '\n' {
			line := data[lineStart : index-1]
			if lineStart != 0 {
				if colon := bytes.IndexByte(line, ':'); colon >= 0 {
					name := line[:colon]
					if bytes.EqualFold(name, []byte("Transfer-Encoding")) || bytes.EqualFold(name, []byte("Trailer")) {
						return nil, errors.New("Native HTTP metadata cannot frame the separately streamed body")
					}
				}
			}
			lineStart = index + 1
		}
	}
	input := bytes.NewReader(data)
	reader := bufio.NewReader(input)
	parsed, err := http.ReadResponse(reader, nil)
	if err != nil {
		return nil, err
	}
	if parsed.StatusCode < 100 || parsed.StatusCode > 599 {
		return nil, errors.New("Native HTTP metadata has an invalid response status")
	}
	if reader.Buffered()+input.Len() != 0 {
		return nil, errors.New("Native HTTP metadata contains body or trailing bytes")
	}
	result := &HTTPNativeResponse{StatusCode: parsed.StatusCode, Headers: parsed.Header}
	return result, nil
}

type nativeHTTPWriter struct {
	data        bytes.Buffer
	headerBytes int
	headerDone  bool
	separator   int
}

func (w *nativeHTTPWriter) Write(data []byte) (int, error) {
	if len(data) > protocol.MaxNativeRequestBytes-w.data.Len() {
		return 0, errors.New("Native HTTP request exceeds its byte bound")
	}
	if !w.headerDone {
		const end = "\r\n\r\n"
		for _, value := range data {
			w.headerBytes++
			if w.headerBytes > protocol.MaxNativeMetadataBytes {
				return 0, errors.New("Native HTTP request headers exceed their byte bound")
			}
			if value == end[w.separator] {
				w.separator++
			} else if value == '\r' {
				w.separator = 1
			} else {
				w.separator = 0
			}
			if w.separator == len(end) {
				w.headerDone = true
				break
			}
		}
	}
	return w.data.Write(data)
}
