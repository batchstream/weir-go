package weir

import (
	"errors"

	"github.com/batchstream/weir-protocol/api/protocol"
	spb "github.com/batchstream/weir-protocol/api/weir/search/v1"
	"google.golang.org/protobuf/proto"
)

type SearchHTTPRequest = spb.Request
type SearchHTTPHeader = spb.Header
type SearchHTTPResponse = spb.Response

const SearchHTTPContentType = "application/vnd.weir.search-http.v1+protobuf"
const MongoCommandContentType = "application/vnd.weir.mongodb-command.v1+protobuf"

// SearchHTTPDescriptor encodes the bounded HTTP metadata for a Native request.
// The request body remains NativeRequest.Body and is not buffered here.
func SearchHTTPDescriptor(request *SearchHTTPRequest) (*Document, error) {
	if request == nil || proto.Size(request) > protocol.NativeDescriptor {
		return nil, errors.New("Search HTTP descriptor requires a bounded request")
	}
	data, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	document := &Document{ContentType: SearchHTTPContentType, Data: data}
	return document, nil
}

// DecodeSearchHTTPResponse reads NativeHead.Metadata. HTTP status describes the
// backend response; it does not establish mutation success or RPC completion.
func DecodeSearchHTTPResponse(metadata *Document) (*SearchHTTPResponse, error) {
	if metadata == nil || metadata.ContentType != SearchHTTPContentType || len(metadata.Data) > protocol.NativeDescriptor {
		return nil, errors.New("invalid Search HTTP response metadata")
	}
	response := &SearchHTTPResponse{}
	if err := proto.Unmarshal(metadata.Data, response); err != nil {
		return nil, err
	}
	return response, nil
}
