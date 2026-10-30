package weir

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// recordPeer presents a packed Execute stream to a test peer one request and
// one response at a time.
type recordPeer struct {
	grpc.BidiStreamingServer[pb.ExecuteRequests, pb.ExecuteResponses]
	pending []*pb.ExecuteRequest
}

func (s *recordPeer) Recv() (*pb.ExecuteRequest, error) {
	for len(s.pending) == 0 {
		message, err := s.BidiStreamingServer.Recv()
		if err != nil {
			return nil, err
		}
		s.pending = message.Requests
	}
	request := s.pending[0]
	s.pending = s.pending[1:]
	return request, nil
}

func (s *recordPeer) Send(response *pb.ExecuteResponse) error {
	message := &pb.ExecuteResponses{Responses: []*pb.ExecuteResponse{response}}
	return s.BidiStreamingServer.Send(message)
}
