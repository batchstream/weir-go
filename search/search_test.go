package search

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/internal/storetest"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

type product struct {
	Name  string `json:"name"`
	Price int    `json:"price"`
}

// recorder answers commands with a handler and keeps every command it saw.
type recorder struct {
	t        *testing.T
	commands []*pb.Command
	answer   func(command *pb.Command) []*pb.Event
}

func (r *recorder) cluster() Cluster {
	handler := func(_ string, command *pb.Command) []*pb.Event {
		r.commands = append(r.commands, command)
		return r.answer(command)
	}
	client := storetest.Client(r.t, handler)
	cluster := Cluster{Client: client, Store: "search"}
	return cluster
}

func (r *recorder) products() Index[product] {
	products := Index[product]{Cluster: r.cluster(), Name: "products"}
	return products
}

// httpResponse is the events of a complete HTTP response.
func httpResponse(status string, body string) []*pb.Event {
	head := "HTTP/1.1 " + status + "\r\nContent-Type: application/json\r\n\r\n"
	metadata := &pb.Document{ContentType: "application/http", Data: []byte(head)}
	return storetest.Native(metadata, []byte(body))
}

// sentHTTP parses the HTTP request of a Native command.
func sentHTTP(t *testing.T, command *pb.Command) (*http.Request, string) {
	t.Helper()
	native := command.GetNative()
	reader := bufio.NewReader(bytes.NewReader(native.GetRequest().GetData()))
	request, err := http.ReadRequest(reader)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return request, string(body)
}

func TestDoSendsAnHTTPRequestAndReturnsAnyStatus(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return httpResponse("404 Not Found", `{"found":false}`) }}
	products := r.products()
	query := map[string]any{"size": 0}
	response, err := products.Do(t.Context(), http.MethodPost, "/_search?timeout=1s", query)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 404 || string(response.Body) != `{"found":false}` || response.Header.Get("Content-Type") != "application/json" {
		t.Fatal("wrong response", response)
	}
	request, body := sentHTTP(t, r.commands[0])
	if r.commands[0].GetNative().GetResource() != "products" || request.Method != http.MethodPost || request.RequestURI != "/_search?timeout=1s" {
		t.Fatal("wrong request", r.commands[0].GetNative().GetResource(), request.Method, request.RequestURI)
	}
	if body != `{"size":0}` || request.Header.Get("Content-Type") != "application/json" {
		t.Fatal("wrong body", body, request.Header)
	}

	raw := []byte("{\"index\":{}}\n{\"name\":\"a\"}\n")
	_, err = products.Cluster.Do(t.Context(), http.MethodPost, "/_bulk", raw)
	if err != nil {
		t.Fatal(err)
	}
	request, body = sentHTTP(t, r.commands[1])
	if r.commands[1].GetNative().GetResource() != "" || request.RequestURI != "/_bulk" || body != string(raw) {
		t.Fatal("raw body or root path changed", request.RequestURI, body)
	}
	_, err = products.Cluster.Do(t.Context(), http.MethodGet, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	request, body = sentHTTP(t, r.commands[2])
	if request.RequestURI != "/" || body != "" || request.Header.Get("Content-Type") != "" {
		t.Fatal("wrong root request", request.RequestURI, body)
	}
}

func TestDoInterruptedByAnUnavailableBackend(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.NativeIncomplete(nil, nil, pb.FailureCode_UNAVAILABLE) }}
	_, err := r.cluster().Do(t.Context(), http.MethodGet, "/", nil)
	if !errors.Is(err, weir.ErrInterrupted) {
		t.Fatal("unavailable backend did not interrupt", err)
	}
}

func TestRecordsUseJSONAndStringIDs(t *testing.T) {
	stored := `{"name":"lamp","price":30}`
	r := &recorder{t: t}
	r.answer = func(command *pb.Command) []*pb.Event {
		if read := command.GetRead(); read != nil {
			if read.GetResource() == "products/s:p%2F1" {
				document := &pb.Document{ContentType: jsonContentType, Data: []byte(stored)}
				return storetest.Read(document)
			}
			return storetest.Missing()
		}
		return storetest.Applied()
	}
	products := r.products()
	lamp := &product{Name: "lamp", Price: 30}
	if err := products.Put(t.Context(), "p/1", lamp); err != nil {
		t.Fatal(err)
	}
	put := r.commands[0].GetMutate()
	if put.GetResource() != "products/s:p%2F1" || string(put.GetPut().GetData()) != stored || put.GetPut().GetContentType() != jsonContentType {
		t.Fatal("wrong put", put)
	}
	got, err := products.Get(t.Context(), "p/1")
	if err != nil || *got != *lamp {
		t.Fatal("wrong document", got, err)
	}
	_, err = products.Get(t.Context(), "p/2")
	if !errors.Is(err, weir.ErrNotFound) {
		t.Fatal("missing document not reported", err)
	}
	many, err := products.GetMany(t.Context(), "p/2", "p/1")
	if err != nil || many[0] != nil || *many[1] != *lamp {
		t.Fatal("wrong documents", many, err)
	}

	products.QueryVisible = true
	price := map[string]int{"price": 25}
	if err := products.Update(t.Context(), "p/1", price); err != nil {
		t.Fatal(err)
	}
	update := r.commands[len(r.commands)-1].GetMutate()
	expression := update.GetAtomicTransform().GetBackendExpression()
	if string(expression.GetData()) != `{"doc":{"price":25}}` || expression.GetContentType() != updateContentType {
		t.Fatal("wrong update", expression)
	}
	if update.GetOptions().GetVisibility() != pb.MutationVisibility_QUERY_VISIBLE {
		t.Fatal("update did not wait for visibility")
	}
	const program = "return function(current, incoming) return incoming end"
	err = products.Transform(t.Context(), "p/1", program, lamp)
	if err != nil {
		t.Fatal(err)
	}
	lua := r.commands[len(r.commands)-1].GetMutate().GetAtomicTransform().GetLua()
	if string(lua.GetSource()) != program || string(lua.GetInput().GetData()) != stored {
		t.Fatal("wrong transform", lua)
	}
}

func TestFailedWritesReportTheirCode(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event {
		return storetest.Mutation(pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
	}}
	products := r.products()
	lamp := &product{Name: "lamp"}
	err := products.Create(t.Context(), "p1", lamp)
	if weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("wrong error", err)
	}
	if err := products.Replace(t.Context(), "p1", nil); err == nil {
		t.Fatal("nil document accepted")
	}
}

func TestSearchDecodesHitsAndKeepsSortValuesExact(t *testing.T) {
	hits := `{"hits":{"total":{"value":2},"hits":[
		{"_id":"p1","_score":null,"sort":[9007199254740993,"lamp"],"_source":{"name":"lamp","price":30}},
		{"_id":"p2","_score":1.5,"_source":{"name":"desk","price":90}}]}}`
	status := "200 OK"
	r := &recorder{t: t}
	r.answer = func(*pb.Command) []*pb.Event { return httpResponse(status, hits) }
	products := r.products()
	query := map[string]any{"query": map[string]any{"match_all": map[string]any{}}}
	found, err := products.Search(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 || found[0].ID != "p1" || found[0].Document.Name != "lamp" || found[1].Score != 1.5 {
		t.Fatal("wrong hits", found)
	}
	if string(found[0].Sort[0]) != "9007199254740993" {
		t.Fatal("sort value lost precision", string(found[0].Sort[0]))
	}
	request, body := sentHTTP(t, r.commands[0])
	if request.RequestURI != "/_search" || !json.Valid([]byte(body)) {
		t.Fatal("wrong search request", request.RequestURI, body)
	}
	status = "400 Bad Request"
	hits = `{"error":"bad query"}`
	_, err = products.Search(t.Context(), query)
	var failed *StatusError
	if !errors.As(err, &failed) || failed.StatusCode != 400 || string(failed.Body) != hits {
		t.Fatal("bad status not reported", err)
	}
}
