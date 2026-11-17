package mongodb

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/batchstream/weir-go/internal/storetest"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type orderedMapCommand map[string]any

func (command orderedMapCommand) MarshalBSON() ([]byte, error) {
	ordered := bson.D{{Key: "find", Value: command["find"]}, {Key: "filter", Value: command["filter"]}}
	return bson.Marshal(ordered)
}

type pointerMapCommand map[string]any

func (command *pointerMapCommand) MarshalBSON() ([]byte, error) {
	return orderedMapCommand(*command).MarshalBSON()
}

func TestCommandsHonorOrderedMapMarshalers(t *testing.T) {
	ordered := orderedMapCommand{"find": "users", "filter": bson.M{"active": true}}
	pointer := pointerMapCommand(ordered)
	inputs := []any{ordered, &ordered, &pointer}
	for _, input := range inputs {
		if _, err := bson.Marshal(input); err != nil {
			t.Fatalf("fixture %T is not a supported BSON document: %v", input, err)
		}
		cursors := &cursorServer{t: t, batches: [][]string{{"a"}}, failAt: -1}
		r := &recorder{t: t, answer: cursors.answer}
		db := r.db()
		if _, err := db.Command(t.Context(), input); err != nil {
			t.Fatalf("Command rejected ordered %T: %v", input, err)
		}
		var seen int
		for document, err := range db.Cursor(t.Context(), input) {
			if err != nil {
				t.Fatalf("Cursor rejected ordered %T: %v", input, err)
			}
			if document.Lookup("name").StringValue() != "a" {
				t.Fatal("wrong cursor document", document)
			}
			seen++
		}
		if seen != 1 || len(r.commands) != 2 {
			t.Fatal("wrong custom command dispatch", seen, r.commands)
		}
		for _, sent := range r.commands {
			if got := command(t, sent)[0].Key; got != "find" {
				t.Fatal("custom command order changed", got)
			}
		}
	}
}

func TestCommandsRejectUnorderedMultiKeyMapsBeforeSending(t *testing.T) {
	type namedMap map[string]any
	unordered := bson.M{"find": "users", "filter": bson.M{"active": true}}
	plain := map[string]any{"find": "users", "limit": 1}
	named := namedMap{"find": "users", "limit": 1}
	pointer := &unordered
	inputs := []any{unordered, &unordered, &pointer, plain, &plain, named, &named}
	r := &recorder{t: t}
	r.answer = func(*pb.Command) []*pb.Event {
		reply := bson.D{{Key: "ok", Value: 1}}
		return storetest.Native(nil, bsonDocument(t, reply).Data)
	}
	db := r.db()
	for _, input := range inputs {
		_, err := db.Command(t.Context(), input)
		if err == nil || !strings.Contains(err.Error(), "multi-key map") {
			t.Fatalf("Command accepted unordered %T: %v", input, err)
		}
		var yielded int
		for document, err := range db.Cursor(t.Context(), input) {
			yielded++
			if document != nil || err == nil || !strings.Contains(err.Error(), "multi-key map") {
				t.Fatalf("Cursor accepted unordered %T: %v, %v", input, document, err)
			}
		}
		if yielded != 1 {
			t.Fatalf("Cursor yielded %d results for invalid %T", yielded, input)
		}
	}
	if len(r.commands) != 0 {
		t.Fatal("unordered commands reached the server", r.commands)
	}
}

func TestCommandAcceptsOrderedCommandsAndSingleKeyMaps(t *testing.T) {
	ordered := bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.M{"active": true, "name": "Ada"}}}
	raw := bson.Raw(bsonDocument(t, ordered).Data)
	ping := bson.M{"ping": 1}
	inputs := []any{ordered, &ordered, raw, &raw, ping, &ping}
	r := &recorder{t: t}
	r.answer = func(*pb.Command) []*pb.Event {
		reply := bson.D{{Key: "ok", Value: 1}}
		return storetest.Native(nil, bsonDocument(t, reply).Data)
	}
	db := r.db()
	for index, input := range inputs {
		if _, err := db.Command(t.Context(), input); err != nil {
			t.Fatalf("rejected ordered %T: %v", input, err)
		}
		sent := command(t, r.commands[index])
		want := "find"
		if index >= 4 {
			want = "ping"
		}
		if sent[0].Key != want {
			t.Fatalf("changed command order for %T: %v", input, sent)
		}
	}
}

type marshaledCursorCommand struct {
	value bson.D
	calls int
}

func (c *marshaledCursorCommand) MarshalBSON() ([]byte, error) {
	c.calls++
	return bson.Marshal(c.value)
}

func TestCursorPreservesExplicitSessionAndTransaction(t *testing.T) {
	tests := []struct {
		name        string
		transaction bool
		raw         bool
		marshaler   bool
	}{
		{name: "session"},
		{name: "transaction", transaction: true},
		{name: "raw transaction", transaction: true, raw: true},
		{name: "marshaled transaction", transaction: true, marshaler: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
			session := bson.D{{Key: "id", Value: id}}
			concern := bson.D{{Key: "level", Value: "majority"}}
			find := bson.D{
				{Key: "find", Value: "users"},
				{Key: "lsid", Value: session},
				{Key: "batchSize", Value: 1},
				{Key: "readConcern", Value: concern},
				{Key: "apiVersion", Value: "1"},
				{Key: "apiStrict", Value: true},
				{Key: "apiDeprecationErrors", Value: true},
			}
			keys := []string{"lsid", "apiVersion", "apiStrict", "apiDeprecationErrors"}
			if test.transaction {
				transaction := bson.D{{Key: "txnNumber", Value: int64(7)}, {Key: "autocommit", Value: false}, {Key: "startTransaction", Value: true}}
				find = append(find, transaction...)
				keys = append(keys, "txnNumber", "autocommit")
			}
			var input any = find
			if test.raw {
				input = bson.Raw(bsonDocument(t, find).Data)
			}
			custom := &marshaledCursorCommand{value: find}
			if test.marshaler {
				input = custom
			}
			cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}, {"c"}}, failAt: -1}
			r := &recorder{t: t, answer: cursors.answer}
			db := r.db()
			seen := 0
			for _, err := range db.Cursor(t.Context(), input) {
				if err != nil {
					t.Fatal(err)
				}
				seen++
				if seen == 2 {
					break
				}
			}
			if len(r.commands) != 3 || len(cursors.killed) != 1 {
				t.Fatal("expected find, getMore and one cleanup", r.commands)
			}
			original := bson.Raw(r.commands[0].GetNative().GetRequest().GetData())
			for _, sent := range r.commands[1:] {
				raw := bson.Raw(sent.GetNative().GetRequest().GetData())
				for _, key := range keys {
					want, got := original.Lookup(key), raw.Lookup(key)
					if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
						t.Fatalf("lost %s in continuation: %v", key, raw)
					}
				}
				for _, key := range []string{"startTransaction", "readConcern"} {
					if raw.Lookup(key).Type != 0 {
						t.Fatalf("copied initial-only %s into continuation: %v", key, raw)
					}
				}
				want, got := original.Lookup("batchSize"), raw.Lookup("batchSize")
				switch name := command(t, sent)[0].Key; name {
				case "getMore":
					if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
						t.Fatalf("getMore lost batchSize: %v", raw)
					}
				case "killCursors":
					if got.Type != 0 {
						t.Fatalf("copied batchSize into cleanup: %v", raw)
					}
				default:
					t.Fatal("unexpected continuation", name)
				}
			}
			if got := cursors.killed[0][1].Value.(bson.A)[0]; got != int64(2) {
				t.Fatal("did not clean up the latest cursor", got)
			}
			if test.marshaler && custom.calls != 1 {
				t.Fatal("encoded the original command more than once", custom.calls)
			}
		})
	}
}

func TestGetMoreRepeatsAPositiveBatchSize(t *testing.T) {
	pipeline := bson.E{Key: "pipeline", Value: bson.A{}}
	tests := []struct {
		name    string
		command bson.D
		want    any
	}{
		{
			name:    "find",
			command: bson.D{{Key: "find", Value: "users"}, {Key: "batchSize", Value: int64(2)}},
			want:    int64(2),
		},
		{
			name:    "aggregate",
			command: bson.D{{Key: "aggregate", Value: "users"}, pipeline, {Key: "cursor", Value: bson.D{{Key: "batchSize", Value: int32(3)}}}},
			want:    int32(3),
		},
		{
			name:    "server default",
			command: bson.D{{Key: "aggregate", Value: "users"}, pipeline, {Key: "cursor", Value: bson.D{}}},
		},
		{
			name:    "empty first batch",
			command: bson.D{{Key: "find", Value: "users"}, {Key: "batchSize", Value: int32(0)}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}, {"c"}}, failAt: -1}
			r := &recorder{t: t, answer: cursors.answer}
			for _, err := range r.db().Cursor(t.Context(), test.command) {
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(r.commands) != 3 {
				t.Fatal("expected the opening command and two getMores", r.commands)
			}
			for _, sent := range r.commands[1:] {
				var size any
				for _, field := range command(t, sent) {
					if field.Key == "batchSize" {
						size = field.Value
					}
				}
				if size != test.want {
					t.Fatalf("getMore batchSize = %#v, want %#v", size, test.want)
				}
			}
		})
	}
}

func TestCursorFieldsDoNotRetainTheEncodedCommand(t *testing.T) {
	session := bson.D{{Key: "id", Value: "session"}}
	find := bson.D{{Key: "find", Value: "users"}, {Key: "lsid", Value: session}, {Key: "apiVersion", Value: "1"}, {Key: "batchSize", Value: int32(5)}}
	data := bson.Raw(bsonDocument(t, find).Data)
	fields := readCursorFields(data)
	kept := slices.Concat(fields.shared, fields.batchSize)
	if len(kept) != 3 {
		t.Fatal("wrong cursor fields", kept)
	}
	want := bsonDocument(t, kept).Data
	clear(data)
	if got := bsonDocument(t, kept).Data; !bytes.Equal(got, want) {
		t.Fatal("cursor fields share memory with the encoded command")
	}
}

func TestCursorPanicCleansUpLatestCursor(t *testing.T) {
	cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}, {"c"}}, failAt: -1}
	r := &recorder{t: t, answer: cursors.answer}
	db := r.db()
	find := bson.D{{Key: "find", Value: "users"}}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		seen := 0
		for _, err := range db.Cursor(t.Context(), find) {
			if err != nil {
				t.Fatal(err)
			}
			seen++
			if seen == 2 {
				panic("consumer failure")
			}
		}
	}()
	if recovered != "consumer failure" {
		t.Fatal("consumer panic was not preserved", recovered)
	}
	if len(cursors.killed) != 1 || cursors.killed[0][1].Value.(bson.A)[0] != int64(2) {
		t.Fatal("panic did not clean up the latest cursor exactly once", cursors.killed)
	}
}

func TestCanceledCursorStillCleansUp(t *testing.T) {
	cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}}, failAt: -1}
	r := &recorder{t: t, answer: cursors.answer}
	db := r.db()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	find := bson.D{{Key: "find", Value: "users"}}
	for _, err := range db.Cursor(ctx, find) {
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		break
	}
	if len(cursors.killed) != 1 {
		t.Fatal("caller cancellation prevented cursor cleanup", cursors.killed)
	}
}

func TestGetMoreFailureCleansUpBeforeYieldingError(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}}, failAt: 1}
		r := &recorder{t: t}
		r.answer = func(sent *pb.Command) []*pb.Event {
			if interrupted && command(t, sent)[0].Key == "getMore" {
				return storetest.NativeIncomplete(nil, nil, pb.FailureCode_UNAVAILABLE)
			}
			return cursors.answer(sent)
		}
		db := r.db()
		find := bson.D{{Key: "find", Value: "users"}}
		var failures int
		for _, err := range db.Cursor(t.Context(), find) {
			if err == nil {
				continue
			}
			failures++
			if len(cursors.killed) != 1 {
				t.Errorf("getMore error yielded before cleanup: interrupted=%t kills=%d", interrupted, len(cursors.killed))
			}
		}
		if failures != 1 || len(cursors.killed) != 1 {
			t.Fatal("expected one failure and exactly one cleanup", failures, cursors.killed)
		}
	}
}
