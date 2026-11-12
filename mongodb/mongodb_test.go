package mongodb

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/internal/storetest"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type user struct {
	ID   string `bson:"_id,omitempty"`
	Name string `bson:"name"`
}

// recorder answers commands with a handler and keeps every command it saw.
type recorder struct {
	t        *testing.T
	commands []*pb.Command
	stores   []string
	answer   func(command *pb.Command) []*pb.Event
}

func (r *recorder) db() DB {
	handler := func(store string, command *pb.Command) []*pb.Event {
		r.commands = append(r.commands, command)
		r.stores = append(r.stores, store)
		return r.answer(command)
	}
	client := storetest.Client(r.t, handler)
	db := DB{Client: client, Store: "mongo", Name: "app"}
	return db
}

func (r *recorder) users() Collection[string, user] {
	users := Collection[string, user]{DB: r.db(), Name: "users"}
	return users
}

func bsonDocument(t *testing.T, value any) *pb.Document {
	t.Helper()
	data, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: bsonContentType, Data: data}
	return document
}

// command decodes a Native MongoDB command.
func command(t *testing.T, sent *pb.Command) bson.D {
	t.Helper()
	var decoded bson.D
	if err := bson.Unmarshal(sent.GetNative().GetRequest().GetData(), &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestWritesAddressTheRecordAndPutItsIDFirst(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.Applied() }}
	users := r.users()
	alice := &user{Name: "Alice"}
	if err := users.Put(t.Context(), "alice/1", alice); err != nil {
		t.Fatal(err)
	}
	put := r.commands[0].GetMutate()
	if put.GetResource() != "app/users/s:alice%2F1" || r.stores[0] != "mongo" {
		t.Fatal("wrong target", put.GetResource(), r.stores[0])
	}
	var stored bson.D
	if err := bson.Unmarshal(put.GetPut().GetData(), &stored); err != nil {
		t.Fatal(err)
	}
	if stored[0].Key != "_id" || stored[0].Value != "alice/1" || stored[1].Key != "name" {
		t.Fatal("_id is not first", stored)
	}
	if put.GetOptions().GetVisibility() == pb.MutationVisibility_QUERY_VISIBLE {
		t.Fatal("visibility requested without QueryVisible")
	}

	users.QueryVisible = true
	if err := users.Delete(t.Context(), "alice/1"); err != nil {
		t.Fatal(err)
	}
	remove := r.commands[1].GetMutate()
	if remove.GetDelete() == nil || remove.GetOptions().GetVisibility() != pb.MutationVisibility_QUERY_VISIBLE {
		t.Fatal("delete did not wait for visibility", remove)
	}
}

func TestIDKindsSelectTheirKeys(t *testing.T) {
	object := bson.NewObjectID()
	type named string
	cases := []struct {
		segment string
		stored  any
		want    string
	}{
		{want: "s:a"},
		{want: "s:n"},
		{want: "i:-7"},
		{want: "i:7"},
		{want: "oid:" + object.Hex()},
	}
	cases[0].segment, cases[0].stored = key("a")
	cases[1].segment, cases[1].stored = key(named("n"))
	cases[2].segment, cases[2].stored = key(-7)
	cases[3].segment, cases[3].stored = key(int32(7))
	cases[4].segment, cases[4].stored = key(object)
	for _, c := range cases {
		if c.segment != c.want {
			t.Fatal("wrong key", c.segment, c.want)
		}
	}
	if cases[1].stored != "n" || cases[2].stored != int64(-7) || cases[4].stored != object {
		t.Fatal("wrong stored _id", cases)
	}
}

func TestRecordDocumentKeepsOrMovesAnEqualID(t *testing.T) {
	first := bson.D{{Key: "_id", Value: int32(5)}, {Key: "n", Value: 1}}
	data, err := bson.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := recordDocument(int64(5), first)
	if err != nil || !bytes.Equal(kept.Data, data) {
		t.Fatal("a leading equal _id was re-encoded", err)
	}
	later := bson.D{{Key: "n", Value: 1}, {Key: "_id", Value: int32(5)}}
	moved, err := recordDocument(int64(5), later)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(moved.Data, data) {
		t.Fatal("a later _id was not moved first with its bytes", bson.Raw(moved.Data))
	}
	other := bson.D{{Key: "_id", Value: "b"}}
	if _, err := recordDocument("a", other); err == nil {
		t.Fatal("a different _id was accepted")
	}
	object := bson.NewObjectID()
	missing := bson.D{{Key: "n", Value: 1}}
	added, err := recordDocument(object, missing)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(added.Data).Lookup("_id").ObjectID(); got != object {
		t.Fatal("ObjectID _id was not added", got)
	}
	unset := struct {
		ID   string `bson:"_id"`
		Name string `bson:"name"`
	}{Name: "Ada"}
	filled, err := recordDocument("ada", unset)
	if err != nil {
		t.Fatal("a zero _id was taken as a different one", err)
	}
	if got := bson.Raw(filled.Data).Lookup("_id").StringValue(); got != "ada" {
		t.Fatal("a zero _id was not replaced", bson.Raw(filled.Data))
	}
}

func TestGetDecodesAndReportsMissingRecords(t *testing.T) {
	alice := &user{ID: "alice", Name: "Alice"}
	r := &recorder{t: t}
	r.answer = func(command *pb.Command) []*pb.Event {
		if command.GetRead().GetResource() == "app/users/s:alice" {
			return storetest.Read(bsonDocument(t, alice))
		}
		return storetest.Missing()
	}
	users := r.users()
	got, err := users.Get(t.Context(), "alice")
	if err != nil || *got != *alice {
		t.Fatal("wrong document", got, err)
	}
	_, err = users.Get(t.Context(), "bob")
	if !errors.Is(err, weir.ErrNotFound) {
		t.Fatal("missing record not reported", err)
	}
	many, err := users.GetMany(t.Context(), "bob", "alice", "bob")
	if err != nil || many[0] != nil || *many[1] != *alice || many[2] != nil {
		t.Fatal("wrong documents", many, err)
	}
	empty, err := users.GetMany(t.Context())
	if err != nil || len(empty) != 0 {
		t.Fatal("empty read", empty, err)
	}
}

func TestFailedRecordsReportTheirCode(t *testing.T) {
	r := &recorder{t: t}
	r.answer = func(command *pb.Command) []*pb.Event {
		if command.GetRead() != nil {
			return storetest.ReadFailure(pb.FailureCode_TARGET_NOT_FOUND)
		}
		return storetest.Mutation(pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
	}
	users := r.users()
	alice := &user{Name: "Alice"}
	err := users.Create(t.Context(), "alice", alice)
	var failed *weir.Error
	if !errors.As(err, &failed) || failed.Outcome != weir.MutationNotApplied || weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("wrong create error", err)
	}
	_, err = users.Get(t.Context(), "alice")
	if weir.CodeOf(err) != weir.FailureTargetNotFound || errors.Is(err, weir.ErrNotFound) {
		t.Fatal("wrong read error", err)
	}
	if err := users.Put(t.Context(), "alice", nil); err == nil {
		t.Fatal("nil document accepted")
	}
}

func TestUpdateAndTransformSendTheirPayloads(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.Applied() }}
	counters := Collection[int64, bson.M]{DB: r.db(), Name: "counters"}
	increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}
	if err := counters.Update(t.Context(), 9, increment); err != nil {
		t.Fatal(err)
	}
	expression := r.commands[0].GetMutate().GetAtomicTransform().GetBackendExpression()
	if r.commands[0].GetMutate().GetResource() != "app/counters/i:9" || expression.GetContentType() != updateContentType {
		t.Fatal("wrong update", r.commands[0])
	}
	const program = "return function(current, incoming) return incoming end"
	input := bson.D{{Key: "n", Value: 2}}
	err := counters.Transform(t.Context(), 9, program, input)
	if err != nil {
		t.Fatal(err)
	}
	lua := r.commands[1].GetMutate().GetAtomicTransform().GetLua()
	if string(lua.GetSource()) != program || lua.GetInput().GetContentType() != bsonContentType {
		t.Fatal("wrong transform", lua)
	}
	err = counters.Transform(t.Context(), 9, program, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.commands[2].GetMutate().GetAtomicTransform().GetLua().GetInput() != nil {
		t.Fatal("nil input was sent")
	}
}

func TestCommandReportsFailedReplies(t *testing.T) {
	replies := []bson.D{
		{{Key: "ok", Value: 1.0}, {Key: "n", Value: 3}},
		{{Key: "ok", Value: 0.0}, {Key: "code", Value: 26}, {Key: "codeName", Value: "NamespaceNotFound"}, {Key: "errmsg", Value: "ns not found"}},
		{{Key: "ok", Value: 1.0}, {Key: "writeErrors", Value: bson.A{bson.D{{Key: "index", Value: 0}, {Key: "code", Value: 11000}, {Key: "errmsg", Value: "duplicate key"}}}}},
		{{Key: "ok", Value: 1.0}, {Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 64}, {Key: "errmsg", Value: "waiting for replication timed out"}}}},
	}
	next := 0
	r := &recorder{t: t}
	r.answer = func(*pb.Command) []*pb.Event {
		reply := replies[next]
		next++
		return storetest.Native(nil, bsonDocument(t, reply).Data)
	}
	db := r.db()
	ping := bson.D{{Key: "ping", Value: 1}}
	reply, err := db.Command(t.Context(), ping)
	if err != nil || reply.Lookup("n").Int32() != 3 {
		t.Fatal("wrong reply", reply, err)
	}
	if r.commands[0].GetNative().GetResource() != "app" {
		t.Fatal("wrong database", r.commands[0])
	}
	wantCodes := []int32{26, 11000, 64}
	for _, code := range wantCodes {
		reply, err := db.Command(t.Context(), ping)
		var failed *CommandError
		if !errors.As(err, &failed) || failed.Code != code || reply == nil {
			t.Fatal("failure not reported", code, err)
		}
	}
}

func TestCommandKeepsACompleteReplyDespiteALaterLostConnection(t *testing.T) {
	ok := bson.D{{Key: "ok", Value: 1.0}}
	lost := false
	r := &recorder{t: t}
	r.answer = func(*pb.Command) []*pb.Event {
		if lost {
			return []*pb.Event{storetest.Lost}
		}
		return append(storetest.Native(nil, bsonDocument(t, ok).Data), storetest.Lost)
	}
	db := r.db()
	ping := bson.D{{Key: "ping", Value: 1}}
	reply, err := db.Command(t.Context(), ping)
	if err != nil || reply.Lookup("ok").Double() != 1 {
		t.Fatal("complete reply was discarded", reply, err)
	}
	lost = true
	if _, err := db.Command(t.Context(), ping); !errors.Is(err, weir.ErrInterrupted) {
		t.Fatal("lost connection not interrupted", err)
	}
}

func TestCommandInterruptedByAnUnavailableBackend(t *testing.T) {
	code := pb.FailureCode_UNAVAILABLE
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.NativeIncomplete(nil, []byte{1}, code) }}
	db := r.db()
	ping := bson.D{{Key: "ping", Value: 1}}
	_, err := db.Command(t.Context(), ping)
	if !errors.Is(err, weir.ErrInterrupted) || weir.CodeOf(err) != weir.FailureUnavailable {
		t.Fatal("unavailable backend did not interrupt", err)
	}
	code = pb.FailureCode_INVALID_ARGUMENT
	_, err = db.Command(t.Context(), ping)
	if err == nil || errors.Is(err, weir.ErrInterrupted) {
		t.Fatal("a failure of the request itself was interrupted", err)
	}
}

// cursorServer answers find, aggregate and getMore from batches of names and
// records the cursors it was asked to kill.
type cursorServer struct {
	t       *testing.T
	batches [][]string
	failAt  int
	killed  []bson.D
}

func (s *cursorServer) answer(sent *pb.Command) []*pb.Event {
	request := command(s.t, sent)
	page := 0
	field := "firstBatch"
	switch request[0].Key {
	case "killCursors":
		s.killed = append(s.killed, request)
		reply := bson.D{{Key: "ok", Value: 1.0}}
		return storetest.Native(nil, bsonDocument(s.t, reply).Data)
	case "getMore":
		page = int(request[0].Value.(int64))
		field = "nextBatch"
		if page == s.failAt {
			reply := bson.D{{Key: "ok", Value: 0.0}, {Key: "code", Value: 43}, {Key: "errmsg", Value: "cursor not found"}}
			return storetest.Native(nil, bsonDocument(s.t, reply).Data)
		}
	}
	documents := bson.A{}
	for _, name := range s.batches[page] {
		document := bson.D{{Key: "_id", Value: name}, {Key: "name", Value: name}}
		documents = append(documents, document)
	}
	cursor := int64(page + 1)
	if page+1 == len(s.batches) {
		cursor = 0
	}
	page0 := bson.D{{Key: "id", Value: cursor}, {Key: "ns", Value: "app.users"}, {Key: field, Value: documents}}
	reply := bson.D{{Key: "cursor", Value: page0}, {Key: "ok", Value: 1.0}}
	return storetest.Native(nil, bsonDocument(s.t, reply).Data)
}

func TestFindFollowsTheCursorAndKillsAnAbandonedOne(t *testing.T) {
	cursors := &cursorServer{t: t, batches: [][]string{{"a", "b"}, {"c"}, {"d"}}, failAt: -1}
	r := &recorder{t: t, answer: cursors.answer}
	users := r.users()
	filter := bson.D{{Key: "name", Value: bson.D{{Key: "$gte", Value: "a"}}}}
	order := bson.E{Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}}
	var names []string
	for found, err := range users.Find(t.Context(), filter, order) {
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, found.Name)
	}
	if !slices.Equal(names, []string{"a", "b", "c", "d"}) || len(cursors.killed) != 0 {
		t.Fatal("wrong documents", names, cursors.killed)
	}
	find := command(t, r.commands[0])
	if find[0].Key != "find" || find[0].Value != "users" || find[2].Key != "sort" {
		t.Fatal("wrong find", find)
	}
	getMore := command(t, r.commands[1])
	if getMore[1].Value != "users" || getMore[2].Key != "$db" || getMore[2].Value != "app" {
		t.Fatal("getMore named the wrong collection", getMore)
	}

	for found, err := range users.Find(t.Context(), nil) {
		if err != nil || found.Name != "a" {
			t.Fatal(found, err)
		}
		break
	}
	if len(cursors.killed) != 1 || cursors.killed[0][1].Value.(bson.A)[0] != int64(1) || cursors.killed[0][2].Value != "app" {
		t.Fatal("abandoned cursor not killed", cursors.killed)
	}
}

func TestFailedGetMoreEndsTheSequenceAndKillsTheCursor(t *testing.T) {
	cursors := &cursorServer{t: t, batches: [][]string{{"a"}, {"b"}}, failAt: 1}
	r := &recorder{t: t, answer: cursors.answer}
	ping := bson.D{{Key: "find", Value: "users"}}
	var seen int
	var failure error
	for _, err := range r.db().Cursor(t.Context(), ping) {
		if err != nil {
			failure = err
			continue
		}
		seen++
	}
	var failed *CommandError
	if seen != 1 || !errors.As(failure, &failed) || len(cursors.killed) != 1 {
		t.Fatal("failed getMore not reported", seen, failure, cursors.killed)
	}
}

func TestCountReturnsTheMatchingDocuments(t *testing.T) {
	r := &recorder{t: t}
	empty := false
	r.answer = func(*pb.Command) []*pb.Event {
		documents := bson.A{bson.D{{Key: "n", Value: int32(42)}}}
		if empty {
			documents = bson.A{}
		}
		cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "app.users"}, {Key: "firstBatch", Value: documents}}
		reply := bson.D{{Key: "cursor", Value: cursor}, {Key: "ok", Value: 1.0}}
		return storetest.Native(nil, bsonDocument(t, reply).Data)
	}
	users := r.users()
	filter := bson.D{{Key: "name", Value: "a"}}
	count, err := users.Count(t.Context(), filter)
	if err != nil || count != 42 {
		t.Fatal("wrong count", count, err)
	}
	aggregate := command(t, r.commands[0])
	if aggregate[0].Key != "aggregate" || aggregate[0].Value != "users" {
		t.Fatal("wrong aggregate", aggregate)
	}
	empty = true
	count, err = users.Count(t.Context(), nil)
	if err != nil || count != 0 {
		t.Fatal("wrong empty count", count, err)
	}
}
