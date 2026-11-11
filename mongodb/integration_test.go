//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type person struct {
	ID     string `bson:"_id,omitempty"`
	Name   string `bson:"name"`
	Visits int64  `bson:"visits"`
}

// collection returns the pre-created disposable collection named by
// WEIR_MONGO_RESOURCE, database/collection, behind WEIR_MONGO_STORE.
func collection(t *testing.T) (mongodb.DB, string) {
	t.Helper()
	address, store, resource := os.Getenv("WEIR_ADDRESS"), os.Getenv("WEIR_MONGO_STORE"), os.Getenv("WEIR_MONGO_RESOURCE")
	database, name, found := strings.Cut(resource, "/")
	if address == "" || store == "" || !found {
		t.Skip("requires WEIR_ADDRESS, WEIR_MONGO_STORE and WEIR_MONGO_RESOURCE")
	}
	client, err := weir.New(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	db := mongodb.DB{Client: client, Store: store, Name: database}
	return db, name
}

func TestCollectionLifecycle(t *testing.T) {
	db, name := collection(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	people := mongodb.Collection[string, person]{DB: db, Name: name, QueryVisible: true}
	prefix := fmt.Sprintf("sdk-mongodb-%d-", time.Now().UnixNano())
	ids := []string{prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e"}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		for _, id := range ids {
			_ = people.Delete(cleanup, id)
		}
	})

	ada := &person{Name: "Ada"}
	if err := people.Create(ctx, ids[0], ada); err != nil {
		t.Fatal(err)
	}
	if err := people.Create(ctx, ids[0], ada); weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("second create was not refused", err)
	}
	got, err := people.Get(ctx, ids[0])
	if err != nil || got.ID != ids[0] || got.Name != "Ada" {
		t.Fatal("wrong document", got, err)
	}
	missing := &person{Name: "Nobody"}
	if err := people.Replace(ctx, ids[1], missing); weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("replace of a missing document was not refused", err)
	}
	visit := bson.D{{Key: "$inc", Value: bson.D{{Key: "visits", Value: int64(2)}}}}
	if err := people.Update(ctx, ids[0], visit); err != nil {
		t.Fatal(err)
	}
	const rename = `return function(current, incoming)
    if current == nil then return weir.reject("no such person") end
    current.name = incoming.name
    return current
end`
	renamed := bson.D{{Key: "name", Value: "Ada L."}}
	err = people.Transform(ctx, ids[0], rename, renamed)
	if err != nil {
		t.Fatal(err)
	}
	err = people.Transform(ctx, ids[1], rename, renamed)
	if weir.CodeOf(err) != weir.FailurePreconditionFailed || !strings.Contains(err.Error(), "no such person") {
		t.Fatal("rejection lost", err)
	}
	got, err = people.Get(ctx, ids[0])
	if err != nil || got.Name != "Ada L." || got.Visits != 2 {
		t.Fatal("update or transform lost", got, err)
	}

	for _, id := range ids[1:] {
		other := &person{ID: id, Name: "Other"}
		if err := people.Put(ctx, id, other); err != nil {
			t.Fatal(err)
		}
	}
	many, err := people.GetMany(ctx, ids[4], prefix+"none", ids[0])
	if err != nil || many[0].ID != ids[4] || many[1] != nil || many[2].ID != ids[0] {
		t.Fatal("wrong documents", many, err)
	}
	mine := bson.D{{Key: "_id", Value: bson.D{{Key: "$regex", Value: "^" + prefix}}}}
	order := bson.E{Key: "sort", Value: bson.D{{Key: "_id", Value: -1}}}
	small := bson.E{Key: "batchSize", Value: 2}
	var found []string
	for found1, err := range people.Find(ctx, mine, order, small) {
		if err != nil {
			t.Fatal(err)
		}
		found = append(found, found1.ID)
	}
	if len(found) != 5 || found[0] != ids[4] || found[4] != ids[0] {
		t.Fatal("find did not follow its cursor", found)
	}
	for first, err := range people.Find(ctx, mine, small) {
		if err != nil || first.ID != ids[0] {
			t.Fatal(first, err)
		}
		break
	}
	count, err := people.Count(ctx, mine)
	if err != nil || count != 5 {
		t.Fatal("wrong count", count, err)
	}

	for _, id := range ids {
		if err := people.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := people.Get(ctx, ids[0]); !errors.Is(err, weir.ErrNotFound) {
		t.Fatal("deleted document still found", err)
	}
	if err := people.Delete(ctx, ids[0]); err != nil {
		t.Fatal("deleting a missing document failed", err)
	}
}

func TestCollectionKeysOfEveryKind(t *testing.T) {
	db, name := collection(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	numbers := mongodb.Collection[int64, bson.M]{DB: db, Name: name}
	number := time.Now().UnixNano()
	value := bson.M{"kind": "number"}
	if err := numbers.Put(ctx, number, &value); err != nil {
		t.Fatal(err)
	}
	defer numbers.Delete(context.WithoutCancel(ctx), number)
	got, err := numbers.Get(ctx, number)
	if err != nil || (*got)["_id"] != number {
		t.Fatal("integer key lost", got, err)
	}
	objects := mongodb.Collection[bson.ObjectID, bson.M]{DB: db, Name: name}
	object := bson.NewObjectID()
	value = bson.M{"kind": "object"}
	if err := objects.Put(ctx, object, &value); err != nil {
		t.Fatal(err)
	}
	defer objects.Delete(context.WithoutCancel(ctx), object)
	got, err = objects.Get(ctx, object)
	if err != nil || (*got)["_id"] != object {
		t.Fatal("ObjectID key lost", got, err)
	}
}

func TestCommandReportsMongoDBFailures(t *testing.T) {
	db, name := collection(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ping := bson.D{{Key: "ping", Value: 1}}
	if _, err := db.Command(ctx, ping); err != nil {
		t.Fatal(err)
	}
	invalid := bson.D{{Key: "find", Value: name}, {Key: "filter", Value: 1}}
	var failed *mongodb.CommandError
	if _, err := db.Command(ctx, invalid); !errors.As(err, &failed) || failed.Code == 0 || failed.Name == "" {
		t.Fatal("ok: 0 not reported", err)
	}
	id := fmt.Sprintf("sdk-mongodb-dup-%d", time.Now().UnixNano())
	document := bson.D{{Key: "_id", Value: id}}
	insert := bson.D{{Key: "insert", Value: name}, {Key: "documents", Value: bson.A{document, document}}}
	_, err := db.Command(ctx, insert)
	defer func() {
		remove := bson.D{{Key: "q", Value: document}, {Key: "limit", Value: 0}}
		cleanup := bson.D{{Key: "delete", Value: name}, {Key: "deletes", Value: bson.A{remove}}}
		_, _ = db.Command(context.WithoutCancel(ctx), cleanup)
	}()
	if !errors.As(err, &failed) || failed.Code != 11000 || failed.Reply.Lookup("n").Int32() != 1 {
		t.Fatal("write error not reported", err)
	}
}
