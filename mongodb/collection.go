package mongodb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strconv"

	weir "github.com/batchstream/weir-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ID is the type of a record's _id: a string, an integer or an ObjectID.
type ID interface {
	~string | ~int | ~int32 | ~int64 | bson.ObjectID
}

// Collection holds documents of type T whose _id values have type K. Writes
// take the _id separately; a document may omit its _id field, and one it has
// must equal the id. The collection must exist before its first write.
type Collection[K ID, T any] struct {
	DB   DB
	Name string
	// QueryVisible makes each write return only once later queries see it.
	QueryVisible bool
}

// Get returns the document with the id, or weir.ErrNotFound.
func (c Collection[K, T]) Get(ctx context.Context, id K) (*T, error) {
	documents, err := c.GetMany(ctx, id)
	if err != nil {
		return nil, err
	}
	if documents[0] == nil {
		return nil, fmt.Errorf("mongodb: %s %v: %w", c.Name, id, weir.ErrNotFound)
	}
	return documents[0], nil
}

// GetMany returns the documents with the ids in one call, in order, with nil
// for each missing one.
func (c Collection[K, T]) GetMany(ctx context.Context, ids ...K) ([]*T, error) {
	resources := make([]string, len(ids))
	for i, id := range ids {
		segment, _ := key(id)
		resources[i] = c.resource(segment)
	}
	documents, err := c.DB.target().Read(ctx, resources)
	if err != nil {
		return nil, fmt.Errorf("mongodb: %w", err)
	}
	values := make([]*T, len(documents))
	for i, document := range documents {
		if document == nil {
			continue
		}
		value := new(T)
		if err := bson.Unmarshal(document.GetData(), value); err != nil {
			return nil, fmt.Errorf("mongodb: decode %s %v: %w", c.Name, ids[i], err)
		}
		values[i] = value
	}
	return values, nil
}

// Create writes a document that must not exist. An existing one, or a unique
// index that rejects it, fails it with FailurePreconditionFailed.
func (c Collection[K, T]) Create(ctx context.Context, id K, document *T) error {
	return c.write(ctx, weir.MutationCreate, id, document)
}

// Put creates or replaces the document.
func (c Collection[K, T]) Put(ctx context.Context, id K, document *T) error {
	return c.write(ctx, weir.MutationPut, id, document)
}

// Replace replaces a document that must exist. A missing one fails it with
// FailurePreconditionFailed.
func (c Collection[K, T]) Replace(ctx context.Context, id K, document *T) error {
	return c.write(ctx, weir.MutationReplace, id, document)
}

// Delete deletes the document. Deleting a missing document succeeds.
func (c Collection[K, T]) Delete(ctx context.Context, id K) error {
	segment, _ := key(id)
	request := &weir.MutateRequest{Resource: c.resource(segment), Action: weir.MutationDelete}
	return c.mutate(ctx, request)
}

// Update changes fields of a document that must exist with an update document
// of $set, $unset and $inc operators. A missing document fails it with
// FailurePreconditionFailed.
func (c Collection[K, T]) Update(ctx context.Context, id K, update any) error {
	data, err := bson.Marshal(update)
	if err != nil {
		return fmt.Errorf("mongodb: encode update: %w", err)
	}
	segment, _ := key(id)
	expression := &weir.Document{ContentType: updateContentType, Data: data}
	request := &weir.MutateRequest{Resource: c.resource(segment), Action: weir.MutationAtomicTransform, BackendExpression: expression}
	return c.mutate(ctx, request)
}

// Transform runs a Lua program atomically against the document. The program
// returns one function, called as function(current, incoming), where current
// is the stored document or nil and incoming is input encoded as BSON, or nil
// when input is nil. Its result replaces the document, and weir.reject(message)
// fails it with FailurePreconditionFailed. See the Lua guide for the rest.
func (c Collection[K, T]) Transform(ctx context.Context, id K, program string, input any) error {
	lua := &weir.LuaTransform{Source: []byte(program)}
	if input != nil {
		data, err := bson.Marshal(input)
		if err != nil {
			return fmt.Errorf("mongodb: encode transform input: %w", err)
		}
		lua.Input = &weir.Document{ContentType: bsonContentType, Data: data}
	}
	segment, _ := key(id)
	request := &weir.MutateRequest{Resource: c.resource(segment), Action: weir.MutationAtomicTransform, Lua: lua}
	return c.mutate(ctx, request)
}

// Find yields the documents that match a filter, nil for all. Options are
// further fields of MongoDB's find command, such as sort, projection, skip,
// limit and batchSize. An error ends the sequence.
func (c Collection[K, T]) Find(ctx context.Context, filter any, options ...bson.E) iter.Seq2[*T, error] {
	command := bson.D{{Key: "find", Value: c.Name}, {Key: "filter", Value: orEmpty(filter)}}
	command = append(command, options...)
	values := func(yield func(*T, error) bool) {
		for document, err := range c.DB.Cursor(ctx, command) {
			if err != nil {
				yield(nil, err)
				return
			}
			value := new(T)
			if err := bson.Unmarshal(document, value); err != nil {
				yield(nil, fmt.Errorf("mongodb: decode %s: %w", c.Name, err))
				return
			}
			if !yield(value, nil) {
				return
			}
		}
	}
	return values
}

// Count returns the number of documents that match a filter, nil for all. It
// counts the matching documents; for a cheap estimate of a whole collection,
// run the count command with DB.Command.
func (c Collection[K, T]) Count(ctx context.Context, filter any) (int64, error) {
	match := bson.D{{Key: "$match", Value: orEmpty(filter)}}
	total := bson.D{{Key: "$count", Value: "n"}}
	command := bson.D{
		{Key: "aggregate", Value: c.Name},
		{Key: "pipeline", Value: bson.A{match, total}},
		{Key: "cursor", Value: bson.D{}},
	}
	var count int64
	for document, err := range c.DB.Cursor(ctx, command) {
		if err != nil {
			return 0, err
		}
		count, _ = document.Lookup("n").AsInt64OK()
	}
	return count, nil
}

func (c Collection[K, T]) write(ctx context.Context, action weir.MutationAction, id K, document *T) error {
	if document == nil {
		return errors.New("mongodb: document is nil")
	}
	segment, stored := key(id)
	value := any(document)
	if reflect.TypeFor[T]().Kind() == reflect.Interface {
		// The generic API adds this pointer, not the caller's document. An
		// interface's concrete value must reach bson.Marshal directly so its
		// top-level Marshaler hook keeps precedence over ValueMarshaler.
		value = *document
	}
	encoded, err := recordDocument(stored, value)
	if err != nil {
		return fmt.Errorf("mongodb: encode %s %v: %w", c.Name, id, err)
	}
	request := &weir.MutateRequest{Resource: c.resource(segment), Action: action, Document: encoded}
	return c.mutate(ctx, request)
}

func (c Collection[K, T]) mutate(ctx context.Context, request *weir.MutateRequest) error {
	err := c.DB.target().Mutate(ctx, request, c.QueryVisible)
	if err != nil {
		return fmt.Errorf("mongodb: %s: %w", request.Resource, err)
	}
	return nil
}

// resource addresses one record: database/collection/key.
func (c Collection[K, T]) resource(segment string) string {
	resource := weir.EncodeSegment(c.DB.Name) + "/" + weir.EncodeSegment(c.Name) + "/" + weir.EncodeSegment(segment)
	return resource
}

// key returns the resource key of an id, s:, i: or oid:, and the _id value
// Weir stores for it: a string, an int64 or an ObjectID.
func key[K ID](id K) (string, any) {
	value := reflect.ValueOf(id)
	switch value.Kind() {
	case reflect.String:
		text := value.String()
		return "s:" + text, text
	case reflect.Int, reflect.Int32, reflect.Int64:
		number := value.Int()
		return "i:" + strconv.FormatInt(number, 10), number
	}
	object := any(id).(bson.ObjectID)
	return "oid:" + object.Hex(), object
}

// recordDocument encodes value as BSON whose first field is an _id equal to
// id, as Weir requires. It moves an _id the value has to the front, keeping
// its bytes, and adds one when it has none or leaves it zero.
func recordDocument(id any, value any) (*weir.Document, error) {
	raw, err := bson.Marshal(value)
	if err != nil {
		return nil, err
	}
	elements, err := bson.Raw(raw).Elements()
	if err != nil {
		return nil, err
	}
	var first []byte
	for i, element := range elements {
		if element.Key() != "_id" || zeroID(element.Value()) {
			continue
		}
		if !sameID(element.Value(), id) {
			return nil, fmt.Errorf("document _id %v differs from %v", element.Value(), id)
		}
		if i == 0 {
			document := &weir.Document{ContentType: bsonContentType, Data: raw}
			return document, nil
		}
		first = element
	}
	if first == nil {
		kind, data, err := bson.MarshalValue(id)
		if err != nil {
			return nil, err
		}
		first = append([]byte{byte(kind)}, "_id\x00"...)
		first = append(first, data...)
	}
	data := make([]byte, 4, len(raw)+len(first))
	data = append(data, first...)
	for _, element := range elements {
		if element.Key() != "_id" {
			data = append(data, element...)
		}
	}
	data = append(data, 0)
	binary.LittleEndian.PutUint32(data, uint32(len(data)))
	document := &weir.Document{ContentType: bsonContentType, Data: data}
	return document, nil
}

// zeroID reports an _id a document left at its zero value or null, as a
// struct field without omitempty does, which counts as no _id.
func zeroID(value bson.RawValue) bool {
	switch value.Type {
	case bson.TypeNull:
		return true
	case bson.TypeString:
		return value.StringValue() == ""
	case bson.TypeInt32:
		return value.Int32() == 0
	case bson.TypeInt64:
		return value.Int64() == 0
	case bson.TypeObjectID:
		return value.ObjectID().IsZero()
	}
	return false
}

// sameID reports whether a stored _id matches an id as Weir compares them: a
// string, an int32 or int64 integer, or an ObjectID.
func sameID(stored bson.RawValue, id any) bool {
	switch want := id.(type) {
	case string:
		text, ok := stored.StringValueOK()
		return ok && text == want
	case int64:
		if number, ok := stored.Int32OK(); ok {
			return int64(number) == want
		}
		number, ok := stored.Int64OK()
		return ok && number == want
	case bson.ObjectID:
		object, ok := stored.ObjectIDOK()
		return ok && object == want
	}
	return false
}

func orEmpty(filter any) any {
	if filter == nil {
		empty := bson.D{}
		return empty
	}
	return filter
}
