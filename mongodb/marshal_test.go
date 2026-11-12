package mongodb

import (
	"bytes"
	"testing"

	"github.com/batchstream/weir-go/internal/storetest"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type dualBSONHooks struct {
	scalar bool
}

func (d *dualBSONHooks) MarshalBSON() ([]byte, error) {
	document := bson.D{{Key: "source", Value: "document hook"}}
	return bson.Marshal(document)
}

func (d *dualBSONHooks) MarshalBSONValue() (byte, []byte, error) {
	var value any = "scalar hook"
	if !d.scalar {
		document := bson.D{{Key: "source", Value: "value hook"}}
		value = document
	}
	kind, data, err := bson.MarshalValue(value)
	return byte(kind), data, err
}

type valueBSONHook struct{}

func (valueBSONHook) MarshalBSON() ([]byte, error) {
	document := bson.D{{Key: "source", Value: "value-receiver hook"}}
	return bson.Marshal(document)
}

func TestWritesRejectNilInterfaceDocumentsBeforeInvokingHooks(t *testing.T) {
	r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.Applied() }}
	db := r.db()
	collection := Collection[string, any]{DB: db, Name: "records"}
	documents := []any{nil, (*valueBSONHook)(nil), (*dualBSONHooks)(nil), (*user)(nil)}
	for _, document := range documents {
		if err := collection.Create(t.Context(), "one", &document); err == nil {
			t.Error("create accepted a nil document")
		}
		if err := collection.Put(t.Context(), "one", &document); err == nil {
			t.Error("put accepted a nil document")
		}
		if err := collection.Replace(t.Context(), "one", &document); err == nil {
			t.Error("replace accepted a nil document")
		}
	}
	var document bson.Marshaler = (*valueBSONHook)(nil)
	abstract := Collection[string, bson.Marshaler]{DB: db, Name: "records"}
	if err := abstract.Put(t.Context(), "one", &document); err == nil {
		t.Error("non-empty interface accepted a typed nil")
	}
	if len(r.commands) != 0 {
		t.Error("a nil document reached Weir", len(r.commands))
	}
}

// A generic interface adds an artificial pointer around the concrete value.
// That wrapper must not change bson.Marshal's document-hook precedence or
// prevent encoding a non-empty interface.
func TestWritesPreserveTopLevelBSONHooksForInterfaceDocuments(t *testing.T) {
	for _, scalar := range []bool{false, true} {
		name := "document value hook"
		if scalar {
			name = "scalar value hook"
		}
		t.Run(name, func(t *testing.T) {
			r := &recorder{t: t, answer: func(*pb.Command) []*pb.Event { return storetest.Applied() }}
			db := r.db()
			document := &dualBSONHooks{scalar: scalar}
			concrete := Collection[string, dualBSONHooks]{DB: db, Name: "records"}
			if err := concrete.Create(t.Context(), "one", document); err != nil {
				t.Fatal("concrete pointer lost its MarshalBSON hook", err)
			}
			var erased any = document
			dynamic := Collection[string, any]{DB: db, Name: "records"}
			if err := dynamic.Put(t.Context(), "one", &erased); err != nil {
				t.Fatal("any lost its MarshalBSON hook", err)
			}
			var projected bson.Marshaler = document
			abstract := Collection[string, bson.Marshaler]{DB: db, Name: "records"}
			if err := abstract.Replace(t.Context(), "one", &projected); err != nil {
				t.Fatal("non-empty interface lost its MarshalBSON hook", err)
			}
			ordered := bson.D{{Key: "_id", Value: "one"}, {Key: "source", Value: "document hook"}}
			want, err := bson.Marshal(ordered)
			if err != nil {
				t.Fatal(err)
			}
			for _, command := range r.commands {
				mutation := command.GetMutate()
				stored := mutation.GetCreate()
				if stored == nil {
					stored = mutation.GetPut()
				}
				if stored == nil {
					stored = mutation.GetReplace()
				}
				if !bytes.Equal(stored.GetData(), want) {
					t.Fatal("interface changed the encoded document", bson.Raw(stored.GetData()))
				}
			}
		})
	}
}
