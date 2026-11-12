// Package mongodb reads and writes MongoDB documents through a Weir Store
// whose backend is MongoDB.
//
// A Collection reads and writes documents by _id and runs find and count
// queries. A DB runs any other command:
//
//	db := mongodb.DB{Client: client, Store: "mongo", Name: "shop"}
//	users := mongodb.Collection[string, User]{DB: db, Name: "users"}
//
//	ada := &User{Name: "Ada"}
//	err := users.Put(ctx, "ada", ada)
//	user, err := users.Get(ctx, "ada")
//
//	active := bson.D{{Key: "active", Value: true}}
//	for user, err := range users.Find(ctx, active) {
//		...
//	}
//
//	ping := bson.D{{Key: "ping", Value: 1}}
//	reply, err := db.Command(ctx, ping)
//
// Set a deadline on every context. A failed record is a *weir.Error: check it
// with weir.CodeOf, and with errors.Is against weir.ErrNotFound and
// weir.ErrInterrupted. The package keeps no state of its own; DB and
// Collection are plain values that are safe to copy and share.
package mongodb

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/internal/record"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	bsonContentType   = "application/bson"
	updateContentType = "application/vnd.weir.mongodb-update.v1+bson"

	// killTimeout bounds the killCursors sent after a loop stops early.
	killTimeout = 5 * time.Second
)

// DB is a MongoDB database behind a Weir Store.
type DB struct {
	Client *weir.Client
	// Store names a Weir Store whose backend is MongoDB.
	Store string
	// Name is the database name.
	Name string
}

// CommandError is a command MongoDB answered with ok: 0, a write error or a
// write concern error. For write errors it describes the first one.
type CommandError struct {
	Code    int32
	Name    string
	Message string
	// Reply is the complete reply.
	Reply bson.Raw
}

func (e *CommandError) Error() string {
	if e.Name == "" {
		return fmt.Sprintf("mongodb: command failed: code %d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("mongodb: command failed: %s (%d): %s", e.Name, e.Code, e.Message)
}

// Command runs one command in the database and returns its reply. A reply
// with ok: 0, writeErrors or a writeConcernError comes with a *CommandError.
// A command is sent again only when a server refused it before it began. One
// a lost connection or an unavailable backend cut short reports
// weir.ErrInterrupted and may have run; send it again only when that is safe.
func (db DB) Command(ctx context.Context, command any) (bson.Raw, error) {
	data, err := bson.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("mongodb: encode command: %w", err)
	}
	document := &weir.Document{ContentType: bsonContentType, Data: data}
	request := &weir.NativeRequest{Resource: db.resource(), Request: document}
	result, err := db.target().Native(ctx, request)
	if err != nil {
		return nil, err
	}
	reply := bson.Raw(result.Body)
	if err := reply.Validate(); err != nil {
		return nil, fmt.Errorf("mongodb: invalid reply: %w", err)
	}
	if failed := commandError(reply); failed != nil {
		return reply, failed
	}
	return reply, nil
}

// Cursor runs a command that opens a cursor, such as find, aggregate or
// listIndexes, and yields its documents. It sends getMore until the cursor
// ends, and kills the cursor when the loop stops early or fails. An error
// ends the sequence.
func (db DB) Cursor(ctx context.Context, command any) iter.Seq2[bson.Raw, error] {
	documents := func(yield func(bson.Raw, error) bool) {
		reply, err := db.Command(ctx, command)
		if err != nil {
			yield(nil, err)
			return
		}
		page, err := readPage(reply)
		if err != nil {
			yield(nil, err)
			return
		}
		space := parseNamespace(page.Cursor.Namespace)
		batch, cursor := page.Cursor.FirstBatch, page.Cursor.ID
		for {
			for _, document := range batch {
				if !yield(document, nil) {
					db.killCursor(ctx, space, cursor)
					return
				}
			}
			if cursor == 0 {
				return
			}
			getMore := bson.D{{Key: "getMore", Value: cursor}, {Key: "collection", Value: space.collection}, {Key: "$db", Value: space.database}}
			reply, err := db.Command(ctx, getMore)
			var next cursorPage
			if err == nil {
				next, err = readPage(reply)
			}
			if err != nil {
				db.killCursor(ctx, space, cursor)
				yield(nil, err)
				return
			}
			batch, cursor = next.Cursor.NextBatch, next.Cursor.ID
		}
	}
	return documents
}

// namespace names the collection of a cursor. Commands that follow the cursor
// name its database, as the command that opened it may have named one with
// $db.
type namespace struct {
	database   string
	collection string
}

// parseNamespace splits database.collection; database names hold no dot.
func parseNamespace(text string) namespace {
	database, collection, _ := strings.Cut(text, ".")
	space := namespace{database: database, collection: collection}
	return space
}

// cursorPage is the cursor of a find, aggregate or getMore reply.
type cursorPage struct {
	Cursor *struct {
		ID         int64      `bson:"id"`
		Namespace  string     `bson:"ns"`
		FirstBatch []bson.Raw `bson:"firstBatch"`
		NextBatch  []bson.Raw `bson:"nextBatch"`
	} `bson:"cursor"`
}

func readPage(reply bson.Raw) (cursorPage, error) {
	var page cursorPage
	if err := bson.Unmarshal(reply, &page); err != nil {
		return page, fmt.Errorf("mongodb: invalid cursor reply: %w", err)
	}
	if page.Cursor == nil {
		return page, fmt.Errorf("mongodb: reply has no cursor")
	}
	return page, nil
}

// killCursor releases a cursor a loop left open. The server also times idle
// cursors out, so a failure here is ignored.
func (db DB) killCursor(ctx context.Context, space namespace, cursor int64) {
	if cursor == 0 {
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTimeout)
	defer cancel()
	command := bson.D{{Key: "killCursors", Value: space.collection}, {Key: "cursors", Value: bson.A{cursor}}, {Key: "$db", Value: space.database}}
	_, _ = db.Command(cleanup, command)
}

func (db DB) target() record.Target {
	target := record.Target{Client: db.Client, Store: db.Store}
	return target
}

// resource addresses the database. An empty Name leaves the database to a
// $db field in each command.
func (db DB) resource() string {
	if db.Name == "" {
		return ""
	}
	return weir.EncodeSegment(db.Name)
}

// commandError returns the failure a reply reports, or nil.
func commandError(reply bson.Raw) *CommandError {
	ok, _ := reply.Lookup("ok").AsFloat64OK()
	if ok != 1 {
		return replyError(reply, reply)
	}
	if errs, found := reply.Lookup("writeErrors").ArrayOK(); found {
		values, _ := errs.Values()
		if len(values) > 0 {
			first, _ := values[0].DocumentOK()
			return replyError(reply, first)
		}
	}
	if concern, found := reply.Lookup("writeConcernError").DocumentOK(); found {
		return replyError(reply, concern)
	}
	return nil
}

// replyError describes the failure in part of a reply.
func replyError(reply, part bson.Raw) *CommandError {
	code, _ := part.Lookup("code").AsInt64OK()
	name, _ := part.Lookup("codeName").StringValueOK()
	message, _ := part.Lookup("errmsg").StringValueOK()
	failed := &CommandError{Code: int32(code), Name: name, Message: message, Reply: reply}
	return failed
}
