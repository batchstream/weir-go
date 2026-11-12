// MongoDB writes, changes, finds and deletes documents of a typed collection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type note struct {
	Text  string `bson:"text"`
	Views int64  `bson:"views"`
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	address := flag.String("address", "127.0.0.1:7447", "Weir application address")
	store := flag.String("store", "mongo", "Store whose backend is MongoDB")
	database := flag.String("database", "weir_example", "existing database")
	collection := flag.String("collection", "records", "existing collection")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := weir.New(*address)
	if err != nil {
		return err
	}
	defer client.Close()

	db := mongodb.DB{Client: client, Store: *store, Name: *database}
	notes := mongodb.Collection[string, note]{DB: db, Name: *collection}

	first := &note{Text: "hello"}
	if err := notes.Put(ctx, "example-note", first); err != nil {
		return err
	}
	view := bson.D{{Key: "$inc", Value: bson.D{{Key: "views", Value: 1}}}}
	if err := notes.Update(ctx, "example-note", view); err != nil {
		return err
	}
	stored, err := notes.Get(ctx, "example-note")
	if err != nil {
		return err
	}
	fmt.Printf("get: %s, %d view\n", stored.Text, stored.Views)

	viewed := bson.D{{Key: "views", Value: bson.D{{Key: "$gte", Value: 1}}}}
	for found, err := range notes.Find(ctx, viewed) {
		if err != nil {
			return err
		}
		fmt.Println("find:", found.Text)
	}
	if err := notes.Delete(ctx, "example-note"); err != nil {
		return err
	}
	_, err = notes.Get(ctx, "example-note")
	fmt.Println("deleted:", errors.Is(err, weir.ErrNotFound))
	return nil
}
