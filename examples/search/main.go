// Search writes documents of a typed index, searches them and deletes them.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/search"
)

type article struct {
	Title string `json:"title"`
	Topic string `json:"topic"`
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	address := flag.String("address", "127.0.0.1:7447", "Weir application address")
	store := flag.String("store", "search", "Store whose backend is Elasticsearch or OpenSearch")
	index := flag.String("index", "weir_example", "existing index")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := weir.New(*address)
	if err != nil {
		return err
	}
	defer client.Close()

	cluster := search.Cluster{Client: client, Store: *store}
	articles := search.Index[article]{Cluster: cluster, Name: *index, QueryVisible: true}

	ids := []string{"example-a", "example-b"}
	titles := []string{"Rivers", "Weirs"}
	for i, id := range ids {
		written := &article{Title: titles[i], Topic: "example-water"}
		if err := articles.Put(ctx, id, written); err != nil {
			return err
		}
	}
	match := map[string]any{"term": map[string]any{"topic.keyword": "example-water"}}
	order := []any{map[string]any{"title.keyword": "asc"}}
	query := map[string]any{"query": match, "sort": order}
	hits, err := articles.Search(ctx, query)
	if err != nil {
		return err
	}
	for _, hit := range hits {
		fmt.Println("hit:", hit.ID, hit.Document.Title)
	}

	health, err := cluster.Do(ctx, http.MethodGet, "/_cluster/health", nil)
	if err != nil {
		return err
	}
	fmt.Println("cluster health status:", health.StatusCode)
	for _, id := range ids {
		if err := articles.Delete(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
