//go:build integration

package search_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	weir "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-go/search"
)

type item struct {
	Group string `json:"group"`
	Name  string `json:"name"`
	Rank  int    `json:"rank"`
}

// index returns the pre-created disposable index named by
// WEIR_SEARCH_RESOURCE behind WEIR_SEARCH_STORE.
func index(t *testing.T) search.Index[item] {
	t.Helper()
	address, store, name := os.Getenv("WEIR_ADDRESS"), os.Getenv("WEIR_SEARCH_STORE"), os.Getenv("WEIR_SEARCH_RESOURCE")
	if address == "" || store == "" || name == "" {
		t.Skip("requires WEIR_ADDRESS, WEIR_SEARCH_STORE and WEIR_SEARCH_RESOURCE")
	}
	client, err := weir.New(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	cluster := search.Cluster{Client: client, Store: store}
	items := search.Index[item]{Cluster: cluster, Name: name, QueryVisible: true}
	return items
}

func TestIndexLifecycle(t *testing.T) {
	items := index(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	group := fmt.Sprintf("sdk-search-%d", time.Now().UnixNano())
	ids := []string{group + "-a", group + "-b", group + "-c"}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		for _, id := range ids {
			_ = items.Delete(cleanup, id)
		}
	})

	first := &item{Group: group, Name: "first", Rank: 1}
	if err := items.Create(ctx, ids[0], first); err != nil {
		t.Fatal(err)
	}
	if err := items.Create(ctx, ids[0], first); weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("second create was not refused", err)
	}
	if err := items.Replace(ctx, ids[1], first); weir.CodeOf(err) != weir.FailurePreconditionFailed {
		t.Fatal("replace of a missing document was not refused", err)
	}
	rank := map[string]int{"rank": 3}
	if err := items.Update(ctx, ids[0], rank); err != nil {
		t.Fatal(err)
	}
	const rename = `return function(current, incoming)
    if current == nil then return weir.reject("no such item") end
    current.name = incoming.name
    return current
end`
	renamed := map[string]string{"name": "renamed"}
	err := items.Transform(ctx, ids[0], rename, renamed)
	if err != nil {
		t.Fatal(err)
	}
	err = items.Transform(ctx, ids[1], rename, renamed)
	if weir.CodeOf(err) != weir.FailurePreconditionFailed || !strings.Contains(err.Error(), "no such item") {
		t.Fatal("rejection lost", err)
	}
	got, err := items.Get(ctx, ids[0])
	if err != nil || got.Name != "renamed" || got.Rank != 3 || got.Group != group {
		t.Fatal("update or transform lost", got, err)
	}
	second := &item{Group: group, Name: "second", Rank: 2}
	third := &item{Group: group, Name: "third", Rank: 1}
	if err := items.Put(ctx, ids[1], second); err != nil {
		t.Fatal(err)
	}
	if err := items.Put(ctx, ids[2], third); err != nil {
		t.Fatal(err)
	}
	many, err := items.GetMany(ctx, ids[2], group+"-none")
	if err != nil || many[0].Name != "third" || many[1] != nil {
		t.Fatal("wrong documents", many, err)
	}

	match := map[string]any{"term": map[string]any{"group.keyword": group}}
	order := []any{map[string]any{"rank": "desc"}, map[string]any{"name.keyword": "asc"}}
	query := map[string]any{"query": match, "sort": order, "size": 2}
	hits, err := items.Search(ctx, query)
	if err != nil || len(hits) != 2 || hits[0].ID != ids[0] || hits[1].Document.Name != "second" {
		t.Fatal("wrong hits", hits, err)
	}
	query["search_after"] = hits[1].Sort
	rest, err := items.Search(ctx, query)
	if err != nil || len(rest) != 1 || rest[0].ID != ids[2] {
		t.Fatal("search_after did not continue", rest, err)
	}
	count := map[string]any{"query": match}
	response, err := items.Do(ctx, http.MethodPost, "/_count", count)
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(response.Body), `"count":3`) {
		t.Fatal("wrong count", response, err)
	}
	bad := map[string]any{"query": map[string]any{"no_such_query": map[string]any{}}}
	var failed *search.StatusError
	if _, err := items.Search(ctx, bad); !errors.As(err, &failed) || failed.StatusCode != 400 {
		t.Fatal("bad query not reported", err)
	}
	root, err := items.Cluster.Do(ctx, http.MethodGet, "/", nil)
	if err != nil || root.StatusCode != 200 {
		t.Fatal("cluster root failed", root, err)
	}

	for _, id := range ids {
		if err := items.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := items.Get(ctx, ids[0]); !errors.Is(err, weir.ErrNotFound) {
		t.Fatal("deleted document still found", err)
	}
}
