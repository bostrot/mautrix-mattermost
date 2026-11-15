package mattermost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetPostThread(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/posts/root1/thread" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Error("missing or wrong Authorization header")
		}
		json.NewEncoder(w).Encode(PostList{
			Order: []string{"root1", "r1"},
			Posts: map[string]*Post{
				"root1": {ID: "root1", Message: "root"},
				"r1":    {ID: "r1", Message: "reply", RootID: "root1"},
			},
		})
	}))
	defer srv.Close()

	pl, err := GetPostThread(srv.URL, "tok", "root1")
	if err != nil {
		t.Fatalf("GetPostThread error: %v", err)
	}
	if len(pl.Order) != 2 {
		t.Fatalf("order length = %d, want 2", len(pl.Order))
	}
	if pl.Posts["r1"].RootID != "root1" {
		t.Errorf("unexpected root id: %q", pl.Posts["r1"].RootID)
	}
}

func TestGetPostThread_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := GetPostThread(srv.URL, "tok", "missing"); err == nil {
		t.Fatal("expected error for 404 response")
	}
}
