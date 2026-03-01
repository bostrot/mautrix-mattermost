package mattermost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetSelf(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/users/me" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer testtoken" {
			t.Error("missing or wrong Authorization header")
		}
		json.NewEncoder(w).Encode(User{ID: "user1", Username: "alice"})
	}))
	defer srv.Close()

	id, username, err := GetSelf(srv.URL, "testtoken")
	if err != nil {
		t.Fatalf("GetSelf error: %v", err)
	}
	if id != "user1" {
		t.Errorf("id = %q, want %q", id, "user1")
	}
	if username != "alice" {
		t.Errorf("username = %q, want %q", username, "alice")
	}
}

func TestGetSelf_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, _, err := GetSelf(srv.URL, "badtoken")
	if err == nil {
		t.Fatal("expected error for 401 response")
	}
}

func TestGetUser(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/users/user42" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(User{ID: "user42", Username: "bob"})
	}))
	defer srv.Close()

	u, err := GetUser(srv.URL, "tok", "user42")
	if err != nil {
		t.Fatalf("GetUser error: %v", err)
	}
	if u.Username != "bob" {
		t.Errorf("username = %q, want %q", u.Username, "bob")
	}
}

func TestGetChannel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/channels/ch1" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(Channel{ID: "ch1", DisplayName: "General", Type: "O"})
	}))
	defer srv.Close()

	ch, err := GetChannel(srv.URL, "tok", "ch1")
	if err != nil {
		t.Fatalf("GetChannel error: %v", err)
	}
	if ch.DisplayName != "General" {
		t.Errorf("DisplayName = %q, want %q", ch.DisplayName, "General")
	}
}

func TestGetPosts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pl := PostList{
			Order: []string{"p1"},
			Posts: map[string]*Post{
				"p1": {ID: "p1", Message: "hello", ChannelID: "ch1", UserID: "u1"},
			},
		}
		json.NewEncoder(w).Encode(pl)
	}))
	defer srv.Close()

	pl, err := GetPosts(srv.URL, "tok", "ch1", 10, "")
	if err != nil {
		t.Fatalf("GetPosts error: %v", err)
	}
	if len(pl.Order) != 1 {
		t.Errorf("order length = %d, want 1", len(pl.Order))
	}
	if pl.Posts["p1"].Message != "hello" {
		t.Errorf("unexpected message: %q", pl.Posts["p1"].Message)
	}
}

func TestGetTeams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/users/u1/teams" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]*Team{{ID: "t1", DisplayName: "Acme"}})
	}))
	defer srv.Close()

	teams, err := GetTeams(srv.URL, "tok", "u1")
	if err != nil {
		t.Fatalf("GetTeams error: %v", err)
	}
	if len(teams) != 1 || teams[0].ID != "t1" {
		t.Errorf("unexpected teams: %+v", teams)
	}
}
