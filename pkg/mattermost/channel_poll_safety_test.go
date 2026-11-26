package mattermost

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestConnectContextCancelsPendingHandshake(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		http.Error(w, "released", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := NewClient(srv.URL, "fixture")
	done := make(chan error, 1)
	go func() { done <- client.ConnectContext(ctx) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("websocket handshake did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled handshake succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConnectContext ignored cancellation during handshake")
	}
}

func TestGetAllChannelsIncludingArchivedRejectsIncompleteSnapshots(t *testing.T) {
	for _, body := range []string{
		`null`, `[{"id":"channel","delete_at":0}`, `[{"id":"channel"}]`,
		`[{"id":"","delete_at":0}]`, `[{"id":"channel","delete_at":-1}]`,
		`[{"id":"channel","delete_at":0},{"id":"channel","delete_at":0}]`,
	} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v4/users/user/channels" || r.URL.Query().Get("include_deleted") != "true" {
					t.Errorf("unexpected listing URL: %s", r.URL)
				}
				fmt.Fprint(w, body)
			}))
			defer srv.Close()
			channels, err := GetAllChannelsIncludingArchived(t.Context(), srv.URL, "fixture", "user")
			if err == nil || channels != nil {
				t.Fatalf("incomplete snapshot accepted: channels=%v error=%v", channels, err)
			}
		})
	}
}
