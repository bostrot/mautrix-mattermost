package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// sendServer fakes GET /api/v4/posts/<id> from posts and records the root_id
// of POST /api/v4/posts.
type sendServer struct {
	posts    map[string]*mattermost.Post
	gets     int
	sentRoot string
}

func newSendClient(t *testing.T, ss *sendServer) *MattermostClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v4/posts" {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			ss.sentRoot, _ = body["root_id"].(string)
			json.NewEncoder(w).Encode(mattermost.Post{ID: "new1"})
			return
		}
		ss.gets++
		if p, ok := ss.posts[r.URL.Path[len("/api/v4/posts/"):]]; ok && r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(p)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := newBackfillClient(srv.URL)
	c.client = &mattermost.Client{}
	return c
}

func sendReply(t *testing.T, c *MattermostClient, target *database.Message) *database.Message {
	t.Helper()
	msg := makeMatrixMessage("ch1", event.MsgText, "hi")
	msg.ReplyTo = target
	resp, err := c.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	return resp.DB
}

// The live case behind the 400: a reply to a reply-only row that is a
// Mattermost thread reply must be sent to that thread's root.
func TestHandleMatrixMessage_ReplyToThreadReply_ResolvesRoot(t *testing.T) {
	ss := &sendServer{posts: map[string]*mattermost.Post{"r1": {ID: "r1", RootID: "root1"}}}
	db := sendReply(t, newSendClient(t, ss), &database.Message{ID: "r1"})
	if ss.sentRoot != "root1" {
		t.Fatalf("sent root_id = %q, want root1", ss.sentRoot)
	}
	if db.ThreadRoot != "root1" {
		t.Fatalf("stored ThreadRoot = %q, want root1", db.ThreadRoot)
	}
}

func TestHandleMatrixMessage_ReplyToTopLevelPost_StartsThread(t *testing.T) {
	ss := &sendServer{posts: map[string]*mattermost.Post{"p1": {ID: "p1"}}}
	db := sendReply(t, newSendClient(t, ss), &database.Message{ID: "p1"})
	if ss.sentRoot != "p1" || db.ThreadRoot != "p1" {
		t.Fatalf("sent %q, stored %q, want p1 for both", ss.sentRoot, db.ThreadRoot)
	}
}

func TestHandleMatrixMessage_StoredThreadRoot_SkipsLookup(t *testing.T) {
	ss := &sendServer{}
	sendReply(t, newSendClient(t, ss), &database.Message{ID: "r1", ThreadRoot: "root1"})
	if ss.gets != 0 {
		t.Fatalf("GetPost called %d times, want 0", ss.gets)
	}
	if ss.sentRoot != "root1" {
		t.Fatalf("sent root_id = %q, want root1", ss.sentRoot)
	}
}

func TestHandleMatrixMessage_LookupFails_FallsBackToTarget(t *testing.T) {
	ss := &sendServer{}
	sendReply(t, newSendClient(t, ss), &database.Message{ID: "gone"})
	if ss.sentRoot != "gone" {
		t.Fatalf("sent root_id = %q, want gone", ss.sentRoot)
	}
}

func TestHandleMatrixMessage_NoReply_NoRoot(t *testing.T) {
	ss := &sendServer{}
	c := newSendClient(t, ss)
	resp, err := c.HandleMatrixMessage(context.Background(), makeMatrixMessage("ch1", event.MsgText, "hi"))
	if err != nil {
		t.Fatal(err)
	}
	if ss.gets != 0 || ss.sentRoot != "" || resp.DB.ThreadRoot != "" {
		t.Fatalf("gets=%d sent=%q stored=%q, want no lookup and no root", ss.gets, ss.sentRoot, resp.DB.ThreadRoot)
	}
}
