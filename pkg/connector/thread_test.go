package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestConvertPost_RootPost_NoThreadRoot(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "rootPostID", Message: "root text"}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cm.ThreadRoot != nil {
		t.Fatalf("expected nil ThreadRoot for a root post, got %v", *cm.ThreadRoot)
	}
}

// A thread reply whose attachments can't be uploaded (no intent, as in tests)
// takes the text-part path; it must still carry the thread root.
func TestConvertPost_ThreadReply_TextWithFiles_SetsThreadRoot(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{
		ID:      "p1",
		Message: "reply with file",
		RootID:  "rootPostID",
		FileIDs: []string{"f1"},
	}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cm.ThreadRoot == nil || *cm.ThreadRoot != networkid.MessageID("rootPostID") {
		t.Fatalf("expected ThreadRoot rootPostID, got %v", cm.ThreadRoot)
	}
}

// Empty message + files without an intent falls through to the fallback part.
func TestConvertPost_ThreadReply_FallbackPart_SetsThreadRoot(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{
		ID:      "p1",
		Message: "",
		RootID:  "rootPostID",
		FileIDs: []string{"f1"},
	}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 fallback part, got %d", len(cm.Parts))
	}
	if cm.ThreadRoot == nil || *cm.ThreadRoot != networkid.MessageID("rootPostID") {
		t.Fatalf("expected ThreadRoot rootPostID, got %v", cm.ThreadRoot)
	}
}

// Capabilities: bridgev2 only populates MatrixMessage.ThreadRoot when thread
// support is advertised.

func TestGetCapabilities_Thread(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps.Thread != event.CapLevelFullySupported {
		t.Errorf("Thread capability = %v, want FullySupported", caps.Thread)
	}
}

type threadTestTransport func(*http.Request) (*http.Response, error)

func (fn threadTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func fakeThreadHTTP(t *testing.T, handler func(*http.Request) (int, string)) {
	t.Helper()
	previous := http.DefaultTransport
	http.DefaultTransport = threadTestTransport(func(req *http.Request) (*http.Response, error) {
		if req.Body != nil {
			defer req.Body.Close()
		}
		if req.URL.Host != "mattermost.example" {
			t.Errorf("unexpected HTTP host %q", req.URL.Host)
			return nil, http.ErrNotSupported
		}
		status, body := handler(req)
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = previous })
}

func newThreadTestClient() *MattermostClient {
	const server = "https://mattermost.example"
	return &MattermostClient{baseURL: server, Token: "test-token", client: mattermost.NewClient(server, "test-token")}
}

type threadDownloadBot struct{ bridgev2.MatrixAPI }

func (threadDownloadBot) DownloadMedia(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
	return []byte("file content"), nil
}

func TestHandleMatrixMessageThreadRoot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		thread, reply *database.Message
		lookupBody    string
		lookupStatus  int
		wantRoot      networkid.MessageID
		attachment    bool
	}{
		{name: "nested reply", reply: &database.Message{ID: "prior-reply"}, lookupBody: `{"root_id":"root"}`, lookupStatus: 200, wantRoot: "root"},
		{name: "root post", reply: &database.Message{ID: "root"}, lookupBody: `{"id":"root"}`, lookupStatus: 200, wantRoot: "root"},
		{name: "stored reply root", reply: &database.Message{ID: "prior-reply", ThreadRoot: "root"}, wantRoot: "root"},
		{name: "explicit thread", thread: &database.Message{ID: "root"}, reply: &database.Message{ID: "prior-reply"}, wantRoot: "root"},
		{name: "stored explicit root", thread: &database.Message{ID: "prior-reply", ThreadRoot: "root"}, wantRoot: "root"},
		{name: "failed lookup", reply: &database.Message{ID: "root"}, lookupStatus: 404, wantRoot: "root"},
		{name: "nonreply"},
		{name: "attachment", reply: &database.Message{ID: "prior-reply"}, lookupBody: `{"root_id":"root"}`, lookupStatus: 200, wantRoot: "root", attachment: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gets, posts, uploads int
			fakeThreadHTTP(t, func(req *http.Request) (int, string) {
				switch req.Method + " " + req.URL.Path {
				case "POST /api/v4/posts":
					posts++
					var post struct {
						RootID  networkid.MessageID `json:"root_id"`
						FileIDs []string            `json:"file_ids"`
					}
					if err := json.NewDecoder(req.Body).Decode(&post); err != nil {
						t.Errorf("decode sent post: %v", err)
						return 400, ""
					}
					if post.RootID != tc.wantRoot {
						t.Errorf("sent root_id = %q, want %q", post.RootID, tc.wantRoot)
					}
					if tc.attachment && (len(post.FileIDs) != 1 || post.FileIDs[0] != "uploaded-file") {
						t.Errorf("sent file IDs = %v, want uploaded-file", post.FileIDs)
					}
					return 201, `{"id":"new-post"}`
				case "POST /api/v4/files":
					uploads++
					return 201, `{"file_infos":[{"id":"uploaded-file"}]}`
				default:
					if req.Method == http.MethodGet && tc.reply != nil && req.URL.Path == "/api/v4/posts/"+string(tc.reply.ID) {
						gets++
						if tc.lookupStatus != 0 {
							return tc.lookupStatus, tc.lookupBody
						}
					}
					t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
					return 404, ""
				}
			})
			c := newThreadTestClient()
			content := &event.MessageEventContent{MsgType: event.MsgText, Body: "hello"}
			if tc.attachment {
				content.MsgType = event.MsgFile
				content.URL = "mxc://example/file"
				c.UserLogin = &bridgev2.UserLogin{Bridge: &bridgev2.Bridge{Bot: threadDownloadBot{}}}
			}
			resp, err := c.HandleMatrixMessage(context.Background(), &bridgev2.MatrixMessage{
				MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Content: content, Portal: makePortal("channel")},
				ThreadRoot:      tc.thread,
				ReplyTo:         tc.reply,
			})
			if err != nil {
				t.Fatal(err)
			}
			if resp.DB.ID != "new-post" || resp.DB.ThreadRoot != tc.wantRoot {
				t.Errorf("response ID = %q, ThreadRoot = %q, want new-post and %q", resp.DB.ID, resp.DB.ThreadRoot, tc.wantRoot)
			}
			wantGets, wantUploads := 0, 0
			if tc.lookupStatus != 0 {
				wantGets = 1
			}
			if tc.attachment {
				wantUploads = 1
			}
			if gets != wantGets || uploads != wantUploads || posts != 1 {
				t.Errorf("lookups/uploads/posts = %d/%d/%d, want %d/%d/1", gets, uploads, posts, wantGets, wantUploads)
			}
		})
	}
}

func TestChannelBackfillKeepsRepliesInThreads(t *testing.T) {
	fakeThreadHTTP(t, func(req *http.Request) (int, string) {
		if req.Method != http.MethodGet || req.URL.Path != "/api/v4/channels/channel/posts" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
			return 404, ""
		}
		return 200, `{
			"order": ["orphan-reply", "reply", "root"],
			"posts": {
				"root": {"id":"root","message":"root text"},
				"reply": {"id":"reply","root_id":"root","message":"reply text"},
				"orphan-reply": {"id":"orphan-reply","root_id":"older-root","message":"reply on another page"}
			}
		}`
	})
	c := newThreadTestClient()
	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: makePortal("channel"), Count: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("got %d posts, want 3", len(resp.Messages))
	}
	for i, want := range []struct{ id, root networkid.MessageID }{
		{"root", ""}, {"reply", "root"}, {"orphan-reply", "older-root"},
	} {
		got := resp.Messages[i]
		var root networkid.MessageID
		if got.ThreadRoot != nil {
			root = *got.ThreadRoot
		}
		if got.ID != want.id || root != want.root || got.ReplyTo != nil || got.ShouldBackfillThread {
			t.Errorf("post %d: ID %q, root %q, ReplyTo %v, ShouldBackfillThread %v", i, got.ID, root, got.ReplyTo, got.ShouldBackfillThread)
		}
	}
}
