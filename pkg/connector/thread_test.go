package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// ──────────────────────────────────────────────────────────────────────────────
// Inbound: Mattermost thread replies must map to Matrix threads
// ──────────────────────────────────────────────────────────────────────────────

func TestConvertPost_ThreadReply_SetsThreadRoot(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "p1", Message: "reply text", RootID: "rootPostID"}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cm.ThreadRoot == nil {
		t.Fatal("expected ThreadRoot to be set for a thread reply")
	}
	if *cm.ThreadRoot != networkid.MessageID("rootPostID") {
		t.Fatalf("ThreadRoot = %v, want rootPostID", *cm.ThreadRoot)
	}
}

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

// ──────────────────────────────────────────────────────────────────────────────
// Capabilities: bridgev2 only populates MatrixMessage.ThreadRoot when thread
// support is advertised.
// ──────────────────────────────────────────────────────────────────────────────

func TestGetCapabilities_Thread(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps.Thread != event.CapLevelFullySupported {
		t.Errorf("Thread capability = %v, want FullySupported", caps.Thread)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Outbound: Matrix threads/replies must set Mattermost root_id
// ──────────────────────────────────────────────────────────────────────────────

func TestOutboundRootID_PrefersThreadRoot(t *testing.T) {
	msg := makeMatrixMessage("ch1", event.MsgText, "hi")
	msg.ReplyTo = &database.Message{ID: "replyTarget"}
	msg.ThreadRoot = &database.Message{ID: "threadRoot"}

	if got := outboundRootID(msg); got != "threadRoot" {
		t.Fatalf("outboundRootID = %q, want threadRoot", got)
	}
}

func TestOutboundRootID_FallsBackToReplyTo(t *testing.T) {
	msg := makeMatrixMessage("ch1", event.MsgText, "hi")
	msg.ReplyTo = &database.Message{ID: "replyTarget"}

	if got := outboundRootID(msg); got != "replyTarget" {
		t.Fatalf("outboundRootID = %q, want replyTarget", got)
	}
}

func TestOutboundRootID_Empty(t *testing.T) {
	msg := makeMatrixMessage("ch1", event.MsgText, "hi")
	if got := outboundRootID(msg); got != "" {
		t.Fatalf("outboundRootID = %q, want empty", got)
	}
}

// Replying inside a thread: bridgev2 stores the thread parent on the root
// message, and Mattermost has a single flat thread, so the outer root wins.
func TestOutboundRootID_NestedThreadRoot(t *testing.T) {
	msg := makeMatrixMessage("ch1", event.MsgText, "hi")
	msg.ThreadRoot = &database.Message{ID: "someReply", ThreadRoot: "outerRoot"}

	if got := outboundRootID(msg); got != "outerRoot" {
		t.Fatalf("outboundRootID = %q, want outerRoot", got)
	}
}

func TestOutboundRootID_NilMessage(t *testing.T) {
	if got := outboundRootID(nil); got != "" {
		t.Fatalf("outboundRootID(nil) = %q, want empty", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Backfill: thread-scoped FetchMessages must use the thread endpoint
// ──────────────────────────────────────────────────────────────────────────────

type threadServer struct {
	srv          *httptest.Server
	threadPath   string
	channelPath  string
	channelQuery string
	posts        *mattermost.PostList
}

func newThreadServer(t *testing.T, posts *mattermost.PostList) *threadServer {
	t.Helper()
	ts := &threadServer{posts: posts}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/posts/root1/thread":
			ts.threadPath = r.URL.Path
			if r.Header.Get("Authorization") != "Bearer tok" {
				t.Errorf("missing Authorization header on thread request")
			}
			json.NewEncoder(w).Encode(ts.posts)
		case "/api/v4/channels/ch1/posts":
			ts.channelPath = r.URL.Path
			ts.channelQuery = r.URL.RawQuery
			json.NewEncoder(w).Encode(ts.posts)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

func newBackfillClient(url string) *MattermostClient {
	return &MattermostClient{
		UserID:  "me",
		Token:   "tok",
		baseURL: url,
		users:   map[string]*mattermost.User{},
	}
}

func threadPostList() *mattermost.PostList {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	posts := []*mattermost.Post{
		{ID: "root1", UserID: "u1", ChannelID: "ch1", Message: "root", CreateAt: ts.UnixMilli(), ReplyCount: 2},
		{ID: "r1", UserID: "u2", ChannelID: "ch1", RootID: "root1", Message: "reply one", CreateAt: ts.Add(time.Minute).UnixMilli()},
		{ID: "r2", UserID: "u2", ChannelID: "ch1", RootID: "root1", Message: "reply two", CreateAt: ts.Add(2 * time.Minute).UnixMilli()},
	}
	pl := &mattermost.PostList{Posts: map[string]*mattermost.Post{}}
	// Mattermost may return the thread in arbitrary order; the connector must sort.
	for i := len(posts) - 1; i >= 0; i-- {
		pl.Order = append(pl.Order, posts[i].ID)
		pl.Posts[posts[i].ID] = posts[i]
	}
	return pl
}

func TestFetchMessages_Thread_UsesThreadEndpoint(t *testing.T) {
	ts := newThreadServer(t, threadPostList())
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     makePortal("ch1"),
		ThreadRoot: networkid.MessageID("root1"),
		Forward:    true,
		Count:      50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if ts.threadPath != "/api/v4/posts/root1/thread" {
		t.Fatalf("thread endpoint not used, threadPath=%q channelPath=%q", ts.threadPath, ts.channelPath)
	}
	if ts.channelPath != "" {
		t.Fatalf("channel endpoint must not be used for thread backfill, got %q", ts.channelPath)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(resp.Messages))
	}
	// Chronological order: oldest first.
	wantOrder := []networkid.MessageID{"root1", "r1", "r2"}
	for i, want := range wantOrder {
		if resp.Messages[i].ID != want {
			t.Fatalf("message %d = %v, want %v", i, resp.Messages[i].ID, want)
		}
	}
	if resp.HasMore {
		t.Error("HasMore should be false for a complete thread")
	}
	if !resp.MarkRead {
		t.Error("MarkRead should be true for forward thread backfill")
	}
}

func TestFetchMessages_Thread_SetsThreadRootOnReplies(t *testing.T) {
	ts := newThreadServer(t, threadPostList())
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     makePortal("ch1"),
		ThreadRoot: networkid.MessageID("root1"),
		Forward:    true,
		Count:      50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	for _, msg := range resp.Messages {
		if msg.ID == "root1" {
			if msg.ThreadRoot != nil {
				t.Errorf("root post should not be marked as its own thread root, got %v", *msg.ThreadRoot)
			}
			continue
		}
		if msg.ThreadRoot == nil || *msg.ThreadRoot != networkid.MessageID("root1") {
			t.Errorf("message %v ThreadRoot = %v, want root1", msg.ID, msg.ThreadRoot)
		}
	}
}

// The connector must return the whole thread even when bridgev2 supplies an
// anchor: the framework's own cutoff removes the already-bridged prefix, and
// cutting here as well risks dropping replies.
func TestFetchMessages_Thread_ReturnsWholeThreadDespiteAnchor(t *testing.T) {
	ts := newThreadServer(t, threadPostList())
	c := newBackfillClient(ts.srv.URL)
	anchorTs := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Minute)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        makePortal("ch1"),
		ThreadRoot:    networkid.MessageID("root1"),
		Forward:       true,
		Count:         50,
		AnchorMessage: &database.Message{ID: "r1", Timestamp: anchorTs},
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("expected the whole thread (3 messages), got %+v", messageIDs(resp.Messages))
	}
}

// The thread endpoint cannot paginate, so Count must not truncate the batch:
// bridgev2 would never ask for the rest and the replies would be lost.
func TestFetchMessages_Thread_CountDoesNotTruncate(t *testing.T) {
	ts := newThreadServer(t, threadPostList())
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     makePortal("ch1"),
		ThreadRoot: networkid.MessageID("root1"),
		Forward:    true,
		Count:      2,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("thread must not be truncated, got %+v", messageIDs(resp.Messages))
	}
	if resp.HasMore {
		t.Error("HasMore must be false: the thread endpoint has no pagination")
	}
}

func TestFetchMessages_Thread_SkipsDeleted(t *testing.T) {
	pl := threadPostList()
	pl.Posts["r1"].DeleteAt = 12345
	ts := newThreadServer(t, pl)
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     makePortal("ch1"),
		ThreadRoot: networkid.MessageID("root1"),
		Forward:    true,
		Count:      50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("expected deleted post to be skipped, got %+v", messageIDs(resp.Messages))
	}
	for _, msg := range resp.Messages {
		if msg.ID == "r1" {
			t.Fatal("deleted post r1 must not be backfilled")
		}
	}
}

// Channel backfill must bridge thread replies inline, even though thread
// backfill is disabled by default in bridgev2's config. Skipping them and
// relying on ShouldBackfillThread loses replies on default installs.
func TestFetchMessages_Channel_IncludesReplies(t *testing.T) {
	pl := threadPostList()
	ts := newThreadServer(t, pl)
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  makePortal("ch1"),
		Forward: true,
		Count:   50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if ts.channelPath != "/api/v4/channels/ch1/posts" {
		t.Fatalf("channel endpoint not used, channelPath=%q", ts.channelPath)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("expected all 3 posts (root and replies), got %+v", messageIDs(resp.Messages))
	}
	wantOrder := []networkid.MessageID{"root1", "r1", "r2"}
	for i, want := range wantOrder {
		if resp.Messages[i].ID != want {
			t.Fatalf("message %d = %v, want %v", i, resp.Messages[i].ID, want)
		}
	}
	for _, msg := range resp.Messages {
		if msg.ID == "root1" {
			// Replies are already in this batch; asking for thread backfill
			// would fetch and bridge them a second time.
			if msg.ShouldBackfillThread {
				t.Error("channel backfill must not request thread backfill")
			}
			continue
		}
		if msg.ThreadRoot == nil || *msg.ThreadRoot != networkid.MessageID("root1") {
			t.Errorf("message %v ThreadRoot = %v, want root1", msg.ID, msg.ThreadRoot)
		}
	}
}

// A reply whose root post is not part of the same channel page (or was
// deleted) must still be bridged: it is a normal channel post.
func TestFetchMessages_Channel_IncludesOrphanReply(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pl := &mattermost.PostList{
		Order: []string{"orphan1"},
		Posts: map[string]*mattermost.Post{
			"orphan1": {
				ID:        "orphan1",
				UserID:    "u2",
				ChannelID: "ch1",
				RootID:    "missing-root",
				Message:   "reply without root in page",
				CreateAt:  ts.UnixMilli(),
			},
		},
	}
	srv := newThreadServer(t, pl)
	c := newBackfillClient(srv.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  makePortal("ch1"),
		Forward: true,
		Count:   50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if len(resp.Messages) != 1 || resp.Messages[0].ID != "orphan1" {
		t.Fatalf("orphan reply must be backfilled, got %+v", messageIDs(resp.Messages))
	}
	if resp.Messages[0].ThreadRoot == nil || *resp.Messages[0].ThreadRoot != networkid.MessageID("missing-root") {
		t.Fatalf("orphan reply ThreadRoot = %v, want missing-root", resp.Messages[0].ThreadRoot)
	}
	if resp.Messages[0].ShouldBackfillThread {
		t.Error("channel backfill must not request thread backfill")
	}
}

func TestFetchMessages_Channel_NoReplies_NoThreadBackfill(t *testing.T) {
	pl := threadPostList()
	pl.Posts["root1"].ReplyCount = 0
	ts := newThreadServer(t, pl)
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  makePortal("ch1"),
		Forward: true,
		Count:   50,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	for _, msg := range resp.Messages {
		if msg.ID == "root1" && msg.ShouldBackfillThread {
			t.Error("root post without replies should not request thread backfill")
		}
	}
}

// Backward pagination must continue from the oldest post of the page. Using the
// newest one (a reversed loop's last write) would re-fetch the same page
// forever or skip history.
func TestFetchMessages_Channel_BackwardCursorIsOldestPost(t *testing.T) {
	pl := threadPostList() // Order is newest-first: r2, r1, root1
	ts := newThreadServer(t, pl)
	c := newBackfillClient(ts.srv.URL)

	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  makePortal("ch1"),
		Forward: false,
		Count:   2,
	})
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if string(resp.Cursor) != "root1" {
		t.Fatalf("cursor = %q, want oldest post root1", resp.Cursor)
	}
	if !resp.HasMore {
		t.Error("HasMore should be true when the page is full")
	}

	// The next call must send the cursor as Mattermost's `before` parameter.
	if _, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  makePortal("ch1"),
		Forward: false,
		Cursor:  resp.Cursor,
		Count:   2,
	}); err != nil {
		t.Fatalf("second FetchMessages error: %v", err)
	}
	if !strings.Contains(ts.channelQuery, "before=root1") {
		t.Fatalf("second request query = %q, want before=root1", ts.channelQuery)
	}
}

func messageIDs(msgs []*bridgev2.BackfillMessage) []networkid.MessageID {
	ids := make([]networkid.MessageID, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}
