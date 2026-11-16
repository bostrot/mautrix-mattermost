package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func reactionClient() *MattermostClient {
	return &MattermostClient{
		UserID: "me",
		UserLogin: &bridgev2.UserLogin{
			UserLogin: &database.UserLogin{ID: "login-1"},
		},
	}
}

func postWithReactions(rs ...*mattermost.Reaction) *mattermost.Post {
	return &mattermost.Post{ID: "p1", Metadata: &mattermost.PostMetadata{Reactions: rs}}
}

func TestBackfillReactionsConvertsEmojiSenderAndTime(t *testing.T) {
	c := reactionClient()
	got := c.backfillReactions(postWithReactions(
		&mattermost.Reaction{UserID: "alice", PostID: "p1", EmojiName: "thumbsup", CreateAt: 1_700_000_000_000},
		&mattermost.Reaction{UserID: "me", PostID: "p1", EmojiName: "joy"},
	))
	if len(got) != 2 {
		t.Fatalf("got %d reactions, want 2", len(got))
	}

	a := got[0]
	if a.Sender.Sender != "alice" || a.Sender.IsFromMe || a.Sender.SenderLogin != "" {
		t.Errorf("other user's sender = %+v, want a plain remote sender", a.Sender)
	}
	if a.EmojiID != networkid.EmojiID("thumbsup") || a.Emoji != mmEmojiToUnicode("thumbsup") || a.Emoji == "thumbsup" {
		t.Errorf("emoji = (%q, %q), want the shortcode as id and the unicode character as emoji", a.EmojiID, a.Emoji)
	}
	if !a.Timestamp.Equal(time.UnixMilli(1_700_000_000_000)) {
		t.Errorf("timestamp = %v, want the reaction's create_at", a.Timestamp)
	}

	me := got[1]
	if !me.Sender.IsFromMe || me.Sender.SenderLogin != "login-1" || me.Sender.Sender != "me" {
		t.Errorf("own sender = %+v, want IsFromMe with the login", me.Sender)
	}
	if !me.Timestamp.IsZero() {
		t.Errorf("timestamp = %v, want zero so bridgev2 places it just after the message", me.Timestamp)
	}
}

func TestBackfillReactionsDropsUnusableEntries(t *testing.T) {
	c := reactionClient()
	got := c.backfillReactions(postWithReactions(
		nil,
		&mattermost.Reaction{UserID: "", EmojiName: "joy"},
		&mattermost.Reaction{UserID: "alice", EmojiName: ""},
		&mattermost.Reaction{UserID: "alice", EmojiName: "joy", DeleteAt: 5},
		&mattermost.Reaction{UserID: "alice", EmojiName: "heart"},
		&mattermost.Reaction{UserID: "alice", EmojiName: "heart"}, // repeat of the same user and emoji
		&mattermost.Reaction{UserID: "bob", EmojiName: "heart"},   // same emoji, different user
	))
	if len(got) != 2 {
		t.Fatalf("got %d reactions, want 2 (alice and bob heart): %+v", len(got), got)
	}
	if got[0].Sender.Sender != "alice" || got[1].Sender.Sender != "bob" {
		t.Errorf("senders = %q, %q, want alice then bob", got[0].Sender.Sender, got[1].Sender.Sender)
	}
}

func TestBackfillReactionsNilWhenNothingToAttach(t *testing.T) {
	c := reactionClient()
	for name, p := range map[string]*mattermost.Post{
		"nil post":      nil,
		"nil metadata":  {ID: "p"},
		"no reactions":  {ID: "p", Metadata: &mattermost.PostMetadata{}},
		"only unusable": postWithReactions(&mattermost.Reaction{UserID: "a", EmojiName: "x", DeleteAt: 1}),
	} {
		if got := c.backfillReactions(p); got != nil {
			t.Errorf("%s: got %+v, want nil", name, got)
		}
	}
}

// A custom emoji has no unicode form. It must come out exactly as the live
// reaction path produces it, so backfilled and live reactions agree.
func TestBackfillReactionsCustomEmojiMatchesLivePath(t *testing.T) {
	c := reactionClient()
	got := c.backfillReactions(postWithReactions(&mattermost.Reaction{UserID: "alice", EmojiName: "partyparrot"}))
	if len(got) != 1 || got[0].EmojiID != "partyparrot" || got[0].Emoji != mmEmojiToUnicode("partyparrot") {
		t.Fatalf("got %+v, want the same emoji conversion as the live path", got)
	}
}

// The reactions Mattermost returns must decode into the type the backfill reads.
func TestReactionDecodesDeleteAt(t *testing.T) {
	var r mattermost.Reaction
	if err := json.Unmarshal([]byte(`{"user_id":"u","post_id":"p","emoji_name":"joy","create_at":1,"delete_at":7}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.DeleteAt != 7 || r.EmojiName != "joy" {
		t.Fatalf("decoded %+v", r)
	}
}

// The helper is only useful if the backfill actually attaches its result to the
// message. This drives the real FetchMessages against a fake Mattermost.
func TestFetchMessagesAttachesReactionsToBackfilledMessages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"order": ["p2", "p1"],
			"posts": {
				"p1": {"id": "p1", "user_id": "alice", "channel_id": "chan", "create_at": 1000, "message": "hello",
					"metadata": {"reactions": [
						{"user_id": "bob", "post_id": "p1", "emoji_name": "thumbsup", "create_at": 2000},
						{"user_id": "me",  "post_id": "p1", "emoji_name": "heart",    "create_at": 3000}]}},
				"p2": {"id": "p2", "user_id": "bob", "channel_id": "chan", "create_at": 4000, "message": "no reactions here"}
			}
		}`))
	}))
	defer srv.Close()

	c := reactionClient()
	c.Token = "tok"
	c.baseURL = srv.URL
	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "chan"}, MXID: "!room:test"}},
		Count:  50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(resp.Messages))
	}
	byID := map[networkid.MessageID]*bridgev2.BackfillMessage{}
	for _, m := range resp.Messages {
		byID[m.ID] = m
	}
	if got := len(byID["p1"].Reactions); got != 2 {
		t.Fatalf("p1 carries %d reactions, want 2", got)
	}
	if byID["p1"].Reactions[0].Sender.Sender != "bob" || !byID["p1"].Reactions[1].Sender.IsFromMe {
		t.Errorf("reaction senders = %+v, %+v", byID["p1"].Reactions[0].Sender, byID["p1"].Reactions[1].Sender)
	}
	if got := len(byID["p2"].Reactions); got != 0 {
		t.Errorf("p2 carries %d reactions, want none", got)
	}
}
