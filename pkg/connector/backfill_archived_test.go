package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func fetchOwnHistory(t *testing.T, archived bool) *bridgev2.FetchMessagesResponse {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"order": ["p2", "p1"],
			"posts": {
				"p1": {"id": "p1", "user_id": "me", "channel_id": "chan", "create_at": 1000, "message": "mine",
					"metadata": {"reactions": [{"user_id": "me", "post_id": "p1", "emoji_name": "heart", "create_at": 2000}]}},
				"p2": {"id": "p2", "user_id": "alice", "channel_id": "chan", "create_at": 4000, "message": "hers"}
			}
		}`))
	}))
	t.Cleanup(srv.Close)
	c := reactionClient()
	c.Token, c.baseURL = "tok", srv.URL
	ch := &mattermost.Channel{ID: "chan"}
	if archived {
		ch.DeleteAt = 99
	}
	c.channels = map[string]*mattermost.Channel{"chan": ch}
	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "chan"}, MXID: "!room:test"}},
		Count:  50,
	})
	if err != nil || len(resp.Messages) != 2 {
		t.Fatalf("FetchMessages = %+v, %v", resp, err)
	}
	return resp
}

// In an archived room the user's own history is sent by their ghost, since
// their Matrix account is read-only there. Nothing is dropped, and the rest of
// the batch is unchanged.
func TestBackfillOwnHistoryInArchivedRoomUsesGhost(t *testing.T) {
	resp := fetchOwnHistory(t, true)
	mine := resp.Messages[0]
	if mine.ID != "p1" || mine.Sender.IsFromMe || mine.Sender.Sender != "me" || mine.Sender.SenderLogin != "" {
		t.Fatalf("own message sender = %+v, want the user's ghost", mine.Sender)
	}
	if len(mine.Reactions) != 1 || mine.Reactions[0].Sender.IsFromMe || mine.Reactions[0].Sender.Sender != "me" {
		t.Fatalf("own reaction = %+v, want the user's ghost", mine.Reactions)
	}
	if resp.Messages[1].Sender.Sender != "alice" || resp.Messages[1].Sender.IsFromMe {
		t.Fatalf("other author = %+v", resp.Messages[1].Sender)
	}
}

func TestBackfillOwnHistoryInActiveRoomIsStillFromMe(t *testing.T) {
	resp := fetchOwnHistory(t, false)
	mine := resp.Messages[0]
	if !mine.Sender.IsFromMe || mine.Sender.SenderLogin == "" || !mine.Reactions[0].Sender.IsFromMe {
		t.Fatalf("active room must keep sending own history as the user: %+v", mine)
	}
}

// "Everything here is mine" decides whether the batch marks the room read. It
// is about who wrote the posts, not which identity sends them.
func TestBackfillMarkReadIgnoresArchivedSenderIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"order":["p1"],"posts":{"p1":{"id":"p1","user_id":"me","channel_id":"chan","create_at":1000,"message":"mine"}}}`))
	}))
	defer srv.Close()
	for _, archived := range []bool{false, true} {
		c := reactionClient()
		c.Token, c.baseURL = "tok", srv.URL
		c.channels = map[string]*mattermost.Channel{"chan": {ID: "chan"}}
		if archived {
			c.channels["chan"].DeleteAt = 5
		}
		resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
			Portal: &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "chan"}}}, Count: 10,
		})
		if err != nil || !resp.MarkRead {
			t.Fatalf("archived=%v: MarkRead=%v err=%v, want true for all-own batch", archived, resp != nil && resp.MarkRead, err)
		}
	}
}

func TestRaiseGhostsLiftsOnlyGhostsBelowTheArchivedLevel(t *testing.T) {
	const bot = id.UserID("@bridgebot:example.org")
	pl := &event.PowerLevelsEventContent{Users: map[id.UserID]int{bot: 100, "@mm_high:example.org": 75}}
	ghosts := []id.UserID{"@mm_left:example.org", "@mm_high:example.org", ""}
	if !raiseGhosts(pl, bot, ghosts) {
		t.Fatal("expected a change")
	}
	if got := pl.GetUserLevel("@mm_left:example.org"); got != archivedChannelEventLevel {
		t.Fatalf("departed author level = %d", got)
	}
	if got := pl.GetUserLevel("@mm_high:example.org"); got != 75 {
		t.Fatalf("higher level was changed to %d", got)
	}
	if raiseGhosts(pl, bot, ghosts) {
		t.Fatal("second call must not change anything, so no extra state event is sent")
	}
	// A Matrix user at 0 stays unable to send.
	pl.EventsDefault = archivedChannelEventLevel
	if pl.GetUserLevel("@alice:example.org") >= pl.GetEventLevel(event.EventMessage) {
		t.Fatal("ordinary Matrix user can post")
	}
}
