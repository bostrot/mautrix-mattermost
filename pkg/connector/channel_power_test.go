package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// channelPowerClient serves one channel over REST and stubs the member
// lookups, so tests exercise GetChatInfo and channelResyncEvent without a
// live Mattermost.
func channelPowerClient(t *testing.T, cached *mattermost.Channel, status int, body any) *MattermostClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/channels/c1" {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}))
	t.Cleanup(srv.Close)
	c := &MattermostClient{
		UserID:    "me",
		Token:     "fixture",
		baseURL:   srv.URL,
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
		channelMembersFn: func(string) ([]*mattermost.ChannelMember, error) {
			return nil, nil
		},
		usersByIDsFn: func([]string) ([]*mattermost.User, error) {
			return nil, nil
		},
	}
	if cached != nil {
		c.channels = map[string]*mattermost.Channel{cached.ID: cached}
	}
	return c
}

// wantChannelPower asserts the user-visible outcome: applying the room's
// overrides leaves message events at the given level for an ordinary user.
func wantChannelPower(t *testing.T, info *bridgev2.ChatInfo, want int) {
	t.Helper()
	if info == nil || info.Members == nil || info.Members.PowerLevels == nil {
		t.Fatalf("no power level overrides in %+v", info)
	}
	pl := &event.PowerLevelsEventContent{}
	info.Members.PowerLevels.Apply("", pl)
	if pl.EventsDefault != want {
		t.Fatalf("events_default = %d, want %d", pl.EventsDefault, want)
	}
	for _, et := range []event.Type{event.EventMessage, event.EventEncrypted} {
		if got := pl.GetEventLevel(et); got != want {
			t.Errorf("%s level = %d, want %d", et, got, want)
		}
	}
	if pl.UsersDefault != 0 {
		t.Fatalf("users_default = %d, want 0", pl.UsersDefault)
	}
}

func wantParent(t *testing.T, info *bridgev2.ChatInfo, want string) {
	t.Helper()
	if (info.ParentID == nil) != (want == "") || (info.ParentID != nil && string(*info.ParentID) != want) {
		t.Fatalf("parent = %v, want %q", info.ParentID, want)
	}
}

func TestGetChatInfoChannelPowerFollowsDeleteAt(t *testing.T) {
	channel := func(deleteAt int64) *mattermost.Channel {
		return &mattermost.Channel{ID: "c1", TeamID: "t1", Type: "O", DisplayName: "Town Square", DeleteAt: deleteAt}
	}
	for _, tc := range []struct {
		name   string
		cached *mattermost.Channel
		status int
		fresh  *mattermost.Channel
		want   int
		parent string
		known  bool
	}{
		{name: "active", cached: channel(0), status: 200, fresh: channel(0), want: 0, parent: "team:t1", known: true},
		{name: "archived", cached: channel(0), status: 200, fresh: channel(123), want: 50, parent: "team:t1", known: true},
		{name: "restored", cached: channel(123), status: 200, fresh: channel(0), want: 0, parent: "team:t1", known: true},
		{name: "archived but fetch failed", cached: channel(123), status: 500, want: 50, parent: "team:t1", known: true},
		{name: "archived direct message", cached: nil, status: 200, fresh: &mattermost.Channel{ID: "c1", Type: "D", Name: "me__alice", DeleteAt: 123}, want: 50, known: true},
		{name: "unknown channel", cached: nil, status: 404, want: 0, known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body any
			if tc.fresh != nil {
				body = tc.fresh
			}
			c := channelPowerClient(t, tc.cached, tc.status, body)
			info, err := c.GetChatInfo(context.Background(), &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "c1", Receiver: "login"}}})
			if err != nil {
				t.Fatalf("GetChatInfo: %v", err)
			}
			if !tc.known {
				if info.Members != nil && info.Members.PowerLevels != nil {
					t.Fatalf("unknown channel got overrides: %+v", info.Members.PowerLevels)
				}
				return
			}
			wantChannelPower(t, info, tc.want)
			wantParent(t, info, tc.parent)
			if tc.want == 50 && info.Members.PowerLevels.Events == nil {
				t.Fatal("archived room relies on events_default alone; an explicit per-event level could still allow posting")
			}
		})
	}
}

func TestChannelResyncEventChannelPowerAtCreation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deleteAt int64
		want     int
	}{
		{name: "active at creation", deleteAt: 0, want: 0},
		{name: "archived at creation", deleteAt: 123, want: 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := channelPowerClient(t, nil, 200, nil)
			ch := &mattermost.Channel{ID: "c1", TeamID: "t1", Type: "O", DisplayName: "Town Square", DeleteAt: tc.deleteAt}
			ev := c.channelResyncEvent(ch)
			if !ev.CreatePortal {
				t.Fatal("channel resync must be able to create the room")
			}
			info, err := ev.GetChatInfo(context.Background(), nil)
			if err != nil {
				t.Fatalf("GetChatInfo: %v", err)
			}
			wantChannelPower(t, info, tc.want)
			wantParent(t, info, "team:t1")
		})
	}
}
