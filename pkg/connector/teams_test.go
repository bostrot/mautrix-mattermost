package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func TestTeamPortalIDs(t *testing.T) {
	if id, ok := teamIDFromPortal(teamPortalID("abc")); !ok || id != "abc" {
		t.Fatalf("round trip: %q %v", id, ok)
	}
	for _, p := range []networkid.PortalID{"abc", "team:", "", "teamabc", "xteam:abc"} {
		if isTeamPortal(p) {
			t.Errorf("%q treated as a team space", p)
		}
	}
}

func TestChannelParent(t *testing.T) {
	cases := []struct {
		ch   *mattermost.Channel
		want string
	}{
		{&mattermost.Channel{ID: "c", TeamID: "t1", Type: "O"}, "team:t1"},
		{&mattermost.Channel{ID: "c", TeamID: "t1", Type: "P"}, "team:t1"},
		{&mattermost.Channel{ID: "d", Type: "D", Name: "a__b"}, ""},
		{&mattermost.Channel{ID: "g", Type: "G"}, ""},
		{&mattermost.Channel{ID: "d2", TeamID: "t1", Type: "D", Name: "a__b"}, ""},
		{&mattermost.Channel{ID: "g2", TeamID: "t1", Type: "G"}, ""},
		{&mattermost.Channel{ID: "x", Type: "O"}, ""},
		{nil, ""},
	}
	for _, tc := range cases {
		got := channelParent(tc.ch)
		if (got == nil) != (tc.want == "") || (got != nil && string(*got) != tc.want) {
			t.Errorf("channelParent(%+v) = %v, want %q", tc.ch, got, tc.want)
		}
	}
}

func teamClient(srvURL string) *MattermostClient {
	return &MattermostClient{
		UserID:    "me",
		Token:     "tok",
		ServerURL: srvURL,
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
	}
}

func TestTeamSpaceInfoAndIcon(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		_, _ = w.Write([]byte("PNGDATA"))
	}))
	defer srv.Close()
	c := teamClient(srv.URL)
	info := c.teamInfo(&mattermost.Team{ID: "t1", DisplayName: "Team One", Name: "team-one", LastTeamIconUpdate: 42})
	if *info.Name != "Team One" || *info.Type != database.RoomTypeSpace {
		t.Fatalf("name/type = %q %v", *info.Name, *info.Type)
	}
	if info.Members == nil || len(info.Members.MemberMap) != 1 || info.Members.PowerLevels == nil || *info.Members.PowerLevels.EventsDefault != 100 {
		t.Fatalf("members/power = %+v", info.Members)
	}
	if m, ok := info.Members.MemberMap["me"]; !ok || !m.IsFromMe {
		t.Fatalf("self not the member: %+v", info.Members.MemberMap)
	}
	if info.Avatar == nil || info.Avatar.Remove || info.Avatar.ID != "team_t1_42" {
		t.Fatalf("avatar = %+v", info.Avatar)
	}
	data, err := info.Avatar.Get(context.Background())
	if err != nil || string(data) != "PNGDATA" || gotPath != "/api/v4/teams/t1/image" || gotAuth != "Bearer tok" {
		t.Fatalf("icon fetch: %q %v path=%s auth=%s", data, err, gotPath, gotAuth)
	}
	if c.teamAvatar(&mattermost.Team{ID: "t1", LastTeamIconUpdate: 43}).ID == info.Avatar.ID {
		t.Fatal("icon change keeps the avatar ID")
	}
	if a := c.teamAvatar(&mattermost.Team{ID: "t1"}); !a.Remove {
		t.Fatalf("no icon avatar = %+v", a)
	}
	if n := *c.teamInfo(&mattermost.Team{ID: "t2", Name: "handle"}).Name; n != "handle" {
		t.Fatalf("fallback name = %q", n)
	}
}

func TestGetChatInfoForTeamSpace(t *testing.T) {
	c := teamClient("http://unused")
	c.getTeamFn = func(id string) (*mattermost.Team, error) {
		return &mattermost.Team{ID: id, DisplayName: "Team " + id}, nil
	}
	info, err := c.GetChatInfo(context.Background(), &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: teamPortalID("t9")}}})
	if err != nil || *info.Name != "Team t9" || *info.Type != database.RoomTypeSpace {
		t.Fatalf("info = %+v err=%v", info, err)
	}
}

// syncServer serves two teams; t1 has a channel and a DM.
func syncServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/users/me/teams"):
			_ = json.NewEncoder(w).Encode([]*mattermost.Team{{ID: "t1", DisplayName: "One"}, {ID: "t2", DisplayName: "Two"}})
		case strings.HasSuffix(r.URL.Path, "/teams/t1/channels"):
			_ = json.NewEncoder(w).Encode([]*mattermost.Channel{
				{ID: "c1", TeamID: "t1", Type: "O", DisplayName: "Town Square"},
				{ID: "d1", Type: "D", Name: "me__other"},
			})
		default:
			_, _ = w.Write([]byte("[]"))
		}
	}))
}

func TestSyncCreatesTeamSpacesBeforeChannels(t *testing.T) {
	srv := syncServer()
	defer srv.Close()
	c := teamClient(srv.URL)
	var mu sync.Mutex
	var keys []string
	parents := map[string]string{}
	record := func(ev bridgev2.RemoteEvent) {
		mu.Lock()
		defer mu.Unlock()
		keys = append(keys, string(ev.GetPortalKey().ID))
		info, err := ev.(*simplevent.ChatResync).GetChatInfo(context.Background(), nil)
		if err != nil {
			t.Errorf("chat info for %v: %v", ev.GetPortalKey(), err)
			return
		}
		if info.ParentID != nil {
			parents[string(ev.GetPortalKey().ID)] = string(*info.ParentID)
		}
	}
	c.queueTeamFn = record
	c.queueChannelFn = record
	c.syncChannels()
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(keys, ",") != "team:t1,team:t2,c1,d1" {
		t.Fatalf("resyncs = %v, want both spaces before the channels", keys)
	}
	if parents["c1"] != "team:t1" || parents["d1"] != "" {
		t.Fatalf("parents = %v", parents)
	}
}

func TestTeamSpacesNeverTreatedAsChannels(t *testing.T) {
	space := &database.Portal{PortalKey: networkid.PortalKey{ID: teamPortalID("t1"), Receiver: "login"}, MXID: "!space"}
	c := teamClient("http://127.0.0.1:1") // any request would fail
	resp, err := c.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: &bridgev2.Portal{Portal: space}})
	if err != nil || resp == nil || len(resp.Messages) != 0 {
		t.Fatalf("space backfill = %+v %v", resp, err)
	}
	c.client = &mattermost.Client{} // not connected: sending typing would fail
	if err := c.HandleMatrixTyping(context.Background(), &bridgev2.MatrixTyping{Portal: &bridgev2.Portal{Portal: space}, IsTyping: true}); err != nil {
		t.Fatalf("typing in a space reached Mattermost: %v", err)
	}
}

func TestTeamUpdatesQueueSpaceResync(t *testing.T) {
	c := teamClient("http://unused")
	var got []bridgev2.RemoteEvent
	c.queueTeamFn = func(ev bridgev2.RemoteEvent) { got = append(got, ev) }
	feed := make(chan any, 1)
	c.client = &mattermost.Client{Events: feed}
	feed <- &mattermost.TeamUpdatedEvent{Team: &mattermost.Team{ID: "t1", DisplayName: "Renamed"}}
	close(feed)
	c.processEvents()
	if len(got) != 1 {
		t.Fatalf("queued %d events", len(got))
	}
	rs, ok := got[0].(*simplevent.ChatResync)
	if !ok || rs.PortalKey.ID != "team:t1" || !rs.CreatePortal || *rs.ChatInfo.Name != "Renamed" {
		t.Fatalf("event = %#v", got[0])
	}
}

func TestQueueTeamResyncPortalKey(t *testing.T) {
	for _, tc := range []struct {
		name     string
		split    bool
		receiver networkid.UserLoginID
	}{
		{name: "default"},
		{name: "split", split: true, receiver: "login"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := teamClient("http://unused")
			c.UserLogin.Bridge = &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{SplitPortals: tc.split}}
			var keys []networkid.PortalKey
			c.queueTeamFn = func(ev bridgev2.RemoteEvent) { keys = append(keys, ev.GetPortalKey()) }
			c.queueTeamResync(&mattermost.Team{ID: "t1"})
			want := networkid.PortalKey{ID: "team:t1", Receiver: tc.receiver}
			if len(keys) != 1 || keys[0] != want {
				t.Fatalf("queued keys = %+v, want [%+v]", keys, want)
			}
		})
	}
}
