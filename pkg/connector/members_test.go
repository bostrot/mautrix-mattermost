package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func memberClient(members map[string][]string, fail bool) *MattermostClient {
	c := &MattermostClient{
		nameFormat: mattermost.NameFormatNicknameFullName,
		Token:      "tok", UserID: "me", baseURL: "http://unused.invalid",
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
		channelMembersFn: func(channelID string) ([]*mattermost.ChannelMember, error) {
			if fail {
				return nil, errors.New("boom")
			}
			var out []*mattermost.ChannelMember
			for _, u := range members[channelID] {
				out = append(out, &mattermost.ChannelMember{ChannelID: channelID, UserID: u})
			}
			return out, nil
		},
	}
	return c
}

func memberIDs(l *bridgev2.ChatMemberList) map[string]bool {
	out := map[string]bool{}
	for id, m := range l.MemberMap {
		if m.Membership != event.MembershipJoin {
			continue
		}
		out[string(id)] = true
	}
	return out
}

func TestChatMembers(t *testing.T) {
	ctx := context.Background()
	channel := &mattermost.Channel{ID: "c1", Type: "O", Name: "off-topic"}
	group := &mattermost.Channel{ID: "g1", Type: "G", Name: "group"}
	const meID, otherID = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
	dm := &mattermost.Channel{ID: "d1", Type: "D", Name: meID + "__" + otherID}
	c := memberClient(map[string][]string{"c1": {"me", "erin", "frank"}, "g1": {"me", "a", "b"}}, false)

	got := c.chatMembers(ctx, channel)
	if fmt.Sprint(memberIDs(got)) != fmt.Sprint(map[string]bool{"me": true, "erin": true, "frank": true}) {
		t.Fatalf("channel members = %v", memberIDs(got))
	}
	if !got.IsFull || !got.ExcludeChangesFromTimeline {
		t.Fatalf("channel list IsFull=%v Exclude=%v, want both true", got.IsFull, got.ExcludeChangesFromTimeline)
	}
	if !got.MemberMap["me"].IsFromMe || got.MemberMap["me"].SenderLogin != "login" {
		t.Fatalf("own entry is not marked as the logged-in user: %+v", got.MemberMap["me"])
	}

	if g := memberIDs(c.chatMembers(ctx, group)); len(g) != 3 || !g["a"] || !g["b"] {
		t.Fatalf("group DM members = %v", g)
	}

	c.UserID = meID
	c.usersByIDsFn = func(ids []string) ([]*mattermost.User, error) {
		return []*mattermost.User{{ID: otherID, Username: "gh", FirstName: "GitHub"}}, nil
	}
	d := c.chatMembers(ctx, dm)
	if !d.IsFull || d.OtherUserID != otherID || len(memberIDs(d)) != 2 {
		t.Fatalf("DM members = %v other=%q full=%v", memberIDs(d), d.OtherUserID, d.IsFull)
	}
	if info := d.MemberMap[otherID].UserInfo; info == nil || *info.Name != "GitHub" {
		t.Fatalf("DM partner carries no current profile: %+v", info)
	}
}

func TestChatMembersFetchFailureRemovesNobody(t *testing.T) {
	c := memberClient(nil, true)
	got := c.chatMembers(context.Background(), &mattermost.Channel{ID: "c1", Type: "O"})
	if got.IsFull {
		t.Fatal("a failed fetch must not be treated as the full list (it would remove everyone)")
	}
	if ids := memberIDs(got); len(ids) != 1 || !ids["me"] {
		t.Fatalf("members after failure = %v, want only the logged-in user", ids)
	}
}

func TestChatMembersTruncatedPageIsNotFull(t *testing.T) {
	var many []string
	for i := 0; i < channelMembersPageSize; i++ {
		many = append(many, fmt.Sprintf("u%d", i))
	}
	c := memberClient(map[string][]string{"big": many}, false)
	got := c.chatMembers(context.Background(), &mattermost.Channel{ID: "big", Type: "O"})
	if got.IsFull {
		t.Fatal("a page-sized response may be truncated and must not remove members")
	}
	if len(memberIDs(got)) != channelMembersPageSize+1 {
		t.Fatalf("got %d members", len(memberIDs(got)))
	}
}

func TestChannelResyncCarriesMembers(t *testing.T) {
	c := memberClient(map[string][]string{"c1": {"me", "erin"}}, false)
	rs := c.channelResyncEvent(&mattermost.Channel{ID: "c1", Type: "O", DisplayName: "Off-Topic"})
	if !rs.CreatePortal || rs.PortalKey != (networkid.PortalKey{ID: "c1", Receiver: "login"}) {
		t.Fatalf("resync targets %+v (create=%v)", rs.PortalKey, rs.CreatePortal)
	}
	info, err := rs.GetChatInfo(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if ids := memberIDs(info.Members); !ids["erin"] || !ids["me"] {
		t.Fatalf("resync members = %v, want the channel's members", ids)
	}
	if info.Name == nil || *info.Name != "Off-Topic" || !info.CanBackfill {
		t.Fatalf("resync lost its name or backfill flag: %+v", info)
	}
}

func TestOwnMembershipEvents(t *testing.T) {
	var fetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v4/channels/c1" {
			fetches.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(mattermost.Channel{ID: "c1", Type: "O", DisplayName: "New"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := memberClient(map[string][]string{"c1": {"me"}}, false)
	c.baseURL = srv.URL

	// Being removed must not resync (and so re-create) the channel's room.
	c.handleChannelMemberEvent(&mattermost.ChannelMemberEvent{ChannelID: "c1", UserID: "me", Added: false})
	time.Sleep(200 * time.Millisecond)
	if n := fetches.Load(); n != 0 {
		t.Fatalf("own removal fetched the channel %d times, want none", n)
	}

	// Being added builds the resync that creates the room.
	rs := c.joinedChannelResync("c1")
	if rs == nil || rs.PortalKey.ID != "c1" || !rs.CreatePortal {
		t.Fatalf("own addition resync = %+v", rs)
	}
	if c.joinedChannelResync("missing") != nil {
		t.Fatal("a channel that can't be fetched must not produce a resync")
	}
}

func TestLiveMemberEvents(t *testing.T) {
	c := memberClient(nil, false)
	want := []struct {
		user  string
		added bool
		m     event.Membership
	}{{"erin", true, event.MembershipJoin}, {"frank", false, event.MembershipLeave}}
	for _, w := range want {
		ev := c.memberChangeEvent(&mattermost.ChannelMemberEvent{ChannelID: "c1", UserID: w.user, Added: w.added})
		if ev.PortalKey != (networkid.PortalKey{ID: "c1", Receiver: "login"}) {
			t.Fatalf("%s: portal = %+v", w.user, ev.PortalKey)
		}
		mc := ev.ChatInfoChange.MemberChanges
		if mc == nil || mc.IsFull || mc.ExcludeChangesFromTimeline {
			t.Fatalf("%s: member changes = %+v; live changes must be partial and visible", w.user, mc)
		}
		if m := mc.MemberMap[networkid.UserID(w.user)]; m.Membership != w.m {
			t.Fatalf("%s membership = %q, want %q", w.user, m.Membership, w.m)
		}
	}
}

func TestChatMembersCarryProfiles(t *testing.T) {
	c := memberClient(map[string][]string{"c1": {"me", "erin", "frank"}}, false)
	var asked []string
	c.usersByIDsFn = func(ids []string) ([]*mattermost.User, error) {
		asked = ids
		return []*mattermost.User{
			{ID: "erin", Username: "erin", FirstName: "Erin"},
			{ID: "frank", Username: "frank", FirstName: "Frank", LastName: "Example"},
		}, nil
	}
	got := c.chatMembers(context.Background(), &mattermost.Channel{ID: "c1", Type: "O"})
	if fmt.Sprint(asked) != "[erin frank]" {
		t.Fatalf("profiles requested for %v, want the other members in one request", asked)
	}
	for uid, want := range map[string]string{"erin": "Erin", "frank": "Frank Example"} {
		info := got.MemberMap[networkid.UserID(uid)].UserInfo
		if info == nil || info.Name == nil || *info.Name != want {
			t.Fatalf("%s UserInfo = %+v, want name %q", uid, info, want)
		}
		if info.Avatar == nil {
			t.Fatalf("%s UserInfo has no avatar; it would stop the ghost's avatar from loading", uid)
		}
	}
	if got.MemberMap["me"].UserInfo != nil {
		t.Fatal("own entry must not carry a ghost profile")
	}
}

func TestChatMembersProfileFailureStillListsMembers(t *testing.T) {
	c := memberClient(map[string][]string{"c1": {"me", "erin"}}, false)
	c.usersByIDsFn = func([]string) ([]*mattermost.User, error) { return nil, errors.New("boom") }
	got := c.chatMembers(context.Background(), &mattermost.Channel{ID: "c1", Type: "O"})
	m, ok := got.MemberMap["erin"]
	if !ok || m.Membership != event.MembershipJoin {
		t.Fatalf("member dropped when profiles failed: %+v", got.MemberMap)
	}
	if m.UserInfo != nil {
		t.Fatalf("no profile was fetched, so no name should be pushed: %+v", m.UserInfo)
	}
	if !got.IsFull {
		t.Fatal("a profile failure must not change whether the member list is full")
	}
}

func TestUserUpdatedRefreshesGhost(t *testing.T) {
	c := memberClient(nil, false)
	got := make(chan *mattermost.User, 1)
	c.refreshGhostFn = func(u *mattermost.User) { got <- u }
	c.handleUserUpdated(&mattermost.User{ID: "erin", FirstName: "Erin", LastName: "Example"})
	select {
	case u := <-got:
		if u.ID != "erin" {
			t.Fatalf("refreshed %+v", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("user_updated did not refresh the ghost")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if u := c.users["erin"]; u == nil || u.LastName != "Example" {
		t.Fatal("user_updated did not update the cache")
	}
}

func TestUserInfoNameHasNoTrailingSpace(t *testing.T) {
	c := &MattermostClient{nameFormat: mattermost.NameFormatNicknameFullName}
	info := c.userInfo(&mattermost.User{ID: "u", Username: "erin", FirstName: "Erin"})
	if *info.Name != "Erin" {
		t.Fatalf("name = %q", *info.Name)
	}
}

// Ghosts must be able to post into an archived room (restore notice, catch-up
// backfill) while the logged-in user stays subject to the read-only overrides.
func TestChatMembersGhostPowerFollowsArchiveState(t *testing.T) {
	ctx := context.Background()
	const meID, otherID = "aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, tc := range []struct {
		name      string
		channel   *mattermost.Channel
		wantGhost int
	}{
		{"active channel", &mattermost.Channel{ID: "c1", Type: "O", Name: "town"}, 0},
		{"archived channel", &mattermost.Channel{ID: "c1", Type: "O", Name: "town", DeleteAt: 123}, archivedChannelEventLevel},
		{"archived group", &mattermost.Channel{ID: "c1", Type: "G", Name: "group", DeleteAt: 123}, archivedChannelEventLevel},
		{"dm", &mattermost.Channel{ID: "c1", Type: "D", Name: meID + "__" + otherID}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := memberClient(map[string][]string{"c1": {"me", "erin", "frank"}}, false)
			if tc.channel.IsDM() {
				c.UserID = meID
			}
			got := c.chatMembers(ctx, tc.channel)
			ghosts := 0
			for id, m := range got.MemberMap {
				if m.IsFromMe {
					if m.PowerLevel != nil {
						t.Errorf("logged-in user %s must not get an explicit level, got %d", id, *m.PowerLevel)
					}
					continue
				}
				ghosts++
				if m.PowerLevel == nil || *m.PowerLevel != tc.wantGhost {
					t.Errorf("ghost %s level=%v, want %d", id, m.PowerLevel, tc.wantGhost)
				}
			}
			if ghosts == 0 {
				t.Fatal("no ghost members to check")
			}
		})
	}
}

// The levels must hold through bridgev2's own Apply: ghosts at the archive
// level can post, the Matrix user at 0 cannot.
func TestArchivedRoomGhostsCanPostUserCannot(t *testing.T) {
	pl := &event.PowerLevelsEventContent{Users: map[id.UserID]int{"@bridge:example.org": 100}}
	overrides := channelPowerLevels(&mattermost.Channel{DeleteAt: 123})
	overrides.Apply("@bridge:example.org", pl)
	pl.EnsureUserLevelAs("@bridge:example.org", "@mm_erin:example.org", *ghostPowerLevel(&mattermost.Channel{DeleteAt: 123}))
	if got, need := pl.GetUserLevel("@mm_erin:example.org"), pl.GetEventLevel(event.EventMessage); got < need {
		t.Fatalf("ghost level %d cannot send (needs %d)", got, need)
	}
	if got, need := pl.GetUserLevel("@alice:example.org"), pl.GetEventLevel(event.EventMessage); got >= need {
		t.Fatalf("Matrix user level %d can send in archived room (needs %d)", got, need)
	}
	// Restore: ghost drops back to 0, as does the send requirement.
	active := &mattermost.Channel{}
	overrides = channelPowerLevels(active)
	overrides.Apply("@bridge:example.org", pl)
	pl.EnsureUserLevelAs("@bridge:example.org", "@mm_erin:example.org", *ghostPowerLevel(active))
	if pl.GetUserLevel("@mm_erin:example.org") != 0 || pl.GetEventLevel(event.EventMessage) != 0 {
		t.Fatal("restore did not return ghost and send levels to 0")
	}
}
