package connector

import (
	"context"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// channelMembersPageSize is the page size GetChannelMembers requests. A
// response this long may be truncated, so it is not treated as the full list.
const channelMembersPageSize = 200

// chatMembers builds the Matrix member list for a Mattermost channel. Every
// room is built from this one function, so channel creation, resyncs and
// GetChatInfo agree.
//
//   - 1:1 DM: the logged-in user and the other participant.
//   - Channels and group DMs: the channel's Mattermost members. The list is
//     marked full (members who left are removed from the room) only when the
//     fetch succeeded and was not cut off at the page size. On failure only
//     the logged-in user is listed and nobody is removed.
//
// Membership found by a sync is excluded from the timeline, so resyncing a
// room does not post a "joined" line per member; live joins and leaves are
// handled separately and stay visible.
func (c *MattermostClient) chatMembers(ctx context.Context, ch *mattermost.Channel) *bridgev2.ChatMemberList {
	memberMap := make(bridgev2.ChatMemberMap)
	if c.UserID != "" {
		memberMap[networkid.UserID(c.UserID)] = c.selfMember()
	}
	list := &bridgev2.ChatMemberList{
		MemberMap:                  memberMap,
		ExcludeChangesFromTimeline: true,
	}
	if ch == nil {
		return list
	}

	if ch.IsDM() {
		if otherID := dmOtherUserID(ch.Name, c.UserID); otherID != "" {
			list.OtherUserID = networkid.UserID(otherID)
			member := joinedMember(otherID)
			if u := c.memberProfiles(ctx, []string{otherID})[otherID]; u != nil {
				member.UserInfo = c.userInfo(u)
			}
			memberMap[networkid.UserID(otherID)] = member
		}
		list.IsFull = true
		return list
	}

	members, err := c.fetchChannelMembers(ch.ID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("channel_id", ch.ID).
			Msg("Failed to fetch channel members; listing only the logged-in user")
		return list
	}
	var ids []string
	for _, m := range members {
		if m == nil || m.UserID == "" || m.UserID == c.UserID {
			continue
		}
		ids = append(ids, m.UserID)
	}
	// Carry each member's profile, so a sync also corrects ghost names that
	// were set from an older profile (bridgev2 only sets a ghost's name once
	// on its own). Without profiles the members are still listed.
	profiles := c.memberProfiles(ctx, ids)
	for _, uid := range ids {
		member := joinedMember(uid)
		if u := profiles[uid]; u != nil {
			member.UserInfo = c.userInfo(u)
		}
		memberMap[networkid.UserID(uid)] = member
	}
	list.IsFull = len(members) < channelMembersPageSize
	list.TotalMemberCount = len(members)
	return list
}

// memberProfiles fetches the profiles of the given users in one request and
// caches them. On failure it returns what is cached.
func (c *MattermostClient) memberProfiles(ctx context.Context, ids []string) map[string]*mattermost.User {
	out := make(map[string]*mattermost.User, len(ids))
	if len(ids) == 0 {
		return out
	}
	var fetched []*mattermost.User
	var err error
	if c.usersByIDsFn != nil {
		fetched, err = c.usersByIDsFn(ids)
	} else {
		fetched, err = mattermost.GetUsersByIDs(c.serverURL(), c.Token, ids)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.users == nil {
		c.users = make(map[string]*mattermost.User)
	}
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to fetch member profiles; ghost names are not refreshed")
	}
	for _, u := range fetched {
		if u != nil && u.ID != "" {
			c.users[u.ID] = u
		}
	}
	for _, id := range ids {
		if u := c.users[id]; u != nil {
			out[id] = u
		}
	}
	return out
}

// refreshGhost pushes a changed Mattermost profile to the user's ghost, if the
// bridge already has one.
func (c *MattermostClient) refreshGhost(ctx context.Context, u *mattermost.User) {
	if c.refreshGhostFn != nil {
		c.refreshGhostFn(u)
		return
	}
	if u == nil || c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.DB == nil {
		return
	}
	ghost, err := c.UserLogin.Bridge.GetExistingGhostByID(ctx, networkid.UserID(u.ID))
	if err != nil || ghost == nil {
		return
	}
	ghost.UpdateInfo(ctx, c.userInfo(u))
}

func (c *MattermostClient) fetchChannelMembers(channelID string) ([]*mattermost.ChannelMember, error) {
	if c.channelMembersFn != nil {
		return c.channelMembersFn(channelID)
	}
	return mattermost.GetChannelMembers(c.serverURL(), c.Token, channelID)
}

func (c *MattermostClient) selfMember() bridgev2.ChatMember {
	sender := bridgev2.EventSender{Sender: networkid.UserID(c.UserID), IsFromMe: true}
	if c.UserLogin != nil {
		sender.SenderLogin = c.UserLogin.ID
	}
	return bridgev2.ChatMember{EventSender: sender, Membership: event.MembershipJoin}
}

func joinedMember(userID string) bridgev2.ChatMember {
	return bridgev2.ChatMember{
		EventSender: bridgev2.EventSender{Sender: networkid.UserID(userID)},
		Membership:  event.MembershipJoin,
	}
}

// handleChannelMemberEvent mirrors a live Mattermost join or leave. When the
// logged-in user is added to a channel, the channel is (re)synced so its room
// exists; their own removal is left to the channel lifecycle handling.
func (c *MattermostClient) handleChannelMemberEvent(evt *mattermost.ChannelMemberEvent) {
	if evt.UserID == c.UserID {
		if evt.Added {
			go func() {
				if rs := c.joinedChannelResync(evt.ChannelID); rs != nil {
					c.UserLogin.QueueRemoteEvent(rs)
				}
			}()
		}
		return
	}
	c.UserLogin.QueueRemoteEvent(c.memberChangeEvent(evt))
}

// joinedChannelResync fetches a channel the logged-in user was just added to
// and returns the resync that creates its room, or nil if it can't be fetched.
func (c *MattermostClient) joinedChannelResync(channelID string) *simplevent.ChatResync {
	ch, err := mattermost.GetChannel(c.serverURL(), c.Token, channelID)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	if c.channels == nil {
		c.channels = make(map[string]*mattermost.Channel)
	}
	c.channels[ch.ID] = ch
	c.mu.Unlock()
	return c.channelResyncEvent(ch)
}

// memberChangeEvent builds the room update for another user's live join or
// leave.
func (c *MattermostClient) memberChangeEvent(evt *mattermost.ChannelMemberEvent) *simplevent.ChatInfoChange {
	membership := event.MembershipJoin
	if !evt.Added {
		membership = event.MembershipLeave
	}
	return &simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
			Sender:    bridgev2.EventSender{Sender: networkid.UserID(evt.UserID)},
			LogContext: func(z zerolog.Context) zerolog.Context {
				return z.Str("member_user_id", evt.UserID).Bool("added", evt.Added)
			},
		},
		ChatInfoChange: memberChange(evt.UserID, membership),
	}
}

// handleUserUpdated caches a changed Mattermost profile and pushes it to the
// user's ghost, so renames reach Matrix, not only the cache.
func (c *MattermostClient) handleUserUpdated(u *mattermost.User) {
	c.mu.Lock()
	if c.users == nil {
		c.users = make(map[string]*mattermost.User)
	}
	c.users[u.ID] = u
	c.mu.Unlock()
	ctx := context.Background()
	if c.UserLogin != nil {
		ctx = c.UserLogin.Log.WithContext(ctx)
	}
	go c.refreshGhost(ctx, u)
}

// memberChange is the ChatInfoChange for one live join or leave. It is shown
// in the timeline, unlike sync-time membership.
func memberChange(userID string, membership event.Membership) *bridgev2.ChatInfoChange {
	return &bridgev2.ChatInfoChange{
		MemberChanges: &bridgev2.ChatMemberList{
			MemberMap: bridgev2.ChatMemberMap{
				networkid.UserID(userID): {
					EventSender: bridgev2.EventSender{Sender: networkid.UserID(userID)},
					Membership:  membership,
				},
			},
		},
	}
}
