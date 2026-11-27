package connector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// One Matrix space per Mattermost team, as the Slack bridge does per
// workspace. A team's channels are children of its space, so two teams'
// "Town Square" rooms are told apart by the space they are in. The space
// carries the team's name and icon. DMs and group DMs belong to no team and
// stay outside the team spaces.
//
// A space is a portal whose ID is teamPortalPrefix + team ID. Mattermost IDs
// never contain ':', so it cannot collide with a channel ID. Code that treats
// a portal ID as a channel ID must skip space portals (isTeamPortal).

const teamPortalPrefix = "team:"

func teamPortalID(teamID string) networkid.PortalID {
	return networkid.PortalID(teamPortalPrefix + teamID)
}

// teamIDFromPortal returns the team ID of a team space portal ID.
func teamIDFromPortal(id networkid.PortalID) (string, bool) {
	s := string(id)
	if !strings.HasPrefix(s, teamPortalPrefix) || len(s) == len(teamPortalPrefix) {
		return "", false
	}
	return s[len(teamPortalPrefix):], true
}

func isTeamPortal(id networkid.PortalID) bool {
	_, ok := teamIDFromPortal(id)
	return ok
}

// channelParent is the space a channel belongs to, or nil for DMs and group
// DMs.
func channelParent(ch *mattermost.Channel) *networkid.PortalID {
	if ch == nil || ch.TeamID == "" || ch.IsDM() || ch.IsGroupDM() {
		return nil
	}
	return ptr.Ptr(teamPortalID(ch.TeamID))
}

// teamInfo is a team space's room info: the team's name and icon, the user
// as the only member, and nobody but the bridge allowed to post.
func (c *MattermostClient) teamInfo(team *mattermost.Team) *bridgev2.ChatInfo {
	name := team.DisplayName
	if name == "" {
		name = team.Name
	}
	self := bridgev2.EventSender{IsFromMe: true, SenderLogin: c.UserLogin.ID, Sender: networkid.UserID(c.UserID)}
	return &bridgev2.ChatInfo{
		Name:   &name,
		Avatar: c.teamAvatar(team),
		Type:   ptr.Ptr(database.RoomTypeSpace),
		Members: &bridgev2.ChatMemberList{
			MemberMap:   map[networkid.UserID]bridgev2.ChatMember{self.Sender: {EventSender: self}},
			PowerLevels: &bridgev2.PowerLevelOverrides{EventsDefault: ptr.Ptr(100)},
		},
	}
}

// teamAvatar is the team icon. Its ID changes with the icon, so a new icon
// replaces the old one and an unchanged icon is not fetched again.
func (c *MattermostClient) teamAvatar(team *mattermost.Team) *bridgev2.Avatar {
	if team.LastTeamIconUpdate <= 0 {
		return &bridgev2.Avatar{Remove: true}
	}
	url := mattermost.TeamImageURL(c.serverURL(), team.ID)
	token := c.Token
	return &bridgev2.Avatar{
		ID: networkid.AvatarID("team_" + team.ID + "_" + strconv.FormatInt(team.LastTeamIconUpdate, 10)),
		Get: func(ctx context.Context) ([]byte, error) {
			return fetchImage(ctx, url, token)
		},
	}
}

// teamChatInfo answers GetChatInfo for a team space portal.
func (c *MattermostClient) teamChatInfo(teamID string) (*bridgev2.ChatInfo, error) {
	var team *mattermost.Team
	var err error
	if c.getTeamFn != nil {
		team, err = c.getTeamFn(teamID)
	} else {
		team, err = mattermost.GetTeam(c.serverURL(), c.Token, teamID)
	}
	if err != nil {
		return nil, err
	}
	return c.teamInfo(team), nil
}

// teamPortalKey is the key of a team's space. Its receiver matches the one the
// framework uses to look up channel parents.
func (c *MattermostClient) teamPortalKey(teamID string) networkid.PortalKey {
	key := networkid.PortalKey{ID: teamPortalID(teamID)}
	if br := c.UserLogin.Bridge; br != nil && br.Config != nil && br.Config.SplitPortals {
		key.Receiver = c.UserLogin.ID
	}
	return key
}

// queueTeamResync creates or updates a team's space (name and icon).
func (c *MattermostClient) queueTeamResync(team *mattermost.Team) {
	key := c.teamPortalKey(team.ID)
	c.queueTeamEvent(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			CreatePortal: true,
			PortalKey:    key,
		},
		ChatInfo: c.teamInfo(team),
	})
}

func (c *MattermostClient) queueTeamEvent(ev bridgev2.RemoteEvent) {
	if c.queueTeamFn != nil {
		c.queueTeamFn(ev)
		return
	}
	c.UserLogin.QueueRemoteEvent(ev)
}

// handleLeaveTeam removes a team's space once the logged-in user is no longer
// in the team. The space is deleted for this login only, so another login
// that shares the space keeps it. The team's channel rooms are left alone, as
// they are for any channel the user leaves. Rejoining the team recreates the
// space the next time one of its channels is synced. Other members leaving
// are ignored: the server announces those to the whole team.
func (c *MattermostClient) handleLeaveTeam(evt *mattermost.LeaveTeamEvent) {
	if evt == nil || evt.TeamID == "" || evt.UserID != c.UserID {
		return
	}
	key := c.teamPortalKey(evt.TeamID)
	// The space row cannot be deleted while the team's channel portals still
	// point at it through the parent key: the delete fails and the space stays.
	// Detaching first clears only the parent link, so the channel rooms survive.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := c.detachSpaceChildren(ctx, key)
	cancel()
	if err != nil && c.UserLogin != nil {
		c.UserLogin.Log.Warn().Err(err).Str("team_id", evt.TeamID).
			Msg("Failed to detach channels from the leaving team's space")
	}
	c.queueTeamEvent(&simplevent.ChatDelete{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatDelete,
			PortalKey: key,
		},
		OnlyForMe: true,
	})
}

// detachSpaceChildren clears the parent link of every portal under a space, so
// the space can be deleted without taking the channel rooms with it.
func (c *MattermostClient) detachSpaceChildren(ctx context.Context, key networkid.PortalKey) error {
	if c.detachChildrenFn != nil {
		return c.detachChildrenFn(ctx, key)
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.DB == nil {
		return nil
	}
	return detachChildren(ctx, c.UserLogin.Bridge.DB.Portal, key)
}

// portalStore is the part of the portal table that detaching needs.
type portalStore interface {
	GetChildren(ctx context.Context, parentKey networkid.PortalKey) ([]*database.Portal, error)
	Update(ctx context.Context, p *database.Portal) error
}

func detachChildren(ctx context.Context, store portalStore, key networkid.PortalKey) error {
	children, err := store.GetChildren(ctx, key)
	if err != nil {
		return err
	}
	for _, child := range children {
		if child.ParentKey != key {
			continue
		}
		child.ParentKey = networkid.PortalKey{}
		if err := store.Update(ctx, child); err != nil {
			return fmt.Errorf("detach %s: %w", child.ID, err)
		}
	}
	return nil
}
