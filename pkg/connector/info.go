package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// GetChatInfo returns Matrix room metadata for a Mattermost channel.
func (c *MattermostClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	chID := string(portal.ID)

	c.mu.RLock()
	ch, ok := c.channels[chID]
	c.mu.RUnlock()

	// Always fetch fresh data so we have up-to-date display name + type.
	if fresh, err := mattermost.GetChannel(c.serverURL(), c.Token, chID); err == nil {
		ch = fresh
		ok = true
		c.mu.Lock()
		if c.channels == nil {
			c.channels = make(map[string]*mattermost.Channel)
		}
		c.channels[chID] = ch
		c.mu.Unlock()
	}

	roomType := database.RoomTypeDefault
	var name *string

	if ok {
		switch {
		case ch.IsDM():
			roomType = database.RoomTypeDM
		case ch.IsGroupDM():
			roomType = database.RoomTypeGroupDM
		}

		if ch.DisplayName != "" {
			n := ch.DisplayName
			name = &n
		}
	}

	// Fallback name.
	if name == nil {
		n := chID
		name = &n
	}

	// Build member map.
	memberMap := make(bridgev2.ChatMemberMap)
	if c.UserID != "" {
		memberMap[networkid.UserID(c.UserID)] = bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{
				Sender:      networkid.UserID(c.UserID),
				IsFromMe:    true,
				SenderLogin: c.UserLogin.ID,
			},
			Membership: event.MembershipJoin,
		}
	}

	var otherUserID networkid.UserID

	if ok && ch.IsDM() {
		otherID := dmOtherUserID(ch.Name, c.UserID)
		if otherID != "" {
			otherUserID = networkid.UserID(otherID)
			memberMap[networkid.UserID(otherID)] = bridgev2.ChatMember{
				EventSender: bridgev2.EventSender{Sender: networkid.UserID(otherID)},
				Membership:  event.MembershipJoin,
			}
			// Use the other user's display name if the channel has no explicit display name.
			if name == nil || *name == chID {
				c.mu.RLock()
				u, hasUser := c.users[otherID]
				c.mu.RUnlock()
				if hasUser {
					n := u.DisplayName()
					name = &n
				}
			}
		}
	} else if ok && !ch.IsDM() && !ch.IsGroupDM() {
		// For team channels, fetch member list for a more complete picture.
		// This is best-effort; failures are non-fatal.
		members, err := mattermost.GetChannelMembers(c.serverURL(), c.Token, chID)
		if err == nil {
			for _, m := range members {
				if m.UserID == c.UserID {
					continue
				}
				memberMap[networkid.UserID(m.UserID)] = bridgev2.ChatMember{
					EventSender: bridgev2.EventSender{Sender: networkid.UserID(m.UserID)},
					Membership:  event.MembershipJoin,
				}
			}
		}
	}

	isFull := ok && ch.IsDM()
	chatMembers := &bridgev2.ChatMemberList{
		IsFull:      isFull,
		MemberMap:   memberMap,
		OtherUserID: otherUserID,
	}

	return &bridgev2.ChatInfo{
		Name:    name,
		Type:    &roomType,
		Members: chatMembers,
	}, nil
}

// GetUserInfo returns Matrix ghost metadata for a Mattermost user.
func (c *MattermostClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	userID := string(ghost.ID)

	c.mu.RLock()
	u, ok := c.users[userID]
	c.mu.RUnlock()

	if !ok {
		fetched, err := mattermost.GetUser(c.serverURL(), c.Token, userID)
		if err != nil {
			return &bridgev2.UserInfo{}, nil
		}
		u = fetched
		c.mu.Lock()
		if c.users == nil {
			c.users = make(map[string]*mattermost.User)
		}
		c.users[userID] = u
		c.mu.Unlock()
	}

	name := u.DisplayName()
	info := &bridgev2.UserInfo{
		Name: &name,
	}

	// Profile picture: fetched via the authenticated image endpoint.
	avatarURL := mattermost.ProfileImageURL(c.serverURL(), userID)
	token := c.Token
	info.Avatar = &bridgev2.Avatar{
		ID: networkid.AvatarID(userID + "_avatar"),
		Get: func(ctx context.Context) ([]byte, error) {
			return fetchImage(ctx, avatarURL, token)
		},
	}

	return info, nil
}
