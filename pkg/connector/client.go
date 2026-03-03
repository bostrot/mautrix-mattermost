package connector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// MattermostClient implements bridgev2.NetworkAPI for a single Mattermost user login.
type MattermostClient struct {
	UserLogin *bridgev2.UserLogin
	Username  string
	Token     string
	ServerURL string
	UserID    string // Mattermost user ID of the logged-in user

	// baseURL may be overridden in tests to point at a mock server.
	baseURL string

	client   *mattermost.Client
	mu       sync.RWMutex
	channels map[string]*mattermost.Channel // keyed by channel ID
	users    map[string]*mattermost.User    // keyed by user ID
	done     chan struct{}                  // closed by Disconnect to stop reconnect loop
}

var _ bridgev2.NetworkAPI = (*MattermostClient)(nil)
var _ bridgev2.BackfillingNetworkAPI = (*MattermostClient)(nil)
var _ bridgev2.RedactionHandlingNetworkAPI = (*MattermostClient)(nil)
var _ bridgev2.ReactionHandlingNetworkAPI = (*MattermostClient)(nil)
var _ bridgev2.TypingHandlingNetworkAPI = (*MattermostClient)(nil)
var _ bridgev2.EditHandlingNetworkAPI = (*MattermostClient)(nil)

// mmTypingEvent is a minimal bridgev2.RemoteTyping implementation.
type mmTypingEvent struct {
	channelID networkid.PortalID
	receiver  networkid.UserLoginID
	senderID  string
}

func (e *mmTypingEvent) GetPortalKey() networkid.PortalKey {
	return networkid.PortalKey{ID: e.channelID, Receiver: e.receiver}
}
func (e *mmTypingEvent) AddLogContext(c zerolog.Context) zerolog.Context { return c }
func (e *mmTypingEvent) GetSender() bridgev2.EventSender {
	return bridgev2.EventSender{Sender: networkid.UserID(e.senderID)}
}
func (e *mmTypingEvent) GetType() bridgev2.RemoteEventType { return bridgev2.RemoteEventTyping }
func (e *mmTypingEvent) ShouldCreatePortal() bool          { return false }
func (e *mmTypingEvent) GetTimeout() time.Duration         { return 10 * time.Second }

// mmChatDeleteEvent fires when a Mattermost channel is deleted.
type mmChatDeleteEvent struct {
	portalKey networkid.PortalKey
}

func (e *mmChatDeleteEvent) GetType() bridgev2.RemoteEventType {
	return bridgev2.RemoteEventChatDelete
}
func (e *mmChatDeleteEvent) GetPortalKey() networkid.PortalKey               { return e.portalKey }
func (e *mmChatDeleteEvent) AddLogContext(c zerolog.Context) zerolog.Context { return c }
func (e *mmChatDeleteEvent) GetSender() bridgev2.EventSender                 { return bridgev2.EventSender{} }
func (e *mmChatDeleteEvent) DeleteOnlyForMe() bool                           { return false }

// serverURL returns the effective REST base URL (may be overridden in tests).
func (c *MattermostClient) serverURL() string {
	if c.baseURL != "" {
		return c.baseURL
	}
	return c.ServerURL
}

// queueChannelResync queues a ChatResync event for a single Mattermost channel.
func (c *MattermostClient) queueChannelResync(ch *mattermost.Channel) {
	roomType := database.RoomTypeDefault
	switch {
	case ch.IsDM():
		roomType = database.RoomTypeDM
	case ch.IsGroupDM():
		roomType = database.RoomTypeGroupDM
	}

	var name *string
	if ch.DisplayName != "" {
		n := ch.DisplayName
		name = &n
	}

	var otherUserID networkid.UserID
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

	// For DMs, resolve the other participant's display name and ID.
	if ch.IsDM() {
		otherID := dmOtherUserID(ch.Name, c.UserID)
		if otherID != "" {
			otherUserID = networkid.UserID(otherID)
			memberMap[networkid.UserID(otherID)] = bridgev2.ChatMember{
				EventSender: bridgev2.EventSender{Sender: networkid.UserID(otherID)},
				Membership:  event.MembershipJoin,
			}
			c.mu.RLock()
			if u, ok := c.users[otherID]; ok && name == nil {
				n := u.DisplayName()
				name = &n
			}
			c.mu.RUnlock()
		}
	}

	members := &bridgev2.ChatMemberList{
		IsFull:      ch.IsDM(),
		MemberMap:   memberMap,
		OtherUserID: otherUserID,
	}

	c.UserLogin.QueueRemoteEvent(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			CreatePortal: true,
			PortalKey:    networkid.PortalKey{ID: networkid.PortalID(ch.ID), Receiver: c.UserLogin.ID},
			LogContext:   func(z zerolog.Context) zerolog.Context { return z },
		},
		ChatInfo: &bridgev2.ChatInfo{
			Name:        name,
			Type:        &roomType,
			Members:     members,
			CanBackfill: true,
		},
		CheckNeedsBackfillFunc: func(_ context.Context, _ *database.Message) (bool, error) { return true, nil },
	})
}

func (c *MattermostClient) Connect(ctx context.Context) {
	if c.UserID == "" {
		id, _, err := mattermost.GetSelf(c.serverURL(), c.Token)
		if err == nil {
			c.UserID = id
		}
	}
	c.done = make(chan struct{})
	go c.connectLoop()
}

func (c *MattermostClient) connectLoop() {
	backoff := 5 * time.Second
	for {
		cl := mattermost.NewClient(c.serverURL(), c.Token)
		if err := cl.Connect(); err != nil {
			c.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateUnknownError,
				Error:      "mm-connect-failed",
				Message:    err.Error(),
			})
		} else {
			c.mu.Lock()
			c.client = cl
			c.mu.Unlock()
			c.processEvents() // blocks until WS closes
			backoff = 5 * time.Second
		}
		select {
		case <-c.done:
			return
		case <-time.After(backoff):
			if backoff < 5*time.Minute {
				backoff *= 2
			}
		}
	}
}

func (c *MattermostClient) processEvents() {
	for raw := range c.client.Events {
		switch evt := raw.(type) {
		case *mattermost.HelloEvent:
			// When connected, load all channels and queue resyncs.
			c.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateConnected,
			})
			go c.syncChannels()

		case *mattermost.PostedEvent:
			if evt.Post == nil {
				continue
			}
			// Pre-seed the channel cache from the inline WS metadata so that
			// GetChatInfo never races against a concurrent REST fetch.
			if evt.ChannelType != "" && evt.ChannelName != "" {
				c.mu.Lock()
				if c.channels == nil {
					c.channels = make(map[string]*mattermost.Channel)
				}
				if _, exists := c.channels[evt.ChannelID]; !exists {
					c.channels[evt.ChannelID] = &mattermost.Channel{
						ID:   evt.ChannelID,
						Type: evt.ChannelType,
						Name: evt.ChannelName,
					}
				}
				c.mu.Unlock()
			}
			isFromMe := c.UserID != "" && evt.Post.UserID == c.UserID
			sender := bridgev2.EventSender{
				Sender:   networkid.UserID(evt.Post.UserID),
				IsFromMe: isFromMe,
			}
			if isFromMe {
				sender.SenderLogin = c.UserLogin.ID
			}
			c.UserLogin.QueueRemoteEvent(&simplevent.Message[*mattermost.Post]{
				EventMeta: simplevent.EventMeta{
					Type:         bridgev2.RemoteEventMessage,
					CreatePortal: true,
					PortalKey:    networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
					Timestamp:    evt.Post.Timestamp(),
					Sender:       sender,
					LogContext:   func(z zerolog.Context) zerolog.Context { return z },
				},
				ID:   networkid.MessageID(evt.Post.ID),
				Data: evt.Post,
				ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data *mattermost.Post) (*bridgev2.ConvertedMessage, error) {
					return c.convertPost(ctx, intent, portal.MXID, data)
				},
			})

		case *mattermost.PostEditedEvent:
			if evt.Post == nil {
				continue
			}
			isFromMe := evt.Post.UserID == c.UserID
			editSender := bridgev2.EventSender{
				Sender:   networkid.UserID(evt.Post.UserID),
				IsFromMe: isFromMe,
			}
			if isFromMe {
				editSender.SenderLogin = c.UserLogin.ID
			}
			c.UserLogin.QueueRemoteEvent(&simplevent.Message[*mattermost.Post]{
				EventMeta: simplevent.EventMeta{
					Type:       bridgev2.RemoteEventEdit,
					PortalKey:  networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
					Timestamp:  evt.Post.Timestamp(),
					Sender:     editSender,
					LogContext: func(z zerolog.Context) zerolog.Context { return z },
				},
				TargetMessage: networkid.MessageID(evt.Post.ID),
				Data:          evt.Post,
				ConvertEditFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data *mattermost.Post) (*bridgev2.ConvertedEdit, error) {
					var parts []*bridgev2.ConvertedEditPart
					for _, dbMsg := range existing {
						content := &event.MessageEventContent{
							MsgType: event.MsgText,
							Body:    data.Message,
						}
						parts = append(parts, &bridgev2.ConvertedEditPart{
							Part:    dbMsg,
							Type:    event.EventMessage,
							Content: content,
						})
						break // only edit the first/text part
					}
					return &bridgev2.ConvertedEdit{ModifiedParts: parts}, nil
				},
			})

		case *mattermost.PostDeletedEvent:
			if evt.Post == nil {
				continue
			}
			c.UserLogin.QueueRemoteEvent(&simplevent.MessageRemove{
				EventMeta: simplevent.EventMeta{
					Type:       bridgev2.RemoteEventMessageRemove,
					PortalKey:  networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
					Sender:     bridgev2.EventSender{IsFromMe: false},
					LogContext: func(z zerolog.Context) zerolog.Context { return z },
				},
				TargetMessage: networkid.MessageID(evt.Post.ID),
			})

		case *mattermost.ReactionEvent:
			evtType := bridgev2.RemoteEventReaction
			if !evt.IsAdd {
				evtType = bridgev2.RemoteEventReactionRemove
			}
			c.UserLogin.QueueRemoteEvent(&simplevent.Reaction{
				EventMeta: simplevent.EventMeta{
					Type:       evtType,
					PortalKey:  networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
					Sender:     bridgev2.EventSender{Sender: networkid.UserID(evt.Reaction.UserID)},
					LogContext: func(z zerolog.Context) zerolog.Context { return z },
				},
				TargetMessage: networkid.MessageID(evt.Reaction.PostID),
				EmojiID:       networkid.EmojiID(evt.Reaction.EmojiName),
				Emoji:         evt.Reaction.EmojiName,
			})

		case *mattermost.TypingEvent:
			if evt.UserID == c.UserID {
				continue // don't echo own typing
			}
			portalKey := networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID}
			c.UserLogin.QueueRemoteEvent(&mmTypingEvent{
				channelID: networkid.PortalID(evt.ChannelID),
				receiver:  c.UserLogin.ID,
				senderID:  evt.UserID,
			})
			// Mark portal as read on behalf of the logged-in user when
			// someone else starts typing — they're clearly looking at it.
			c.UserLogin.QueueRemoteEvent(&simplevent.Receipt{
				EventMeta: simplevent.EventMeta{
					Type:       bridgev2.RemoteEventReadReceipt,
					PortalKey:  portalKey,
					Sender:     bridgev2.EventSender{IsFromMe: true, SenderLogin: c.UserLogin.ID},
					LogContext: func(z zerolog.Context) zerolog.Context { return z },
				},
				ReadUpTo: time.Now(),
			})

		case *mattermost.ChannelCreatedEvent:
			if evt.Channel == nil {
				continue
			}
			c.mu.Lock()
			if c.channels == nil {
				c.channels = make(map[string]*mattermost.Channel)
			}
			c.channels[evt.Channel.ID] = evt.Channel
			c.mu.Unlock()
			c.queueChannelResync(evt.Channel)

		case *mattermost.ChannelUpdatedEvent:
			if evt.Channel == nil {
				continue
			}
			c.mu.Lock()
			if c.channels != nil {
				c.channels[evt.Channel.ID] = evt.Channel
			}
			c.mu.Unlock()
			newName := evt.Channel.DisplayName
			c.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
				EventMeta: simplevent.EventMeta{
					Type:       bridgev2.RemoteEventChatInfoChange,
					PortalKey:  networkid.PortalKey{ID: networkid.PortalID(evt.Channel.ID), Receiver: c.UserLogin.ID},
					LogContext: func(z zerolog.Context) zerolog.Context { return z },
				},
				ChatInfoChange: &bridgev2.ChatInfoChange{
					ChatInfo: &bridgev2.ChatInfo{Name: &newName},
				},
			})

		case *mattermost.ChannelDeletedEvent:
			c.mu.Lock()
			if c.channels != nil {
				delete(c.channels, evt.ChannelID)
			}
			c.mu.Unlock()
			c.UserLogin.QueueRemoteEvent(&mmChatDeleteEvent{
				portalKey: networkid.PortalKey{ID: networkid.PortalID(evt.ChannelID), Receiver: c.UserLogin.ID},
			})

		case *mattermost.DirectAddedEvent:
			// A new DM was opened. Fetch channel info and queue a resync.
			go func(chanID string) {
				ch, err := mattermost.GetChannel(c.serverURL(), c.Token, chanID)
				if err != nil {
					return
				}
				c.mu.Lock()
				if c.channels == nil {
					c.channels = make(map[string]*mattermost.Channel)
				}
				c.channels[ch.ID] = ch
				c.mu.Unlock()
				c.queueChannelResync(ch)
			}(evt.ChannelID)

		case *mattermost.UserUpdatedEvent:
			if evt.User == nil {
				continue
			}
			c.mu.Lock()
			if c.users == nil {
				c.users = make(map[string]*mattermost.User)
			}
			c.users[evt.User.ID] = evt.User
			c.mu.Unlock()

		case *mattermost.AuthErrorEvent:
			c.UserLogin.BridgeState.Send(status.BridgeState{
				StateEvent: status.StateBadCredentials,
				Error:      "mm-auth-error",
				Message:    evt.Message,
			})
			go c.Disconnect()
		}
	}

	// Events channel closed — WebSocket disconnected.
	c.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateUnknownError,
		Error:      "mm-disconnected",
		Message:    "WebSocket connection closed",
	})
}

// syncChannels fetches all teams + channels for the user and queues ChatResync events.
func (c *MattermostClient) syncChannels() {
	teams, err := mattermost.GetTeams(c.serverURL(), c.Token, c.UserID)
	if err != nil {
		return
	}

	seenChannels := make(map[string]bool)

	for _, team := range teams {
		channels, err := mattermost.GetChannelsForUser(c.serverURL(), c.Token, c.UserID, team.ID)
		if err != nil {
			continue
		}

		c.mu.Lock()
		if c.channels == nil {
			c.channels = make(map[string]*mattermost.Channel, len(channels))
		}
		for _, ch := range channels {
			c.channels[ch.ID] = ch
		}
		c.mu.Unlock()

		for _, ch := range channels {
			if seenChannels[ch.ID] {
				continue // DMs appear in multiple teams
			}
			seenChannels[ch.ID] = true
			c.queueChannelResync(ch)
		}
	}
}

func (c *MattermostClient) Disconnect() {
	if c.done != nil {
		close(c.done)
	}
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl != nil {
		cl.Close()
	}
}

func (c *MattermostClient) IsLoggedIn() bool {
	return c.Token != ""
}

func (c *MattermostClient) LogoutRemote(ctx context.Context) {
	c.Disconnect()
}

func (c *MattermostClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	if c.UserID != "" {
		return string(userID) == c.UserID
	}
	return string(userID) == c.Username
}

// dmOtherUserID extracts the other participant's ID from a DM channel name.
// Mattermost DM channel names are "<userID1>__<userID2>".
func dmOtherUserID(channelName, ownUserID string) string {
	if len(channelName) < 54 { // 26 + 2 + 26 minimum
		return ""
	}
	const sep = "__"
	idx := len(channelName)/2 - 1
	// find the __ separator around the middle
	for i := 0; i < len(channelName)-1; i++ {
		if channelName[i] == '_' && channelName[i+1] == '_' {
			idx = i
			break
		}
	}
	if idx <= 0 || idx+2 >= len(channelName) {
		return ""
	}
	_ = sep
	part1 := channelName[:idx]
	part2 := channelName[idx+2:]
	if part1 == ownUserID {
		return part2
	}
	return part1
}

// resolveDisplayName returns a cached or freshly fetched display name for a Mattermost user.
func (c *MattermostClient) resolveDisplayName(userID string) string {
	c.mu.RLock()
	u, ok := c.users[userID]
	c.mu.RUnlock()
	if ok {
		return u.DisplayName()
	}
	// Fetch and cache.
	fetched, err := mattermost.GetUser(c.serverURL(), c.Token, userID)
	if err != nil {
		return userID
	}
	c.mu.Lock()
	if c.users == nil {
		c.users = make(map[string]*mattermost.User)
	}
	c.users[userID] = fetched
	c.mu.Unlock()
	return fetched.DisplayName()
}

// sendNotice queues a notice m.text event into a channel portal.
func (c *MattermostClient) sendNotice(channelID networkid.PortalID, body string) {
	c.UserLogin.QueueRemoteEvent(&simplevent.Message[string]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			CreatePortal: false,
			PortalKey:    networkid.PortalKey{ID: channelID, Receiver: c.UserLogin.ID},
			Timestamp:    time.Now(),
			Sender:       bridgev2.EventSender{},
			LogContext:   func(z zerolog.Context) zerolog.Context { return z },
		},
		ID:   networkid.MessageID(fmt.Sprintf("notice:%s:%d", channelID, time.Now().UnixMilli())),
		Data: body,
		ConvertMessageFunc: func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, text string) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				ID:   networkid.PartID(""),
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType: event.MsgNotice,
					Body:    text,
				},
			}}}, nil
		},
	})
}
