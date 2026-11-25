package connector

import (
	"context"
	"errors"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type channelLifecycleHint struct {
	channelID   string
	allowDelete bool
}

// A single worker keeps REST probes off the websocket event loop.
func (c *MattermostClient) runChannelLifecycle(ctx context.Context, hints <-chan channelLifecycleHint) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case hint := <-hints:
			c.handleChannelLifecycle(ctx, hint.channelID, hint.allowDelete, c.queueLifecycleEvent)
		}
	}
}

func (c *MattermostClient) queueLifecycleEvent(evt bridgev2.RemoteEvent) {
	c.UserLogin.QueueRemoteEvent(evt)
}

func (c *MattermostClient) handleChannelLifecycle(ctx context.Context, channelID string, allowDelete bool, queue func(bridgev2.RemoteEvent)) {
	if channelID == "" || channelID == "." || channelID == ".." {
		return
	}
	serverURL, token := c.serverURL(), c.Token
	if allowDelete {
		self, _, err := mattermost.GetSelfContext(ctx, serverURL, token)
		if err != nil || self == "" || self != c.UserID {
			return
		}
	}
	ch, err := mattermost.GetChannelContext(ctx, serverURL, token, channelID)
	if ctx.Err() != nil {
		return
	}
	if err == nil && ch.ID == channelID {
		c.resyncLifecycleChannel(ch, queue)
		return
	}
	var httpErr *mattermost.HTTPError
	if allowDelete && errors.As(err, &httpErr) && httpErr.IsAuthoritativeChannelNotFound(channelID) {
		c.mu.Lock()
		delete(c.channels, channelID)
		c.mu.Unlock()
		queue(&mmChatDeleteEvent{
			portalKey: networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: c.UserLogin.ID},
		})
	}
}

func (c *MattermostClient) resyncLifecycleChannel(ch *mattermost.Channel, queue func(bridgev2.RemoteEvent)) {
	c.mu.Lock()
	if c.channels == nil {
		c.channels = make(map[string]*mattermost.Channel)
	}
	c.channels[ch.ID] = ch
	c.mu.Unlock()
	queue(c.channelResyncEvent(ch))
}
