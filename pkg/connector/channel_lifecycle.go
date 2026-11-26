package connector

import (
	"context"
	"errors"
	"net/http"

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
		case hint := <-hints:
			c.handleChannelLifecycle(ctx, hint.channelID, hint.allowDelete, c.queueLifecycleEvent)
		}
	}
}

func (c *MattermostClient) queueLifecycleEvent(evt bridgev2.RemoteEvent) {
	c.UserLogin.QueueRemoteEvent(evt)
}

type channelLifecycleOutcome uint8

const (
	lifecycleInconclusive channelLifecycleOutcome = iota
	lifecycleActive
	lifecycleArchived
	lifecycleDeleted
	lifecycleAccessLost
)

func (c *MattermostClient) handleChannelLifecycle(ctx context.Context, channelID string, allowDelete bool, queue func(bridgev2.RemoteEvent)) channelLifecycleOutcome {
	ch, outcome := c.probeChannelLifecycle(ctx, channelID, allowDelete)
	switch outcome {
	case lifecycleActive, lifecycleArchived:
		c.resyncLifecycleChannel(ch, queue)
	case lifecycleDeleted:
		c.deleteLifecycleChannel(channelID, queue)
	}
	return outcome
}

// probeChannelLifecycle reads authoritative state without resyncing a room.
// Polling a channel absent from the membership listing must not recreate it.
func (c *MattermostClient) probeChannelLifecycle(ctx context.Context, channelID string, allowDelete bool) (*mattermost.Channel, channelLifecycleOutcome) {
	if channelID == "" || channelID == "." || channelID == ".." {
		return nil, lifecycleInconclusive
	}
	serverURL, token := c.serverURL(), c.Token
	if ctx.Err() != nil {
		return nil, lifecycleInconclusive
	}
	if allowDelete {
		self, _, err := mattermost.GetSelfContext(ctx, serverURL, token)
		if err != nil || self == "" || self != c.UserID {
			return nil, lifecycleInconclusive
		}
	}
	ch, err := mattermost.GetChannelContext(ctx, serverURL, token, channelID)
	if ctx.Err() != nil {
		return nil, lifecycleInconclusive
	}
	if err == nil && ch != nil && ch.ID == channelID {
		if ch.DeleteAt > 0 {
			return ch, lifecycleArchived
		}
		return ch, lifecycleActive
	}
	var httpErr *mattermost.HTTPError
	if allowDelete && errors.As(err, &httpErr) && httpErr.IsAuthoritativeChannelNotFound(channelID) {
		return nil, lifecycleDeleted
	}
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusForbidden &&
		(httpErr.BodyStatusCode == 0 || httpErr.BodyStatusCode == http.StatusForbidden) {
		return nil, lifecycleAccessLost
	}
	return nil, lifecycleInconclusive
}

func (c *MattermostClient) deleteLifecycleChannel(channelID string, queue func(bridgev2.RemoteEvent)) {
	c.mu.Lock()
	delete(c.channels, channelID)
	c.mu.Unlock()
	queue(&mmChatDeleteEvent{
		portalKey: networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: c.UserLogin.ID},
	})
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
