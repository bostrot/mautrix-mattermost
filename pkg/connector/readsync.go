package connector

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Read sync, both ways, like the Slack bridge:
//
//   - Reading a room in Matrix marks the channel viewed in Mattermost, which
//     clears its unread badge there. Reading a thread marks the Mattermost
//     thread read (collapsed reply threads keep thread unreads separately).
//   - Viewing a channel in Mattermost sends a read receipt for the user in
//     Matrix (through double puppeting), which clears the badge in Matrix
//     clients.
//
// Receipts the bridge itself sends are ignored by the framework, so the two
// directions cannot loop. Mattermost can only mark a channel read up to now,
// not up to a given post; a message arriving between the Matrix client
// showing the room and its receipt reaching Mattermost is marked read too.

var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*MattermostClient)(nil)

// HandleMatrixReadReceipt marks the channel or thread read in Mattermost.
func (c *MattermostClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if msg.Implicit || msg.Portal == nil {
		// Implicit receipts come from the user sending a message, which
		// Mattermost already treats as reading the channel.
		return nil
	}
	if isTeamPortal(msg.Portal.ID) {
		return nil
	}
	channelID := string(msg.Portal.ID)
	if thread := msg.Receipt.ThreadID; thread != "" && thread != "main" {
		return c.markThreadRead(ctx, channelID, string(thread), msg.ReadUpTo)
	}
	if !c.noteViewed(channelID, msg.ReadUpTo) {
		return nil
	}
	return c.viewChannel(channelID)
}

func (c *MattermostClient) viewChannel(channelID string) error {
	if c.viewChannelFn != nil {
		return c.viewChannelFn(channelID)
	}
	return mattermost.ViewChannel(c.serverURL(), c.Token, channelID)
}

// noteViewed records that the channel was read up to t and reports whether
// that is news, so repeated receipts for the same point cost no requests.
func (c *MattermostClient) noteViewed(channelID string, t time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.viewedAt == nil {
		c.viewedAt = make(map[string]time.Time)
	}
	if prev, ok := c.viewedAt[channelID]; ok && !t.After(prev) {
		return false
	}
	c.viewedAt[channelID] = t
	return true
}

// markThreadRead marks a Mattermost thread read. The Matrix thread ID is the
// root's event ID; the root's Mattermost post ID is the thread ID there.
func (c *MattermostClient) markThreadRead(ctx context.Context, channelID string, threadMXID string, upTo time.Time) error {
	var rootID string
	if c.threadRootFn != nil {
		rootID = c.threadRootFn(threadMXID)
	} else {
		root, err := c.UserLogin.Bridge.DB.Message.GetPartByMXID(ctx, id.EventID(threadMXID))
		if err != nil {
			return err
		} else if root == nil {
			return nil
		}
		rootID = string(root.ID)
	}
	if rootID == "" {
		return nil
	}
	teamID := c.teamForChannel(channelID)
	if teamID == "" {
		zerolog.Ctx(ctx).Debug().Str("channel_id", channelID).Msg("No team to mark the thread read in")
		return nil
	}
	if upTo.IsZero() {
		upTo = c.readNow()
	}
	if c.markThreadReadFn != nil {
		return c.markThreadReadFn(teamID, rootID, upTo.UnixMilli())
	}
	return mattermost.MarkThreadRead(c.serverURL(), c.Token, c.UserID, teamID, rootID, upTo.UnixMilli())
}

// teamForChannel is the channel's team, or for DMs and group DMs (which have
// none) any team the user is in.
func (c *MattermostClient) teamForChannel(channelID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ch := c.channels[channelID]; ch != nil && ch.TeamID != "" {
		return ch.TeamID
	}
	best := ""
	for _, ch := range c.channels {
		if ch.TeamID != "" && (best == "" || ch.TeamID < best) {
			best = ch.TeamID // smallest ID: stable across calls
		}
	}
	return best
}

// handleChannelsViewed mirrors reading in Mattermost as Matrix read receipts.
func (c *MattermostClient) handleChannelsViewed(evt *mattermost.ChannelsViewedEvent) {
	for channelID, ms := range evt.Times {
		t := c.readNow()
		if ms > 0 {
			t = time.UnixMilli(ms)
		}
		// Remember it, so the Matrix receipt this produces is not sent back.
		c.noteViewed(channelID, t)
		c.queueReceipt(&simplevent.Receipt{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventReadReceipt,
				PortalKey: networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: c.UserLogin.ID},
				Sender:    bridgev2.EventSender{IsFromMe: true, SenderLogin: c.UserLogin.ID},
				Timestamp: t,
			},
			ReadUpTo: t,
		})
	}
}

func (c *MattermostClient) readNow() time.Time {
	if c.readNowFn != nil {
		return c.readNowFn()
	}
	return time.Now()
}

func (c *MattermostClient) queueReceipt(ev bridgev2.RemoteEvent) {
	if c.queueReceiptFn != nil {
		c.queueReceiptFn(ev)
		return
	}
	c.UserLogin.QueueRemoteEvent(ev)
}
