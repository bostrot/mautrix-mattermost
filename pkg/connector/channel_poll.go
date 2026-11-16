package connector

import (
	"sort"
	"time"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Why this file exists: Mattermost's websocket cannot be trusted to report
// archive and restore of a channel.
//
// For every connection the server keeps a membership snapshot that decides which
// channel events the connection receives. It is built lazily at the first
// channel-scoped event, loaded with includeDeleted=false (so archived channels
// are missing from it), and reused for 30 minutes (web_conn.go ShouldSendEvent,
// webConnMemberCacheTime). A connection whose snapshot was built while a channel
// was archived therefore never sees the restore, nor any later post in that
// channel, until the snapshot expires or the bridge reconnects. The archive
// itself can be lost the same way: the system post is published just after the
// channel is soft-deleted, so a connection with no earlier snapshot builds one
// without the channel and drops that very event.
//
// So lifecycle state is polled over REST, including archived channels, and the
// websocket is only used for what it is good at: live messages.
//
// Safety rules:
//   - The first poll only records a baseline. Nothing is acted on at startup,
//     so rooms that are already stale are never deleted by a deploy.
//   - An incomplete listing (any team failed) changes nothing and keeps the
//     previous baseline: a partial answer is not evidence that a channel vanished.
//   - A channel that disappears is only a hint. It goes through the same
//     probe-gated, dequeue-time deletion check as a channel_deleted event.
//     A 403, 5xx or other ambiguous probe response preserves the room.

// lifecyclePollInterval is how often channel state is reconciled.
const lifecyclePollInterval = 30 * time.Second

type lifecycleKind int

const (
	lifecycleArchived lifecycleKind = iota + 1
	lifecycleRestored
	lifecycleGone
)

func (k lifecycleKind) String() string {
	switch k {
	case lifecycleArchived:
		return "archived"
	case lifecycleRestored:
		return "restored"
	case lifecycleGone:
		return "gone"
	}
	return "unknown"
}

type lifecycleTransition struct {
	ChannelID string
	Kind      lifecycleKind
}

// archiveStates maps a channel listing to channelID -> archived.
func archiveStates(channels map[string]*mattermost.Channel) map[string]bool {
	out := make(map[string]bool, len(channels))
	for id, ch := range channels {
		out[id] = ch != nil && ch.DeleteAt > 0
	}
	return out
}

// diffChannelLifecycle reports what changed between two snapshots, in channel ID
// order so the result is deterministic. Channels that are new in cur are not a
// transition: creation is handled by the normal sync paths.
func diffChannelLifecycle(prev, cur map[string]bool) []lifecycleTransition {
	ids := make([]string, 0, len(prev))
	for id := range prev {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []lifecycleTransition
	for _, id := range ids {
		wasArchived := prev[id]
		nowArchived, present := cur[id]
		switch {
		case !present:
			out = append(out, lifecycleTransition{id, lifecycleGone})
		case !wasArchived && nowArchived:
			out = append(out, lifecycleTransition{id, lifecycleArchived})
		case wasArchived && !nowArchived:
			out = append(out, lifecycleTransition{id, lifecycleRestored})
		}
	}
	return out
}

// listChannels returns every channel of the user, archived ones included. The
// bool is false when the listing is incomplete.
func (c *MattermostClient) listChannels() (map[string]*mattermost.Channel, bool) {
	if c.listChannelStatesFn != nil {
		return c.listChannelStatesFn()
	}
	teams, err := mattermost.GetTeams(c.serverURL(), c.Token, c.UserID)
	if err != nil {
		return nil, false
	}
	out := make(map[string]*mattermost.Channel)
	for _, team := range teams {
		channels, err := mattermost.GetChannelsForUserIncludingArchived(c.serverURL(), c.Token, c.UserID, team.ID)
		if err != nil {
			return nil, false
		}
		for _, ch := range channels {
			if _, seen := out[ch.ID]; !seen { // DMs appear in every team
				out[ch.ID] = ch
			}
		}
	}
	return out, true
}

// reconnectWebsocket drops the current websocket so connectLoop builds a new
// one. A new connection gets a fresh membership snapshot, and its Hello triggers
// a full resync that backfills whatever the old connection missed.
func (c *MattermostClient) reconnectWebsocket() {
	if c.reconnectFn != nil {
		c.reconnectFn()
		return
	}
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl != nil {
		cl.Close()
	}
}

// applyLifecycleTransition reacts to one observed change. ch is the channel as
// listed just now (nil for lifecycleGone).
//
// It reports whether the websocket must be replaced. The caller coalesces that
// across the whole poll: N restores in one cycle need one reconnect, not N, each
// of which would trigger a full resync.
func (c *MattermostClient) applyLifecycleTransition(tr lifecycleTransition, ch *mattermost.Channel) (needsReconnect bool) {
	log := c.lifecycleLogger()
	log.Info().
		Str("channel_id", tr.ChannelID).
		Str("transition", tr.Kind.String()).
		Msg("Channel lifecycle change observed by poll")
	switch tr.Kind {
	case lifecycleArchived:
		// Reconcile the journal, then resync to backfill the archive notice.
		c.queueAccessSync(tr.ChannelID)
		if ch != nil {
			c.queueChannelResync(ch)
		}
	case lifecycleRestored:
		// Make the room writable before anything else, resync it (backfill of
		// the restore notice and of missed posts), and replace the websocket,
		// whose snapshot still excludes this channel.
		c.noteChannelAlive(tr.ChannelID)
		c.queueAccessSync(tr.ChannelID)
		if ch != nil {
			c.queueChannelResync(ch)
		}
		return true
	case lifecycleGone:
		// Only a hint: the dequeue-time probe decides whether the room goes.
		c.handleChannelDeleted(tr.ChannelID)
	}
	return false
}

// pollChannelLifecycle runs one reconciliation. prev is the last complete
// snapshot (nil before the first one); the new snapshot is returned.
func (c *MattermostClient) pollChannelLifecycle(prev map[string]bool) map[string]bool {
	channels, complete := c.listChannels()
	if !complete {
		return prev
	}
	cur := archiveStates(channels)
	if prev != nil {
		reconnect := false
		for _, tr := range diffChannelLifecycle(prev, cur) {
			if c.applyLifecycleTransition(tr, channels[tr.ChannelID]) {
				reconnect = true
			}
		}
		if reconnect {
			c.reconnectWebsocket()
		}
	}
	return cur
}

// lifecyclePollLoop polls until Disconnect.
func (c *MattermostClient) lifecyclePollLoop() {
	done := c.done
	var snapshot map[string]bool
	ticker := time.NewTicker(lifecyclePollInterval)
	defer ticker.Stop()
	for {
		snapshot = c.pollChannelLifecycle(snapshot)
		select {
		case <-done:
			return
		case <-ticker.C:
		}
	}
}
