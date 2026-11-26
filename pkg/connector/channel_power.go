package connector

import (
	"context"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// archivedChannelEventLevel is the power level ordinary room members need to
// send messages in an archived channel's room. The bridge's own user keeps a
// higher level, so the room stays readable and the bridge can keep syncing.
const archivedChannelEventLevel = 50

// channelPowerLevels makes a room match its Mattermost channel's lifecycle:
// archived channels are read-only for ordinary users, active ones are not.
// Message and encrypted-message levels are set explicitly as well as the
// default, so a per-event level left over from an earlier room state cannot
// be used to post past the default. Restoring a channel sets them back to 0,
// which is the same state a freshly created room has.
func channelPowerLevels(ch *mattermost.Channel) *bridgev2.PowerLevelOverrides {
	level := 0
	if ch != nil && ch.DeleteAt > 0 {
		level = archivedChannelEventLevel
	}
	return &bridgev2.PowerLevelOverrides{
		EventsDefault: ptr.Ptr(level),
		Events: map[event.Type]int{
			event.EventMessage:   level,
			event.EventEncrypted: level,
		},
	}
}

// ghostPowerLevel is the power level of the channel's Mattermost ghosts. The
// archive overrides gate ordinary members, but the bridge still has to post as
// ghosts in an archived room: the "unarchived the channel" notice is bridged
// before the restore resync lifts the restriction, and catch-up backfill after
// an archive sends old messages as their authors. Ghosts therefore sit at the
// archived send level (and at 0 again once the channel is restored); only
// Matrix users, who cannot be ghosts, are held back by the overrides.
//
// The logged-in user is deliberately left out: their Matrix account is what
// the read-only overrides are meant to stop, and it may be the same account
// that double-puppets their Mattermost messages. Their history is backfilled
// through their ghost instead (see backfillSender).
func ghostPowerLevel(ch *mattermost.Channel) *int {
	level := 0
	if ch != nil && ch.DeleteAt > 0 {
		level = archivedChannelEventLevel
	}
	return ptr.Ptr(level)
}

// channelArchived reports whether the channel is known to be archived.
func (c *MattermostClient) channelArchived(channelID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.channels[channelID]
	return ch != nil && ch.DeleteAt > 0
}

// ghostMXID is the Matrix ID of a Mattermost user's ghost.
func (c *MattermostClient) ghostMXID(userID string) id.UserID {
	if c.ghostMXIDFn != nil {
		return c.ghostMXIDFn(userID)
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.Matrix == nil {
		return ""
	}
	return c.UserLogin.Bridge.Matrix.GhostIntent(networkid.UserID(userID)).GetMXID()
}

// raiseGhosts lifts the given ghosts to the archived send level as actor and
// reports whether anything changed. Levels already at or above it, and users
// the actor does not outrank, are left as they are.
func raiseGhosts(pl *event.PowerLevelsEventContent, actor id.UserID, ghosts []id.UserID) (changed bool) {
	for _, ghost := range ghosts {
		if ghost == "" || pl.GetUserLevel(ghost) >= archivedChannelEventLevel {
			continue
		}
		changed = pl.EnsureUserLevelAs(actor, ghost, archivedChannelEventLevel) || changed
	}
	return changed
}

// allowArchivedPosting lets the authors of a backfill batch post into an
// archived room. They are not all channel members (people leave, and the
// logged-in user's own history is sent by their ghost), so the member list
// cannot cover them. It runs before the batch is sent. A failure is logged and
// the backfill goes ahead, losing only the messages that are then refused.
//
// Ghosts left at this level after the channel is restored are harmless: only
// the bridge ever sends as a ghost.
func (c *MattermostClient) allowArchivedPosting(ctx context.Context, room id.RoomID, authorIDs map[string]bool) {
	if room == "" || len(authorIDs) == 0 || c.UserLogin == nil || c.UserLogin.Bridge == nil ||
		c.UserLogin.Bridge.Matrix == nil || c.UserLogin.Bridge.Bot == nil {
		return
	}
	log := zerolog.Ctx(ctx)
	var ghosts []id.UserID
	for userID := range authorIDs {
		ghosts = append(ghosts, c.ghostMXID(userID))
	}
	slices.Sort(ghosts)
	pl, err := c.UserLogin.Bridge.Matrix.GetPowerLevels(ctx, room)
	if err != nil {
		log.Warn().Err(err).Msg("Could not read power levels to backfill an archived room")
		return
	}
	bot := c.UserLogin.Bridge.Bot
	if !raiseGhosts(pl, bot.GetMXID(), ghosts) {
		return
	}
	if _, err = bot.SendState(ctx, room, event.StatePowerLevels, "", &event.Content{Parsed: pl}, time.Time{}); err != nil {
		log.Warn().Err(err).Msg("Could not let ghosts post into an archived room; some history may be missing")
	}
}
