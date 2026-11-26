package connector

import (
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

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
// that double-puppets their Mattermost messages.
func ghostPowerLevel(ch *mattermost.Channel) *int {
	level := 0
	if ch != nil && ch.DeleteAt > 0 {
		level = archivedChannelEventLevel
	}
	return ptr.Ptr(level)
}
