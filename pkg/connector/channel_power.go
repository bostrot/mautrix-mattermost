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
