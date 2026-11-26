package connector

import (
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestChannelPowerReconcilesExplicitMessageLevels(t *testing.T) {
	for _, archived := range []bool{false, true} {
		name, deleteAt, before, want := "restore", int64(0), 50, 0
		if archived {
			name, deleteAt, before, want = "archive", 123, 0, 50
		}
		t.Run(name, func(t *testing.T) {
			pl := &event.PowerLevelsEventContent{
				EventsDefault: before,
				Events: map[string]int{
					event.EventMessage.Type:   before,
					event.EventEncrypted.Type: before,
					event.EventReaction.Type:  25,
				},
				Users: map[id.UserID]int{"@bridge:example.org": 100, "@alice:example.org": 0},
			}
			overrides := channelPowerLevels(&mattermost.Channel{DeleteAt: deleteAt})
			if !overrides.Apply("@bridge:example.org", pl) {
				t.Fatal("transition did not update power levels")
			}
			if pl.EventsDefault != want {
				t.Fatalf("default=%d, want %d", pl.EventsDefault, want)
			}
			for _, typ := range []event.Type{event.EventMessage, event.EventEncrypted} {
				if got := pl.GetEventLevel(typ); got != want {
					t.Fatalf("%s=%d, want %d", typ, got, want)
				}
			}
			if pl.GetEventLevel(event.EventReaction) != 25 || pl.GetUserLevel("@bridge:example.org") != 100 || pl.GetUserLevel("@alice:example.org") != 0 {
				t.Fatal("unrelated event or user levels changed")
			}
			if overrides.Apply("@bridge:example.org", pl) {
				t.Fatal("unchanged lifecycle state produced another permission update")
			}
		})
	}
}
