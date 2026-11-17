package connector

import (
	"context"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestDeleteProbeMustMatchLoginIdentity(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: func(id string) mattermost.ChannelProbe {
		p := authoritativeNotFoundProbe(id)
		p.AuthenticatedUserID = "wrong-user"
		return p
	}})
	h.client.UserID = "expected-user"
	gen := h.client.bumpChannelGeneration("channel")
	ev := mmChannelDeleteCheckEvent{client: h.client, channelID: "channel", portalKey: networkid.PortalKey{ID: "channel"}, generation: gen}
	result := h.client.applyDeferredChannelDelete(context.Background(), h.sink, ev)
	if result.Applied || h.sink.count() != 0 {
		t.Fatal("deleted room using a token belonging to a different user")
	}
}
