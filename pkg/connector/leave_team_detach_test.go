package connector

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// A team's space cannot be deleted while its channel portals still point at it
// through the parent key: the delete fails with a foreign key error and the
// space survives. Leaving the team must therefore detach the children first,
// without deleting them, because their rooms are meant to survive.
func TestLeaveTeamDetachesSpaceChildrenBeforeDeleting(t *testing.T) {
	c := teamClient("http://unused")
	c.UserLogin.Bridge = &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{}}

	var order []string
	var detached networkid.PortalKey

	c.detachChildrenFn = func(_ context.Context, key networkid.PortalKey) error {
		order = append(order, "detach")
		detached = key
		return nil
	}
	c.queueTeamFn = func(ev bridgev2.RemoteEvent) {
		order = append(order, "queue")
		if _, ok := ev.(*simplevent.ChatDelete); !ok {
			t.Fatalf("queued %#v, want a ChatDelete", ev)
		}
	}

	c.handleLeaveTeam(&mattermost.LeaveTeamEvent{TeamID: "t1", UserID: "me"})

	if detached.ID != "team:t1" {
		t.Fatalf("detached parent = %q, want team:t1", detached.ID)
	}
	if len(order) != 2 || order[0] != "detach" || order[1] != "queue" {
		t.Fatalf("order = %v, want detach before queue", order)
	}
}

// Detaching is best effort: if it fails the delete is still queued, so the
// failure shows up in the log instead of the space being silently kept.
func TestLeaveTeamQueuesDeleteWhenDetachFails(t *testing.T) {
	c := teamClient("http://unused")
	c.UserLogin.Bridge = &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{}}

	queued := 0
	c.detachChildrenFn = func(context.Context, networkid.PortalKey) error {
		return errors.New("database is locked")
	}
	c.queueTeamFn = func(bridgev2.RemoteEvent) { queued++ }

	c.handleLeaveTeam(&mattermost.LeaveTeamEvent{TeamID: "t1", UserID: "me"})

	if queued != 1 {
		t.Fatalf("queued %d deletes, want 1", queued)
	}
}

// Another login's departure must not touch this login's space.
func TestLeaveTeamIgnoresOtherUsers(t *testing.T) {
	c := teamClient("http://unused")
	c.UserLogin.Bridge = &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{}}

	detached, queued := 0, 0
	c.detachChildrenFn = func(context.Context, networkid.PortalKey) error { detached++; return nil }
	c.queueTeamFn = func(bridgev2.RemoteEvent) { queued++ }

	c.handleLeaveTeam(&mattermost.LeaveTeamEvent{TeamID: "t1", UserID: "someone-else"})

	if detached != 0 || queued != 0 {
		t.Fatalf("detached=%d queued=%d, want no work for another user", detached, queued)
	}
}
