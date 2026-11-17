package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// accessSyncs lists the access-sync events the connector queued.
func (r *eventRecorder) accessSyncs() []*mmChannelAccessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*mmChannelAccessEvent
	for _, ev := range r.events {
		if a, ok := ev.(*mmChannelAccessEvent); ok {
			out = append(out, a)
		}
	}
	return out
}

// TestAccessEventIsNeutralAndOrderedOnThePortalLane pins the framework contract
// the design relies on: RemoteEventUnknown (ignored after PreHandle) plus a
// PreHandle hook.
func TestAccessEventIsNeutralAndOrderedOnThePortalLane(t *testing.T) {
	var ev bridgev2.RemoteEvent = &mmChannelAccessEvent{}
	if ev.GetType() != bridgev2.RemoteEventUnknown {
		t.Fatalf("type = %v, want RemoteEventUnknown", ev.GetType())
	}
	if _, ok := ev.(bridgev2.RemotePreHandler); !ok {
		t.Fatal("must implement RemotePreHandler")
	}
	if _, ok := ev.(bridgev2.RemoteChatDelete); ok {
		t.Fatal("an access sync must never be a chat deletion")
	}
	// Nil-safety: PreHandle must not panic on an unattached event.
	(&mmChannelAccessEvent{}).PreHandle(t.Context(), nil)
}

// TestProcessEventsRestoreMakesRoomWritableBeforeTheNotice: the restore notice is
// sent from the user's own account, so the sync must be queued ahead of it.
func TestProcessEventsRestoreMakesRoomWritableBeforeTheNotice(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: activeProbe})
	p.deliver(
		&mattermost.ChannelRestoredEvent{ChannelID: "chan-r"},
		&mattermost.PostedEvent{
			ChannelID: "chan-r", ChannelType: "P", ChannelName: "test-channel",
			Post: &mattermost.Post{ID: "p-restored", Type: "system_channel_restored", Message: "alice unarchived the channel.", UserID: "other"},
		},
	)
	evs := p.events.drain()
	firstMessage := -1
	lastAccess := -1
	for i, ev := range evs {
		switch ev.(type) {
		case *mmChannelAccessEvent:
			lastAccess = i
		default:
			if ev.GetType() == bridgev2.RemoteEventMessage && firstMessage < 0 {
				firstMessage = i
			}
		}
	}
	if firstMessage < 0 {
		t.Fatalf("the restore notice was not queued: %d events", len(evs))
	}
	if lastAccess < 0 || lastAccess > firstMessage {
		t.Fatalf("access sync at %d, notice at %d: the room must be writable before the notice is relayed", lastAccess, firstMessage)
	}
}

// TestProcessEventsArchivePostIsRelayedBeforeReadOnly: the archive notice is
// relayed first, then the room is locked.
func TestProcessEventsArchivePostIsRelayedBeforeReadOnly(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: archivedProbe})
	p.deliver(
		&mattermost.PostedEvent{
			ChannelID: "chan-a", ChannelType: "P", ChannelName: "test-channel",
			Post: &mattermost.Post{ID: "p-archived", Type: "system_channel_deleted", Message: "alice archived the channel.", UserID: "other"},
		},
		&mattermost.ChannelDeletedEvent{ChannelID: "chan-a"},
	)
	evs := p.events.drain()
	notice, access := -1, -1
	for i, ev := range evs {
		if _, ok := ev.(*mmChannelAccessEvent); ok {
			access = i
		} else if ev.GetType() == bridgev2.RemoteEventMessage {
			notice = i
		}
	}
	if notice < 0 || access < 0 || access < notice {
		t.Fatalf("notice at %d, access sync at %d: the notice must come first", notice, access)
	}
}

// TestProcessEventsRestorePostAloneMakesRoomWritable covers a missed
// channel_restored websocket event: the restore post is then the only signal,
// and it must still unlock the room ahead of its own notice.
func TestProcessEventsRestorePostAloneMakesRoomWritable(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: activeProbe})
	p.deliver(&mattermost.PostedEvent{
		ChannelID: "chan-missed", ChannelType: "P", ChannelName: "test-channel",
		Post: &mattermost.Post{ID: "p-restored-2", Type: "system_channel_restored", Message: "alice unarchived the channel.", UserID: "other"},
	})
	evs := p.events.drain()
	if len(evs) < 2 {
		t.Fatalf("queued %d events, want the access sync and the notice", len(evs))
	}
	if _, ok := evs[0].(*mmChannelAccessEvent); !ok {
		t.Fatalf("first queued event is %T, want the access sync ahead of the notice", evs[0])
	}
	if evs[1].GetType() != bridgev2.RemoteEventMessage {
		t.Fatalf("second queued event is %v, want the relayed notice", evs[1].GetType())
	}
}

func TestUnlistedPortalIDs(t *testing.T) {
	portals := []*database.Portal{
		{PortalKey: networkid.PortalKey{ID: "listed", Receiver: "me"}, MXID: "!a:x"},
		{PortalKey: networkid.PortalKey{ID: "archived", Receiver: "me"}, MXID: "!b:x"},
		{PortalKey: networkid.PortalKey{ID: "no-room-yet", Receiver: "me"}},
		{PortalKey: networkid.PortalKey{ID: "someone-elses", Receiver: "other"}, MXID: "!c:x"},
		nil,
	}
	got := unlistedPortalIDs(portals, "me", map[string]bool{"listed": true})
	if len(got) != 1 || got[0] != "archived" {
		t.Fatalf("unlisted = %v, want only [archived]: listed, room-less and other logins' portals must be skipped", got)
	}
}

func TestReconcileUnlistedPortalsWithoutBridgeIsANoOp(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{noLogin: true})
	h.client.reconcileUnlistedPortals(map[string]bool{}) // must not panic
	if n := h.events.len(); n != 0 {
		t.Fatalf("queued %d events without a bridge", n)
	}
}

func TestHasArchiveJournal(t *testing.T) {
	withJournal := &database.Portal{MXID: "!r:x", Metadata: &PortalMetadata{archiveJournalKey: []byte(`{}`), "other": []byte(`1`)}}
	cases := []struct {
		name string
		p    *database.Portal
		want bool
	}{
		{"journal present", withJournal, true},
		{"journal absent", &database.Portal{MXID: "!r:x", Metadata: &PortalMetadata{"other": []byte(`1`)}}, false},
		{"no room yet", &database.Portal{Metadata: &PortalMetadata{archiveJournalKey: []byte(`{}`)}}, false},
		{"foreign metadata type", &database.Portal{MXID: "!r:x", Metadata: struct{}{}}, false},
		{"nil metadata", &database.Portal{MXID: "!r:x"}, false},
		{"nil portal", nil, false},
	}
	for _, tc := range cases {
		if got := hasArchiveJournal(tc.p); got != tc.want {
			t.Errorf("%s: hasArchiveJournal = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A channel deleted while the bridge was offline never produced an event, and the
// access sync ignores it, so its room used to stay forever. A server 404 must now
// reach the deferred deletion check; every other answer must not.
func TestRouteUnlistedChannelByProbeState(t *testing.T) {
	cases := []struct {
		name       string
		probe      func(string) mattermost.ChannelProbe
		wantChecks int
		wantAccess int
	}{
		{"deleted on the server", authoritativeNotFoundProbe, 1, 0},
		{"archived", archivedProbe, 0, 1},
		{"active", activeProbe, 0, 1},
		{"forbidden is not a deletion", func(string) mattermost.ChannelProbe { return httpProbe(403) }, 0, 1},
		{"server error is not a deletion", func(string) mattermost.ChannelProbe { return httpProbe(500) }, 0, 1},
		{"transport error is not a deletion", func(string) mattermost.ChannelProbe { return transportErrProbe() }, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t, lifecycleOptions{probe: tc.probe})
			h.client.routeUnlistedChannel("chan-x")
			if got := len(h.events.checks()); got != tc.wantChecks {
				t.Errorf("deletion checks = %d, want %d", got, tc.wantChecks)
			}
			if got := len(h.events.accessSyncs()); got != tc.wantAccess {
				t.Errorf("access syncs = %d, want %d", got, tc.wantAccess)
			}
			if got := len(h.events.chatDeletes()); got != 0 {
				t.Errorf("queued %d delete events at hint time, want none", got)
			}
		})
	}
}

// End to end through the dequeue-time decision: a 404 that cannot be proven
// authoritative (here: the token no longer identifies the login) keeps the room.
func TestRouteUnlistedChannelStillPreservesOnIdentityMismatch(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: func(id string) mattermost.ChannelProbe {
		p := authoritativeNotFoundProbe(id)
		p.AuthenticatedUserID = "someone-else"
		return p
	}})
	h.client.routeUnlistedChannel("chan-x")
	for _, ev := range h.events.drain() {
		if chk, ok := ev.(*mmChannelDeleteCheckEvent); ok {
			if res := h.client.applyDeferredChannelDelete(context.Background(), h.sink, *chk); res.Applied {
				t.Fatalf("deleted a room on a 404 that was not identity-matched (reason %q)", res.Reason)
			} else if res.Reason != "probe-identity-mismatch" {
				t.Fatalf("reason = %q, want probe-identity-mismatch", res.Reason)
			}
		}
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("sink received %d deletions, want 0", n)
	}
}
