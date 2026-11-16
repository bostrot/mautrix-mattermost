package connector

import (
	"reflect"
	"sync/atomic"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func TestDiffChannelLifecycle(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur map[string]bool
		want      []lifecycleTransition
	}{
		{"unchanged", map[string]bool{"a": false, "b": true}, map[string]bool{"a": false, "b": true}, nil},
		{"archived", map[string]bool{"a": false}, map[string]bool{"a": true}, []lifecycleTransition{{"a", lifecycleArchived}}},
		{"restored", map[string]bool{"a": true}, map[string]bool{"a": false}, []lifecycleTransition{{"a", lifecycleRestored}}},
		{"gone while active", map[string]bool{"a": false}, map[string]bool{}, []lifecycleTransition{{"a", lifecycleGone}}},
		{"gone while archived", map[string]bool{"a": true}, map[string]bool{}, []lifecycleTransition{{"a", lifecycleGone}}},
		{"new channel is not a transition", map[string]bool{}, map[string]bool{"n": false}, nil},
		{"deterministic order", map[string]bool{"b": false, "a": false, "c": true},
			map[string]bool{"b": true, "a": true, "c": false},
			[]lifecycleTransition{{"a", lifecycleArchived}, {"b", lifecycleArchived}, {"c", lifecycleRestored}}},
		{"nil prev", nil, map[string]bool{"a": true}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diffChannelLifecycle(tc.prev, tc.cur); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestArchiveStates(t *testing.T) {
	got := archiveStates(map[string]*mattermost.Channel{
		"live": {ID: "live"}, "old": {ID: "old", DeleteAt: 99}, "nil": nil,
	})
	want := map[string]bool{"live": false, "old": true, "nil": false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// pollRig drives pollChannelLifecycle against a scripted server listing.
type pollRig struct {
	*lifecycleHarness
	listing   map[string]*mattermost.Channel
	complete  bool
	reconnect atomic.Int32
}

func newPollRig(t *testing.T) *pollRig {
	t.Helper()
	r := &pollRig{lifecycleHarness: newLifecycleHarness(t, lifecycleOptions{probe: activeProbe}), complete: true}
	r.client.listChannelStatesFn = func() (map[string]*mattermost.Channel, bool) {
		if !r.complete {
			return nil, false
		}
		cp := make(map[string]*mattermost.Channel, len(r.listing))
		for k, v := range r.listing {
			cp[k] = v
		}
		return cp, true
	}
	r.client.reconnectFn = func() { r.reconnect.Add(1) }
	return r
}

func chans(archived ...string) map[string]*mattermost.Channel {
	out := map[string]*mattermost.Channel{"active": {ID: "active", Type: "O", Name: "active", DisplayName: "Active"}}
	for _, id := range archived {
		out[id] = &mattermost.Channel{ID: id, Type: "P", Name: id, DisplayName: id, DeleteAt: 1234}
	}
	return out
}

func assertResyncPreservesPower(t *testing.T, ev bridgev2.RemoteEvent) {
	t.Helper()
	rs, ok := ev.(*simplevent.ChatResync)
	if !ok {
		t.Fatalf("%T is not a ChatResync", ev)
	}
	if rs.ChatInfo.Members.PowerLevels != nil {
		t.Fatal("resync must not override power levels")
	}
}

func TestPollFirstRunOnlyRecordsABaseline(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans("already-archived") // archived before we started
	snap := r.client.pollChannelLifecycle(nil)
	if n := r.events.len(); n != 0 || r.reconnect.Load() != 0 {
		t.Fatalf("baseline poll queued %d events and %d reconnects, want none", n, r.reconnect.Load())
	}
	if !snap["already-archived"] || snap["active"] {
		t.Fatalf("baseline = %v", snap)
	}
}

func TestPollArchiveLocksAndBackfills(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans()
	r.listing["room"] = &mattermost.Channel{ID: "room", Type: "P", Name: "room", DisplayName: "Room"}
	snap := r.client.pollChannelLifecycle(nil)

	r.listing["room"] = &mattermost.Channel{ID: "room", Type: "P", Name: "room", DisplayName: "Room", DeleteAt: 5}
	r.client.pollChannelLifecycle(snap)

	evs := r.events.drain()
	if len(evs) != 2 {
		t.Fatalf("queued %d events, want the access sync and one resync", len(evs))
	}
	if _, ok := evs[0].(*mmChannelAccessEvent); !ok {
		t.Errorf("first event is %T, want the access sync", evs[0])
	}
	assertResyncPreservesPower(t, evs[1])
	if r.reconnect.Load() != 0 {
		t.Error("archiving must not reconnect the websocket")
	}
	if len(r.events.checks()) != 0 {
		t.Error("archiving must not queue a deletion check")
	}
}

func TestPollRestoreUnlocksResyncsAndReconnects(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans("room")
	snap := r.client.pollChannelLifecycle(nil)
	genBefore := r.client.channelGeneration("room")

	r.listing["room"] = &mattermost.Channel{ID: "room", Type: "P", Name: "room", DisplayName: "Room"}
	r.client.pollChannelLifecycle(snap)

	evs := r.events.drain()
	if len(evs) != 2 {
		t.Fatalf("queued %d events, want the access sync and one resync", len(evs))
	}
	if _, ok := evs[0].(*mmChannelAccessEvent); !ok {
		t.Errorf("first event is %T, want the access sync ahead of the resync", evs[0])
	}
	assertResyncPreservesPower(t, evs[1])
	if r.reconnect.Load() != 1 {
		t.Errorf("reconnects = %d, want exactly 1: the old connection's snapshot excludes the channel", r.reconnect.Load())
	}
	if r.client.channelGeneration("room") == genBefore {
		t.Error("a restore must invalidate pending deletion checks")
	}
}

func TestPollDisappearedChannelOnlyQueuesTheGatedCheck(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans()
	r.listing["room"] = &mattermost.Channel{ID: "room", Type: "P", Name: "room"}
	snap := r.client.pollChannelLifecycle(nil)

	delete(r.listing, "room")
	r.client.pollChannelLifecycle(snap)

	evs := r.events.drain()
	if len(evs) != 1 {
		t.Fatalf("queued %d events, want exactly the neutral deletion check", len(evs))
	}
	if _, ok := evs[0].(*mmChannelDeleteCheckEvent); !ok {
		t.Fatalf("event is %T, want *mmChannelDeleteCheckEvent", evs[0])
	}
	if evs[0].GetType() == bridgev2.RemoteEventChatDelete {
		t.Fatal("a vanished channel must never queue a room deletion directly")
	}
	if r.reconnect.Load() != 0 {
		t.Error("a vanished channel must not reconnect")
	}
}

// An incomplete listing proves nothing: keep the old baseline, change nothing.
func TestPollIncompleteListingChangesNothing(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans("room")
	snap := r.client.pollChannelLifecycle(nil)

	r.complete = false
	next := r.client.pollChannelLifecycle(snap)
	if !reflect.DeepEqual(next, snap) {
		t.Fatalf("baseline changed on a failed listing: %v -> %v", snap, next)
	}
	if r.events.len() != 0 || r.reconnect.Load() != 0 {
		t.Fatal("a failed listing caused actions")
	}

	// And the next good listing is compared against the baseline from before.
	r.complete = true
	r.listing["room"] = &mattermost.Channel{ID: "room", Type: "P", Name: "room"}
	r.client.pollChannelLifecycle(next)
	if r.reconnect.Load() != 1 {
		t.Errorf("reconnects = %d, want the restore noticed after the outage", r.reconnect.Load())
	}
}

func TestPollUnchangedAndNewChannelsDoNothing(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans("room")
	snap := r.client.pollChannelLifecycle(nil)
	snap = r.client.pollChannelLifecycle(snap)
	r.listing["brand-new"] = &mattermost.Channel{ID: "brand-new", Type: "O", Name: "n"}
	r.client.pollChannelLifecycle(snap)
	if r.events.len() != 0 || r.reconnect.Load() != 0 {
		t.Fatalf("queued %d events / %d reconnects for no lifecycle change", r.events.len(), r.reconnect.Load())
	}
}

// N restores in one poll cycle must replace the websocket once, not N times:
// every reconnect triggers a full resync.
func TestPollCoalescesReconnectsAcrossRestores(t *testing.T) {
	r := newPollRig(t)
	r.listing = chans("a", "b", "c")
	snap := r.client.pollChannelLifecycle(nil)
	for _, id := range []string{"a", "b", "c"} {
		r.listing[id] = &mattermost.Channel{ID: id, Type: "P", Name: id, DisplayName: id}
	}
	r.client.pollChannelLifecycle(snap)
	if got := r.reconnect.Load(); got != 1 {
		t.Fatalf("reconnects = %d for 3 restores in one poll, want exactly 1", got)
	}
	if n := len(r.events.accessSyncs()); n != 3 {
		t.Errorf("access syncs = %d, want one per restored channel (3)", n)
	}
}
