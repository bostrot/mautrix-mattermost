package connector

// Ordering regression tests for the rewritten channel-lifecycle path.
//
// The old shape queued a bridgev2.RemoteChatDelete as soon as a channel_deleted
// hint looked like a deletion, and then could not stop the framework from
// applying it after a restore had landed: bridgev2's PreHandle/PostHandle hooks
// return nothing and RemoteChatDelete has no abort method.
//
// The current shape queues only a neutral mmChannelDeleteCheckEvent
// (RemoteEventUnknown + PreHandle). bridgev2 calls PreHandle when it dequeues
// the event on the portal lane and then ignores the unknown type, so the
// decision - and the only destructive call - happens at dequeue time, after the
// event's generation has been re-checked against restores.
//
// These tests model that lane faithfully: FIFO drain, PreHandle's observable
// work (applyDeferredChannelDelete with the framework's deletion path as sink),
// and nothing else for an unknown type.

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// TestBridgeV2ChatDeleteOffersNoVetoHook pins the framework constraint that
// motivates the neutral-event design: there is no way to cancel a queued
// RemoteChatDelete.
//
//  1. the pre/post dispatch hooks run *around* handling and return nothing, so
//     a connector cannot veto a queued event from them;
//  2. mmChatDeleteEvent is a genuine bridgev2.RemoteChatDelete and implements
//     neither hook, so it is only ever constructed once deletion is proven;
//  3. bridgev2.RemoteChatDelete exposes only event identity plus
//     DeleteOnlyForMe - there is no "abort"/"reconsider" method to implement.
func TestBridgeV2ChatDeleteOffersNoVetoHook(t *testing.T) {
	hooks := []struct {
		iface  reflect.Type
		method string
	}{
		{reflect.TypeOf((*bridgev2.RemotePreHandler)(nil)).Elem(), "PreHandle"},
		{reflect.TypeOf((*bridgev2.RemotePostHandler)(nil)).Elem(), "PostHandle"},
	}
	for _, hook := range hooks {
		m, ok := hook.iface.MethodByName(hook.method)
		if !ok {
			t.Fatalf("bridgev2.%s no longer has %s", hook.iface.Name(), hook.method)
		}
		if n := m.Type.NumOut(); n != 0 {
			t.Fatalf(
				"bridgev2.%s.%s now returns %d value(s): the framework has a veto opportunity and the "+
					"deferred-check design can be reconsidered",
				hook.iface.Name(), hook.method, n,
			)
		}
	}

	var queuedEvent bridgev2.RemoteChatDelete = &mmChatDeleteEvent{}
	if _, ok := queuedEvent.(bridgev2.RemotePreHandler); ok {
		t.Fatal("mmChatDeleteEvent implements bridgev2.RemotePreHandler; a real deletion must stay hook-free so it is only queued after proof")
	}
	if _, ok := queuedEvent.(bridgev2.RemotePostHandler); ok {
		t.Fatal("mmChatDeleteEvent implements bridgev2.RemotePostHandler; a real deletion must stay hook-free")
	}

	// RemoteEvent (4 methods) + DeleteOnlyForMe. A new method here is exactly
	// the shape a veto would take, so this assertion is the canary.
	want := map[string]bool{
		"GetType":         true,
		"GetPortalKey":    true,
		"AddLogContext":   true,
		"GetSender":       true,
		"DeleteOnlyForMe": true,
	}
	iface := reflect.TypeOf((*bridgev2.RemoteChatDelete)(nil)).Elem()
	if iface.NumMethod() != len(want) {
		t.Fatalf("bridgev2.RemoteChatDelete has %d methods (%v), want %d: inspect for a newly added veto method",
			iface.NumMethod(), methodNames(iface), len(want))
	}
	for i := 0; i < iface.NumMethod(); i++ {
		name := iface.Method(i).Name
		if !want[name] {
			t.Fatalf("bridgev2.RemoteChatDelete gained method %s; re-check whether queued deletions are now revocable", name)
		}
	}
}

func methodNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		names = append(names, t.Method(i).Name)
	}
	return names
}

// TestChannelDeleteCheckEventIsInert pins the neutral event's contract: an
// unknown type (so bridgev2's type switch ignores it) that still gets PreHandle.
func TestChannelDeleteCheckEventIsInert(t *testing.T) {
	var ev bridgev2.RemoteEvent = &mmChannelDeleteCheckEvent{}
	if ev.GetType() != bridgev2.RemoteEventUnknown {
		t.Fatalf("event type = %v, want RemoteEventUnknown (bridgev2 must ignore it after PreHandle)", ev.GetType())
	}
	if _, ok := ev.(bridgev2.RemoteChatDelete); ok {
		t.Fatal("the neutral check must not satisfy RemoteChatDelete")
	}
	if _, ok := ev.(bridgev2.RemotePreHandler); !ok {
		t.Fatal("the neutral check must implement RemotePreHandler; that is the only way it acts")
	}
	if _, ok := ev.(bridgev2.RemotePostHandler); ok {
		t.Fatal("the neutral check must not implement RemotePostHandler; the decision is taken before handling")
	}
}

// portalEventQueue models bridgev2's per-portal event buffer precisely: events
// are appended in FIFO order when queued and applied later, one at a time, by
// Portal.eventLoop. For an unknown event type bridgev2 only calls PreHandle and
// then logs and ignores the event; a RemoteEventChatDelete would be applied by
// Portal.handleRemoteChatDelete unconditionally.
type portalEventQueue struct {
	mu      sync.Mutex
	pending []bridgev2.RemoteEvent
	results []deferredDeleteResult
	// roomsGone records chat-delete events the framework would have applied.
	// The connector must never put one on this queue.
	roomsGone []networkid.PortalID
}

func (q *portalEventQueue) enqueue(ev bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, ev)
	return bridgev2.EventHandlingResultQueued
}

func (q *portalEventQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// drain applies queued events in FIFO order, the way Portal.eventLoop does.
func (q *portalEventQueue) drain(c *MattermostClient, sink portalDeleteSink) {
	q.mu.Lock()
	defer q.mu.Unlock()
	ctx := context.Background()
	for _, ev := range q.pending {
		if check, ok := ev.(*mmChannelDeleteCheckEvent); ok {
			q.results = append(q.results, c.applyDeferredChannelDelete(ctx, sink, *check))
			continue
		}
		if ev.GetType() == bridgev2.RemoteEventChatDelete {
			q.roomsGone = append(q.roomsGone, ev.GetPortalKey().ID)
		}
	}
	q.pending = nil
}

func (q *portalEventQueue) deletedRooms() []networkid.PortalID {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]networkid.PortalID(nil), q.roomsGone...)
}

func (q *portalEventQueue) verdicts() []deferredDeleteResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]deferredDeleteResult(nil), q.results...)
}

// TestDeletionQueuedBeforeRestoreIsRejectedAtDequeue is the deterministic
// regression gate for the ordering hazard that the old shape could not close:
// the hint is queued, a restore lands, and only then does the portal lane drain.
// No probe is issued, no room is deleted and the verdict is explicitly recorded
// as superseded.
func TestDeletionQueuedBeforeRestoreIsRejectedAtDequeue(t *testing.T) {
	const channelID = "chan-queued-restore"

	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	q := &portalEventQueue{}
	h.client.queueRemoteEventFn = q.enqueue

	// t0: the channel_deleted hint arrives. Only a neutral check is queued.
	h.client.handleChannelDeleted(channelID)
	if n := q.len(); n != 1 {
		t.Fatalf("queued %d events for one hint, want 1", n)
	}

	// t1: the channel is restored (channel_created / channel_updated /
	// system_channel_restored) while the check is still in the portal buffer.
	gen := h.client.noteChannelAlive(channelID)
	if gen == 0 {
		t.Fatal("restore must bump the channel generation")
	}
	if h.client.isChannelDeleted(channelID) {
		t.Fatal("restore must clear the connector's deletion mark")
	}

	// t2: the framework drains the portal event buffer.
	q.drain(h.client, h.sink)

	if n := q.len(); n != 0 {
		t.Fatalf("%d events left in the queue after draining, want 0", n)
	}
	if rooms := q.deletedRooms(); len(rooms) != 0 {
		t.Fatalf("rooms deleted after a restore: %v, want none", rooms)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("framework deletion path invoked %d times after a restore, want 0", n)
	}
	if n := h.probeCount(); n != 0 {
		t.Fatalf("issued %d probes for a superseded check, want 0", n)
	}
	verdicts := q.verdicts()
	if len(verdicts) != 1 {
		t.Fatalf("got %d verdicts, want 1", len(verdicts))
	}
	if verdicts[0].Applied || verdicts[0].Reason != "superseded-before-probe" {
		t.Fatalf("verdict = %+v, want unapplied superseded-before-probe", verdicts[0])
	}
	if h.client.isChannelDeleted(channelID) {
		t.Error("channel must not be marked deleted")
	}
}

// TestChannelDeletedHintNeverQueuesChatDelete is the property the old code
// violated: a hint must not put a RemoteChatDelete anywhere near the framework,
// because the framework would apply it without re-checking anything.
func TestChannelDeletedHintNeverQueuesChatDelete(t *testing.T) {
	const channelID = "chan-hint-neutral"

	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	q := &portalEventQueue{}
	h.client.queueRemoteEventFn = q.enqueue

	h.client.handleChannelDeleted(channelID)
	if n := q.len(); n != 1 {
		t.Fatalf("queued %d events, want 1", n)
	}
	q.mu.Lock()
	queued := q.pending[0]
	q.mu.Unlock()
	if queued.GetType() == bridgev2.RemoteEventChatDelete {
		t.Fatalf("queued a RemoteChatDelete (%T) for a hint; only the neutral check may be queued", queued)
	}
	if _, ok := queued.(bridgev2.RemoteChatDelete); ok {
		t.Fatalf("queued event %T satisfies RemoteChatDelete", queued)
	}

	// Draining without an intervening lifecycle event still decides at dequeue:
	// proof deletes once, through the framework's own path.
	q.drain(h.client, h.sink)
	if n := h.sink.count(); n != 1 {
		t.Fatalf("framework deletion path invoked %d times, want 1", n)
	}
	if rooms := q.deletedRooms(); len(rooms) != 0 {
		t.Fatalf("the framework applied %d chat-delete events directly (%v); deletions must go through the sink", len(rooms), rooms)
	}
}
