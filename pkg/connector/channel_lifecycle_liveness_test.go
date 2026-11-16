package connector

// Liveness regressions in the deferred channel-deletion path.
//
// The deferred check is only useful if it can eventually run and its verdict
// can eventually be applied. Three shapes broke that:
//
//  1. handleChannelDeleted marks the per-channel check slot before it hands
//     the neutral event to the framework, and the slot used to be released
//     only when the check was dequeued. bridgev2.UserLogin.QueueRemoteEvent
//     returns an EventHandlingResult and answers EventHandlingResultFailed
//     when it cannot queue (no portal row for the channel, portal lookup
//     error). The event then never reaches the portal lane, so the slot stayed
//     set forever and every later channel_deleted hint for that channel was
//     dropped: the portal was never deleted, even after the channel vanished.
//
//  2. processEvents treated any channel_updated with delete_at == 0 as proof
//     that the channel was alive again and fed it through noteChannelAlive,
//     which drops the queued check and bumps the generation. A routine update
//     (rename, header, purpose) landing between the hint and the dequeue
//     therefore cancelled the deletion while the channel was in fact gone.
//     A metadata update is not evidence about deletion; the dequeue-time probe
//     is, so the update must not touch lifecycle state.
//
//  3. applyDeferredChannelDelete ignored the EventHandlingResult of
//     PortalInternals.HandleRemoteChatDelete and reported Applied even when
//     the framework refused. The commit was made before the call, so a refused
//     deletion also suppressed every later attempt.
//
// These tests drive the real processEvents loop over a fake WebSocket feed and
// the real queue contract (EventHandlingResult) rather than calling the
// handlers directly, so the wiring - not just the policy - is covered.

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// rejectingQueue models a framework queue that accepts no events, the way
// bridgev2.Bridge.QueueRemoteEvent answers when it cannot find the portal row
// (it returns EventHandlingResultFailed.WithError(ErrPortalNotFoundInEventHandler)).
type rejectingQueue struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (q *rejectingQueue) queue(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	return bridgev2.EventHandlingResultFailed.WithError(q.err)
}

func (q *rejectingQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

// ignoringQueue models a portal that was already deleted: bridgev2 answers
// EventHandlingResultIgnored and drops the event.
type ignoringQueue struct{}

func (ignoringQueue) queue(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	return bridgev2.EventHandlingResultIgnored
}

// processEventsHarness runs the connector's real processEvents loop over a fake
// Mattermost WebSocket feed.
type processEventsHarness struct {
	*lifecycleHarness
	feed chan any
	done chan struct{}
}

func newProcessEventsHarness(t *testing.T, opts lifecycleOptions) *processEventsHarness {
	t.Helper()
	h := newLifecycleHarness(t, opts)
	feed := make(chan any, 64)
	h.client.mu.Lock()
	h.client.client = &mattermost.Client{Events: feed}
	h.client.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.client.processEvents()
	}()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			close(feed)
			<-done
		}
	})
	return &processEventsHarness{lifecycleHarness: h, feed: feed, done: done}
}

// deliver feeds events through the real loop in order and returns once
// processEvents has handled all of them (it returns when the feed closes).
func (p *processEventsHarness) deliver(events ...any) {
	for _, ev := range events {
		p.feed <- ev
	}
	close(p.feed)
	<-p.done
}

// queuedTypes lists the types of everything the connector queued, in order.
func (h *lifecycleHarness) queuedTypes() []bridgev2.RemoteEventType {
	var out []bridgev2.RemoteEventType
	for _, ev := range h.events.drain() {
		out = append(out, ev.GetType())
	}
	return out
}

// applyChecks drains the queue like Portal.eventLoop does and applies every
// neutral check. Event types the framework handles itself (ChatInfoChange and
// friends) are not the harness's business and are skipped.
func (h *lifecycleHarness) applyChecks(t *testing.T) []deferredDeleteResult {
	t.Helper()
	var results []deferredDeleteResult
	for _, ev := range h.events.drain() {
		check, ok := ev.(*mmChannelDeleteCheckEvent)
		if !ok {
			if ev.GetType() == bridgev2.RemoteEventChatDelete {
				t.Fatalf("connector queued %T for the framework to apply directly; only the neutral check may be queued", ev)
			}
			continue
		}
		results = append(results, h.client.applyDeferredChannelDelete(t.Context(), h.sink, *check))
	}
	return results
}

// ──────────────────────────────────────────────────────────────────────────────
// processEvents wiring
// ──────────────────────────────────────────────────────────────────────────────

// TestProcessEventsChannelDeletedQueuesNeutralCheckOnly pins the hint path as it
// really runs: a ChannelDeletedEvent off the WebSocket queues exactly one neutral
// check, performs no I/O and queues no RemoteChatDelete.
func TestProcessEventsChannelDeletedQueuesNeutralCheckOnly(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	p.deliver(&mattermost.ChannelDeletedEvent{ChannelID: "chan-ws"})

	// A delete hint queues the neutral delete check plus the neutral access
	// sync (an archived channel stays, but read-only); nothing else.
	if n := p.events.len(); n != 2 {
		t.Fatalf("processEvents queued %d events, want the delete check and the access sync", n)
	}
	checks := p.events.checks()
	if len(checks) != 1 || checks[0].channelID != "chan-ws" {
		t.Fatalf("checks = %+v, want one check for chan-ws", checks)
	}
	if acc := p.events.accessSyncs(); len(acc) != 1 || acc[0].channelID != "chan-ws" {
		t.Fatalf("access syncs = %+v, want one for chan-ws", acc)
	}
	if n := p.probeCount(); n != 0 {
		t.Errorf("the hint issued %d probes, want 0; the probe belongs to the dequeue", n)
	}
	if n := p.sink.count(); n != 0 {
		t.Errorf("the hint invoked the deletion path %d times, want 0", n)
	}
	if !p.client.isCheckQueued("chan-ws") {
		t.Error("the queued check must hold the per-channel slot")
	}

	// And the check still decides at dequeue: proof deletes once.
	results := p.applyChecks(t)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("verdicts = %+v, want one applied deletion", results)
	}
	if n := p.sink.count(); n != 1 {
		t.Fatalf("framework deletion path invoked %d times, want 1", n)
	}
}

// TestProcessEventsBenignUpdateDoesNotCancelDeletion is the liveness regression:
// a routine channel_updated (delete_at == 0) that lands between the hint and the
// dequeue must not cancel the pending check. The channel is gone on the server,
// the probe says so, and the portal must still be deleted.
func TestProcessEventsBenignUpdateDoesNotCancelDeletion(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	p.deliver(
		&mattermost.ChannelDeletedEvent{ChannelID: "chan-meta"},
		// Rename/header change for the same channel, still delete_at == 0 as
		// far as this event is concerned.
		&mattermost.ChannelUpdatedEvent{Channel: &mattermost.Channel{
			ID: "chan-meta", DisplayName: "renamed", DeleteAt: 0,
		}},
	)

	if !p.client.isCheckQueued("chan-meta") {
		t.Fatal("a benign channel_updated cancelled the pending deletion check; the deletion can now never be evaluated")
	}
	if got := p.client.channelGeneration("chan-meta"); got == 0 {
		t.Fatal("the hint must have recorded a lifecycle generation")
	}

	results := p.applyChecks(t)
	if len(results) != 1 {
		t.Fatalf("got %d verdicts, want 1", len(results))
	}
	if !results[0].Applied || results[0].Reason != "channel-permanently-deleted" {
		t.Fatalf("verdict = %+v, want applied channel-permanently-deleted", results[0])
	}
	if n := p.sink.count(); n != 1 {
		t.Fatalf("framework deletion path invoked %d times, want 1", n)
	}
	if n := p.probeCount(); n != 1 {
		t.Errorf("issued %d probes, want exactly 1", n)
	}
}

// TestProcessEventsBenignUpdateStillForwardsChatInfo pins that the fix does not
// stop the update from being bridged: the ChatInfoChange is still queued.
func TestProcessEventsBenignUpdateStillForwardsChatInfo(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: archivedProbe})
	p.deliver(&mattermost.ChannelUpdatedEvent{Channel: &mattermost.Channel{
		ID: "chan-info", DisplayName: "new name", DeleteAt: 0,
	}})

	types := p.queuedTypes()
	if len(types) != 1 || types[0] != bridgev2.RemoteEventChatInfoChange {
		t.Fatalf("queued %v, want exactly [chat info change]", types)
	}
}

// TestProcessEventsRestoreStillCancelsDeletion keeps the opposite guarantee: a
// real restore (channel_restored) must still invalidate the pending check.
func TestProcessEventsRestoreStillCancelsDeletion(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	p.deliver(
		&mattermost.ChannelDeletedEvent{ChannelID: "chan-restored"},
		&mattermost.ChannelRestoredEvent{ChannelID: "chan-restored"},
	)

	if p.client.isCheckQueued("chan-restored") {
		t.Fatal("restore must clear the queued check")
	}
	results := p.applyChecks(t)
	if len(results) != 1 || results[0].Applied {
		t.Fatalf("verdicts = %+v, want one unapplied verdict", results)
	}
	if results[0].Reason != "superseded-before-probe" {
		t.Errorf("reason = %q, want superseded-before-probe", results[0].Reason)
	}
	if n := p.probeCount(); n != 0 {
		t.Errorf("superseded check issued %d probes, want 0", n)
	}
	if n := p.sink.count(); n != 0 {
		t.Fatalf("framework deletion path invoked %d times after a restore, want 0", n)
	}
}

// TestProcessEventsArchivedUpdateDoesNotDelete pins that an archive-shaped
// channel_updated is still handled by the probe, not by the update.
func TestProcessEventsArchivedUpdateDoesNotDelete(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: archivedProbe})
	p.deliver(
		&mattermost.ChannelDeletedEvent{ChannelID: "chan-arch"},
		&mattermost.ChannelUpdatedEvent{Channel: &mattermost.Channel{
			ID: "chan-arch", DeleteAt: 1614556800000,
		}},
	)
	results := p.applyChecks(t)
	if len(results) != 1 || results[0].Applied {
		t.Fatalf("verdicts = %+v, want one unapplied verdict", results)
	}
	if n := p.sink.count(); n != 0 {
		t.Fatalf("archived channel invoked the deletion path %d times, want 0", n)
	}
}

// TestProcessEventsSystemRestorePostCancelsDeletion covers the other restore
// signal: the system_channel_restored post.
func TestProcessEventsSystemRestorePostCancelsDeletion(t *testing.T) {
	p := newProcessEventsHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	p.deliver(
		&mattermost.ChannelDeletedEvent{ChannelID: "chan-post"},
		&mattermost.PostedEvent{
			ChannelID:   "chan-post",
			ChannelType: "O",
			ChannelName: "town-square",
			Post:        &mattermost.Post{ID: "p1", Type: "system_channel_restored"},
		},
	)
	if p.client.isCheckQueued("chan-post") {
		t.Fatal("system_channel_restored must clear the queued check")
	}
	results := p.applyChecks(t)
	if len(results) != 1 || results[0].Applied {
		t.Fatalf("verdicts = %+v, want one unapplied verdict", results)
	}
	if n := p.sink.count(); n != 0 {
		t.Fatalf("framework deletion path invoked %d times, want 0", n)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Queue failures must not wedge the per-channel slot
// ──────────────────────────────────────────────────────────────────────────────

// TestQueueFailureReleasesPendingCheck is the liveness regression for the slot:
// when bridgev2 refuses the neutral event (no portal row yet, lookup error), the
// check will never run, so the slot must be released and a later hint must be
// able to queue a fresh check.
func TestQueueFailureReleasesPendingCheck(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	reject := &rejectingQueue{err: errors.New("portal not found")}
	h.client.queueRemoteEventFn = reject.queue

	h.client.handleChannelDeleted("chan-unbridged")
	if reject.count() != 1 {
		t.Fatalf("queue attempts = %d, want 1", reject.count())
	}
	if h.client.isCheckQueued("chan-unbridged") {
		t.Fatal("a check the framework refused still holds the per-channel slot; every later hint will be dropped")
	}

	// A later hint must be able to try again ...
	h.client.handleChannelDeleted("chan-unbridged")
	if reject.count() != 2 {
		t.Fatalf("queue attempts = %d, want 2; the failed check wedged the channel", reject.count())
	}

	// ... and once the queue accepts the event, the check runs and decides.
	h.client.queueRemoteEventFn = h.events.queue
	h.client.handleChannelDeleted("chan-unbridged")
	results := h.applyChecks(t)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("verdicts = %+v, want one applied deletion", results)
	}
	if n := h.sink.count(); n != 1 {
		t.Fatalf("framework deletion path invoked %d times, want 1", n)
	}
}

// TestQueueIgnoredReleasesPendingCheck covers the other non-queued result: the
// portal is already gone, so the check will never run and must not hold the slot.
func TestQueueIgnoredReleasesPendingCheck(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.queueRemoteEventFn = ignoringQueue{}.queue
	h.client.handleChannelDeleted("chan-gone-already")
	if h.client.isCheckQueued("chan-gone-already") {
		t.Fatal("an ignored (never to be handled) check must not hold the per-channel slot")
	}
}

// TestQueueDeferredCheckBoundedWhileQueued pins that the burst bound still works
// when the queue accepts the check (the slot is only released on dequeue).
func TestQueueDeferredCheckBoundedWhileQueued(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.handleChannelDeleted("chan-burst")
	h.client.handleChannelDeleted("chan-burst")
	h.client.handleChannelDeleted("chan-burst")
	if n := h.events.len(); n != 1 {
		t.Fatalf("burst queued %d checks, want 1", n)
	}
	h.applyChecks(t)
	if h.client.isCheckQueued("chan-burst") {
		t.Fatal("the slot must be released once the check was dequeued")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// The framework's deletion path may refuse
// ──────────────────────────────────────────────────────────────────────────────

// TestSinkFailurePreservesPortalAndAllowsRetry covers the ignored result of
// PortalInternals.HandleRemoteChatDelete: when the framework reports failure,
// the deletion is not applied, the commit must be undone (otherwise every later
// attempt is suppressed) and the portal must be preserved.
func TestSinkFailurePreservesPortalAndAllowsRetry(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	failed := bridgev2.EventHandlingResultFailed.WithError(errors.New("room delete failed"))
	h.sink.result = &failed

	h.client.handleChannelDeleted("chan-refused-delete")
	results := h.applyChecks(t)

	if len(results) != 1 {
		t.Fatalf("got %d verdicts, want 1", len(results))
	}
	if results[0].Applied {
		t.Fatalf("verdict = %+v, reported applied although the framework refused", results[0])
	}
	if !strings.Contains(results[0].Reason, "sink") {
		t.Errorf("reason = %q, want it to name the framework sink", results[0].Reason)
	}
	if h.client.isChannelDeleted("chan-refused-delete") {
		t.Fatal("a refused deletion must not stay marked as deleted; a later hint could never retry")
	}

	// The room is still there, so a later authoritative check must be able to
	// delete it once the framework accepts.
	h.sink.result = nil
	h.client.handleChannelDeleted("chan-refused-delete")
	results = h.applyChecks(t)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("verdicts = %+v, want one applied retry", results)
	}
	if n := h.sink.count(); n != 2 {
		t.Fatalf("framework deletion path invoked %d times, want 2 (refused, then applied)", n)
	}
	if !h.client.isChannelDeleted("chan-refused-delete") {
		t.Error("the successful retry must leave the deletion mark")
	}
}

// TestSinkIgnoredIsTreatedAsApplied pins that EventHandlingResultIgnored (the
// portal was already deleted by another path) is not reported as a failure.
func TestSinkIgnoredIsTreatedAsApplied(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	ignored := bridgev2.EventHandlingResultIgnored
	h.sink.result = &ignored
	h.client.handleChannelDeleted("chan-already-gone")
	results := h.applyChecks(t)
	if len(results) != 1 || !results[0].Applied {
		t.Fatalf("verdicts = %+v, want applied (ignored means the portal is already gone)", results)
	}
	if !h.client.isChannelDeleted("chan-already-gone") {
		t.Error("an ignored deletion still means the portal is gone; the mark should stay")
	}
}
