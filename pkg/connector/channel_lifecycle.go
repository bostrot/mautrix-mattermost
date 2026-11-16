package connector

import (
	"context"
	"errors"
	"net/http"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// This file implements the channel lifecycle (archive/delete/restore) handling.
//
// Design rules, in priority order:
//
//  1. Archiving is reversible in Mattermost and is NOT a deletion. A channel
//     whose delete_at is set (archived) always keeps its portal and history.
//  2. Removing a portal is irreversible for the bridge, so a destructive event
//     is only produced on positive, authoritative proof that this exact channel
//     no longer exists on the server: an authenticated GET
//     /api/v4/channels/{id} answered 404 with Mattermost's own
//     app.channel.get.existing.app_error id and a consistent status_code.
//     Anything else - 403, 5xx, transport errors, untyped errors, a generic or
//     proxy 404, a mismatched error id/path/id, an unauthenticated probe -
//     preserves the portal (fail closed).
//  3. A `channel_deleted` WebSocket event is a hint, not proof. It is sent for
//     restorable archives too, and delivery can race a restore. It therefore
//     never queues a deletion. It queues a neutral placeholder event
//     (mmChannelDeleteCheckEvent) whose type is RemoteEventUnknown and which
//     implements RemotePreHandler.
//  4. The deletion decision is taken when the framework dequeues that neutral
//     event on the portal's own event lane (PreHandle), not when the hint
//     arrived. bridgev2 calls PreHandle before its type switch and then ignores
//     an unknown event, so the placeholder itself is inert: it can only delete
//     by explicitly invoking the framework's real room-deletion path.
//  5. PreHandle never re-queues. A check that is not proven deletes nothing and
//     is not retried; the next channel_deleted hint starts a fresh check.
//  6. Two bridge configurations are refused outright because the dequeue
//     decision can no longer be ordered against restores on this lane:
//     AsyncEvents=true (per-portal events run concurrently and out of order)
//     and bridgev2.PortalEventBuffer==0 (events are handled inline in the
//     caller's goroutine). In both cases the portal is preserved.
//
// Residual risk, stated plainly: this is not a proof of safety. A restore that
// reaches Mattermost after the dequeue-time probe has answered, but before the
// portal deletion is applied, still loses the room. What the design removes is
// the unbounded window - the previous shape queued a deletion whose verdict
// could be arbitrarily old by the time the framework processed it. Now the
// verdict is taken in the portal lane itself, with one probe, one commit under
// the lifecycle lock, and a generation check that aborts probes overtaken by a
// restore.

// maxDeferredDeletionProbes is the number of REST probes a single deferred
// check may issue. One is a hard bound on the work a channel_deleted hint can
// cause on the portal lane; there are no retries, because a failed probe is not
// evidence and must not be retried into one.
const maxDeferredDeletionProbes = 1

var (
	// errNilRemoteEvent is returned when a caller tries to queue a nil event.
	errNilRemoteEvent = errors.New("cannot queue a nil remote event")
	// errNoEventQueue is returned when the client has no user login to deliver
	// through, so the event would otherwise be dropped silently.
	errNoEventQueue = errors.New("no event queue attached")
)

// portalDeleteSink is the part of bridgev2.Portal the deferred deletion
// decision needs. *bridgev2.PortalInternals satisfies it, so production always
// deletes through the framework's own room deletion path
// (Portal.handleRemoteChatDelete) rather than a connector-local shortcut.
type portalDeleteSink interface {
	HandleRemoteChatDelete(ctx context.Context, source *bridgev2.UserLogin, evt bridgev2.RemoteChatDelete) bridgev2.EventHandlingResult
}

var _ portalDeleteSink = (*bridgev2.PortalInternals)(nil)

// mmChannelDeleteCheckEvent is the neutral placeholder queued for a
// channel_deleted hint.
//
// Its type is deliberately RemoteEventUnknown: bridgev2 will call PreHandle (the
// dequeue-time hook) and then ignore the event, so it never reaches
// Portal.handleRemoteChatDelete on its own. It is not a RemoteChatDelete and
// cannot be treated as one by the framework.
type mmChannelDeleteCheckEvent struct {
	client *MattermostClient
	// portalKey is the portal the hint was about; it is also the key the
	// eventual deletion event carries.
	portalKey networkid.PortalKey
	// channelID is the Mattermost channel the hint named, kept separately so
	// the check can refuse a key/id mismatch.
	channelID string
	// generation is the lifecycle generation observed when the check was
	// queued. A restore (or any other lifecycle event) bumps it, which makes
	// this check stale.
	generation uint64
}

func (e *mmChannelDeleteCheckEvent) GetType() bridgev2.RemoteEventType {
	return bridgev2.RemoteEventUnknown
}
func (e *mmChannelDeleteCheckEvent) GetPortalKey() networkid.PortalKey { return e.portalKey }
func (e *mmChannelDeleteCheckEvent) AddLogContext(c zerolog.Context) zerolog.Context {
	return c.Str("channel_id", e.channelID).Str("lifecycle", "delete-check")
}
func (e *mmChannelDeleteCheckEvent) GetSender() bridgev2.EventSender { return bridgev2.EventSender{} }

// PreHandle is called by bridgev2 when the neutral event is dequeued on the
// portal lane. That is the whole point of this event: the deletion decision is
// taken here, late, against the server's current answer - not when the hint
// arrived.
//
// It never queues another event. If the answer is not authoritative proof, the
// portal simply stays.
func (e *mmChannelDeleteCheckEvent) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	if e == nil || e.client == nil || portal == nil {
		return
	}
	e.client.applyDeferredChannelDelete(ctx, portal.Internal(), *e)
}

// deferredDeleteResult is the auditable outcome of one dequeue-time check.
type deferredDeleteResult struct {
	// Applied is true only when the framework's deletion handler was invoked.
	Applied bool
	// Reason is a short stable token for logs and tests.
	Reason string
}

// queueEvent delivers a remote event to the bridge's event queue.
//
// It routes through queueRemoteEventFn when a test installs one, and otherwise
// uses the attached user login. A missing login (unit tests, pre-login) simply
// drops the event: there is no queue to deliver to yet.
//
// The framework's answer is returned because it is the only way to know whether
// the event will ever be handled: bridgev2 answers EventHandlingResultFailed
// when it cannot find the portal (ErrPortalNotFoundInEventHandler) and
// EventHandlingResultIgnored when the portal was already deleted.
func (c *MattermostClient) queueEvent(ev bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	if ev == nil {
		return bridgev2.EventHandlingResultFailed.WithError(errNilRemoteEvent)
	}
	if c.queueRemoteEventFn != nil {
		return c.queueRemoteEventFn(ev)
	}
	if c.UserLogin == nil {
		return bridgev2.EventHandlingResultFailed.WithError(errNoEventQueue)
	}
	return c.UserLogin.QueueRemoteEvent(ev)
}

// lifecycleLogger returns a logger that works with or without an attached login.
func (c *MattermostClient) lifecycleLogger() zerolog.Logger {
	if c.UserLogin != nil {
		return c.UserLogin.Log
	}
	return zerolog.Nop()
}

// receiverID is the user login ID used in portal keys, or "" when unknown.
func (c *MattermostClient) receiverID() networkid.UserLoginID {
	if c.UserLogin == nil {
		return ""
	}
	return c.UserLogin.ID
}

// deferredDeletionSupported reports whether this bridge configuration lets the
// neutral event's dequeue-time decision be ordered against restores. When it
// returns false the portal is preserved and the reason is logged.
func (c *MattermostClient) deferredDeletionSupported() (bool, string) {
	if c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.Config == nil {
		// No bridge means no portal lane and no configuration we can reason
		// about; never treat that as permission to delete.
		return false, "bridge-config-unavailable"
	}
	if c.UserLogin.Bridge.Config.AsyncEvents {
		// Each portal event is handled in its own goroutine, so a restore
		// queued after this check can be handled before it (or concurrently).
		return false, "async-events-unsupported"
	}
	if bridgev2.PortalEventBuffer == 0 {
		// Events are handled inline in whatever goroutine queued them, which
		// removes the lane boundary this design relies on.
		return false, "portal-event-buffer-inline"
	}
	return true, ""
}

// channelLifecycleAction is the outcome of verifying a channel probe.
type channelLifecycleAction int

const (
	// lifecyclePreserve leaves the Matrix portal and its history alone. This is
	// the fail-closed outcome used whenever the server's answer is not proof
	// that this exact channel was deleted.
	lifecyclePreserve channelLifecycleAction = iota
	// lifecycleDeletePortal hands the portal to bridgev2's room deletion path.
	lifecycleDeletePortal
)

func (a channelLifecycleAction) String() string {
	if a == lifecycleDeletePortal {
		return "delete-portal"
	}
	return "preserve"
}

// decideChannelDeletion classifies a completed channel probe into a destructive
// decision plus a short reason suitable for logs and tests.
//
// It is deliberately a pure function so the policy is auditable without a
// network or a bridge. Deletion is returned for exactly one shape of evidence:
// an authenticated probe of this exact channel ID whose typed HTTP error is
// Mattermost's canonical channel-not-found answer. Every other input preserves.
//
// The caller is still responsible for the ordering guards (generation check,
// commit under lifecycleMu); this function only answers "is this proof?".
func decideChannelDeletion(channelID string, p mattermost.ChannelProbe) (channelLifecycleAction, string) {
	if channelID == "" {
		return lifecyclePreserve, "empty-channel-id"
	}
	if !p.Authenticated {
		return lifecyclePreserve, "probe-unauthenticated"
	}
	switch p.State {
	case mattermost.ChannelStateArchived:
		// Soft delete / archive: restorable, so the portal and its history stay.
		return lifecyclePreserve, "channel-archived"
	case mattermost.ChannelStateMissing:
		if p.Channel != nil {
			return lifecyclePreserve, "missing-probe-carried-channel"
		}
		if p.StatusCode != http.StatusNotFound {
			return lifecyclePreserve, "missing-probe-unexpected-status"
		}
		var httpErr *mattermost.HTTPError
		if !errors.As(p.Err, &httpErr) {
			return lifecyclePreserve, "missing-probe-untyped-error"
		}
		if !httpErr.IsAuthoritativeChannelNotFound(channelID) {
			return lifecyclePreserve, "http-404-not-authoritative"
		}
		return lifecycleDeletePortal, "channel-permanently-deleted"
	case mattermost.ChannelStateActive:
		return lifecyclePreserve, "channel-active"
	default:
		if p.StatusCode != 0 {
			return lifecyclePreserve, "probe-unknown-http-status"
		}
		return lifecyclePreserve, "probe-unknown"
	}
}

// bumpChannelGeneration records a new lifecycle event for a channel and returns
// the generation assigned to it. Checks and verdicts started under an older
// generation are discarded, because the channel state has moved on since.
func (c *MattermostClient) bumpChannelGeneration(channelID string) uint64 {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.bumpChannelGenerationLocked(channelID)
}

// bumpChannelGenerationLocked is bumpChannelGeneration for callers that already
// hold lifecycleMu.
func (c *MattermostClient) bumpChannelGenerationLocked(channelID string) uint64 {
	if c.channelGen == nil {
		c.channelGen = make(map[string]uint64)
	}
	c.channelGen[channelID]++
	return c.channelGen[channelID]
}

// channelGeneration returns the current lifecycle generation for a channel.
func (c *MattermostClient) channelGeneration(channelID string) uint64 {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.channelGen[channelID]
}

// noteChannelAlive records that the channel was seen alive (created, updated
// while live, restored): it bumps the generation so pending checks and their
// in-flight probes become stale, drops any queued check so a later delete can
// still be evaluated, and clears the deletion mark so a later delete is not
// suppressed. It takes the same lock as the deletion commit, so a restore can
// never interleave with a verdict being applied.
func (c *MattermostClient) noteChannelAlive(channelID string) uint64 {
	if channelID == "" {
		return 0
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	delete(c.deletedChannels, channelID)
	delete(c.pendingChecks, channelID)
	return c.bumpChannelGenerationLocked(channelID)
}

// markCheckQueued records that a deferred check is already queued for the
// channel. It returns false when one already is, which bounds the number of
// probes a burst of channel_deleted hints can cause: at most one queued check
// per channel per lifecycle generation, each issuing at most
// maxDeferredDeletionProbes probes.
func (c *MattermostClient) markCheckQueued(channelID string) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.pendingChecks == nil {
		c.pendingChecks = make(map[string]bool)
	}
	if c.pendingChecks[channelID] {
		return false
	}
	c.pendingChecks[channelID] = true
	return true
}

// clearCheckQueued forgets a queued check (it is being evaluated now, or the
// channel was restored).
func (c *MattermostClient) clearCheckQueued(channelID string) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	delete(c.pendingChecks, channelID)
}

// commitChannelDelete marks the channel deleted and re-verifies the generation
// in one critical section, so a restore cannot interleave between "the probe
// said gone" and "the deletion is committed". It returns false when the check
// is stale or a deletion was already committed.
func (c *MattermostClient) commitChannelDelete(channelID string, gen uint64) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.channelGen[channelID] != gen {
		return false
	}
	if c.deletedChannels[channelID] {
		return false
	}
	if c.deletedChannels == nil {
		c.deletedChannels = make(map[string]bool)
	}
	c.deletedChannels[channelID] = true
	return true
}

// clearChannelDeleted removes the deletion mark (used when the channel is
// restored).
func (c *MattermostClient) clearChannelDeleted(channelID string) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	delete(c.deletedChannels, channelID)
}

// isChannelDeleted reports whether a portal deletion was already committed.
func (c *MattermostClient) isChannelDeleted(channelID string) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.deletedChannels[channelID]
}

// isCheckQueued reports whether a deferred deletion check is queued for the
// channel. Tests use it; production code only needs the bound it enforces.
func (c *MattermostClient) isCheckQueued(channelID string) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.pendingChecks[channelID]
}

// probeChannel reads the current lifecycle state of a channel over REST.
func (c *MattermostClient) probeChannel(channelID string) mattermost.ChannelProbe {
	if c.probeChannelFn != nil {
		return c.probeChannelFn(c.serverURL(), c.Token, channelID)
	}
	return mattermost.ProbeChannel(c.serverURL(), c.Token, channelID)
}

// handleChannelDeleted reacts to a channel_deleted WebSocket hint.
//
// It performs no I/O and queues no deletion. It queues one neutral check event
// (bounded to one per channel per generation) whose PreHandle - called when the
// portal lane dequeues it - takes the actual decision. Configurations where the
// lane cannot be ordered against restores are refused here as well.
func (c *MattermostClient) handleChannelDeleted(channelID string) {
	if channelID == "" {
		return
	}
	log := c.lifecycleLogger()
	c.lifecycleMu.Lock()
	if c.deletedChannels[channelID] || c.pendingChecks[channelID] {
		c.lifecycleMu.Unlock()
		return
	}
	gen := c.bumpChannelGenerationLocked(channelID)
	if c.pendingChecks == nil {
		c.pendingChecks = make(map[string]bool)
	}
	c.pendingChecks[channelID] = true
	c.lifecycleMu.Unlock()
	if ok, reason := c.deferredDeletionSupported(); !ok {
		c.clearCheckQueued(channelID)
		log.Warn().
			Str("channel_id", channelID).
			Str("reason", reason).
			Msg("Deferred channel deletion check refused by bridge configuration, preserving portal")
		return
	}
	log.Debug().
		Str("channel_id", channelID).
		Uint64("generation", gen).
		Msg("Queued deferred channel deletion check")
	res := c.queueEvent(&mmChannelDeleteCheckEvent{
		client:     c,
		portalKey:  networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: c.receiverID()},
		channelID:  channelID,
		generation: gen,
	})
	if res.Queued {
		return
	}
	// The framework will never run this check: it refused the event (no portal
	// row for the channel, portal lookup error) or dropped it because the
	// portal was already deleted. Holding the per-channel slot would wedge the
	// channel for the rest of the process, because Mattermost sends
	// channel_deleted once, so release it and let a later hint try again.
	// Nothing is re-queued here: re-queueing on failure would spin.
	c.clearCheckQueued(channelID)
	log.Warn().
		Err(res.Error).
		Str("channel_id", channelID).
		Uint64("generation", gen).
		Msg("Framework did not queue the deferred channel deletion check, released the per-channel slot")
}

// applyDeferredChannelDelete takes the deletion decision at dequeue time.
//
// It is called from mmChannelDeleteCheckEvent.PreHandle, i.e. from the portal's
// own event lane, with the framework's real deletion path as sink. The guards,
// in order:
//
//  1. the bridge configuration must allow an ordered dequeue (fail closed);
//  2. the event's channel and portal key must agree;
//  3. the generation captured at queue time must still be current before the
//     probe (a restore already landed);
//  4. exactly one probe is issued (maxDeferredDeletionProbes);
//  5. the generation must still be current after the probe (a restore landed
//     while the probe was in flight - the verdict is about a state the channel
//     has left, so it is dropped);
//  6. only authoritative proof is allowed to commit, and the commit re-checks
//     the generation under the lifecycle lock.
//
// Nothing is re-queued and nothing is retried: a check that does not delete is
// finished.
func (c *MattermostClient) applyDeferredChannelDelete(ctx context.Context, sink portalDeleteSink, ev mmChannelDeleteCheckEvent) deferredDeleteResult {
	log := zerolog.Ctx(ctx)
	if sink == nil {
		return deferredDeleteResult{Reason: "no-portal-sink"}
	}
	// This check is being consumed, so it no longer holds the per-channel slot.
	c.clearCheckQueued(ev.channelID)

	if ok, reason := c.deferredDeletionSupported(); !ok {
		log.Warn().
			Str("channel_id", ev.channelID).
			Str("reason", reason).
			Msg("Deferred channel deletion check refused by bridge configuration, preserving portal")
		return deferredDeleteResult{Reason: reason}
	}
	if ev.channelID == "" || ev.channelID != string(ev.portalKey.ID) {
		return deferredDeleteResult{Reason: "channel-portal-mismatch"}
	}
	if c.channelGeneration(ev.channelID) != ev.generation {
		log.Debug().
			Str("channel_id", ev.channelID).
			Msg("Channel lifecycle event superseded the deferred check before its probe, preserving portal")
		return deferredDeleteResult{Reason: "superseded-before-probe"}
	}

	var probe mattermost.ChannelProbe
	for i := 0; i < maxDeferredDeletionProbes; i++ {
		probe = c.probeChannel(ev.channelID)
	}

	// Abort a probe overtaken by a restore: its verdict describes a state the
	// channel has already left.
	if c.channelGeneration(ev.channelID) != ev.generation {
		log.Debug().
			Str("channel_id", ev.channelID).
			Msg("Channel lifecycle event superseded the deferred check during its probe, dropping verdict")
		return deferredDeleteResult{Reason: "superseded-during-probe"}
	}

	action, reason := decideChannelDeletion(ev.channelID, probe)
	if action != lifecycleDeletePortal {
		log.Warn().
			Str("channel_id", ev.channelID).
			Str("reason", reason).
			Str("probe_state", probe.State.String()).
			Int("status_code", probe.StatusCode).
			Msg("Channel deletion not proven at dequeue, preserving portal and history")
		return deferredDeleteResult{Reason: reason}
	}

	if c.UserID == "" || probe.AuthenticatedUserID != c.UserID {
		log.Warn().Str("channel_id", ev.channelID).Msg("Channel deletion probe identity mismatch; preserving portal")
		return deferredDeleteResult{Reason: "probe-identity-mismatch"}
	}

	if !c.commitChannelDelete(ev.channelID, ev.generation) {
		log.Debug().
			Str("channel_id", ev.channelID).
			Msg("Channel deletion check lost the commit to a newer lifecycle event, preserving portal")
		return deferredDeleteResult{Reason: "superseded-at-commit"}
	}

	log.Info().
		Str("channel_id", ev.channelID).
		Str("reason", reason).
		Msg("Channel deletion confirmed at dequeue, deleting portal")
	// The only destructive call in this file, and it goes through the
	// framework's own room deletion path. No event is re-queued.
	res := sink.HandleRemoteChatDelete(ctx, c.UserLogin, &mmChatDeleteEvent{portalKey: ev.portalKey})
	if !res.Success {
		// The framework refused the deletion, so the room is still there. The
		// commit above is undone: keeping the mark would suppress every later
		// attempt for the life of the process. The portal is preserved, which
		// is the fail-closed outcome.
		c.clearChannelDeleted(ev.channelID)
		log.Error().
			Err(res.Error).
			Str("channel_id", ev.channelID).
			Msg("Framework refused the portal deletion, undid the deletion mark and preserved the portal")
		return deferredDeleteResult{Reason: "sink-failed"}
	}
	return deferredDeleteResult{Applied: true, Reason: reason}
}
