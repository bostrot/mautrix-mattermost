package connector

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// ──────────────────────────────────────────────────────────────────────────────
// The user-visible contract these tests pin down:
//
//   * archiving (delete_at != 0, HTTP 200) is reversible and MUST keep the
//     portal and its history;
//   * a channel_deleted hint queues no deletion at all - only a neutral
//     mmChannelDeleteCheckEvent (RemoteEventUnknown + PreHandle);
//   * the deletion decision is taken when that event is dequeued, by
//     applyDeferredChannelDelete, and only an authenticated probe of this exact
//     channel that answered 404 with Mattermost's canonical
//     app.channel.get.existing.app_error id may delete;
//   * everything else (403, 401, 5xx, transport failure, generic/proxy 404,
//     mismatched path or error id, unauthenticated probe, unexpected status,
//     in-flight restore, unsupported bridge config) preserves the portal;
//   * duplicate hints produce at most one queued check and at most one
//     deletion.
//
// The framework's Portal.eventLoop is modelled by lifecycleHarness.dequeue:
// events are taken off the queue in order and PreHandle's observable work
// (applyDeferredChannelDelete with the framework's deletion path as sink) is
// performed for each neutral check. Nothing else is applied, which is exactly
// what bridgev2 does with an unknown event type.
// ──────────────────────────────────────────────────────────────────────────────

type probeFunc func(channelID string) mattermost.ChannelProbe

// eventRecorder is the test stand-in for UserLogin.QueueRemoteEvent.
type eventRecorder struct {
	mu     sync.Mutex
	events []bridgev2.RemoteEvent
	// result overrides the framework's answer (the default is Queued, which is
	// what bridgev2 returns when the event reached the portal lane).
	result *bridgev2.EventHandlingResult
}

func (r *eventRecorder) queue(ev bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	if r.result != nil {
		return *r.result
	}
	return bridgev2.EventHandlingResultQueued
}

func (r *eventRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// drain returns the queued events in FIFO order and empties the queue.
func (r *eventRecorder) drain() []bridgev2.RemoteEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

func (r *eventRecorder) checks() []*mmChannelDeleteCheckEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*mmChannelDeleteCheckEvent
	for _, ev := range r.events {
		if check, ok := ev.(*mmChannelDeleteCheckEvent); ok {
			out = append(out, check)
		}
	}
	return out
}

// chatDeletes reports RemoteChatDelete events on the queue. A channel_deleted
// hint must never put one there.
func (r *eventRecorder) chatDeletes() []*mmChatDeleteEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*mmChatDeleteEvent
	for _, ev := range r.events {
		if del, ok := ev.(*mmChatDeleteEvent); ok {
			out = append(out, del)
		}
	}
	return out
}

type deleteCall struct {
	portalKey networkid.PortalKey
	source    *bridgev2.UserLogin
	onlyForMe bool
}

// fakePortalDeleteSink stands in for *bridgev2.PortalInternals: it records the
// framework's room-deletion call instead of touching a real portal.
type fakePortalDeleteSink struct {
	mu    sync.Mutex
	calls []deleteCall
	// result, when non-nil, is what the framework answers instead of success;
	// it models a refused or ignored deletion.
	result *bridgev2.EventHandlingResult
}

func (s *fakePortalDeleteSink) HandleRemoteChatDelete(_ context.Context, source *bridgev2.UserLogin, evt bridgev2.RemoteChatDelete) bridgev2.EventHandlingResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, deleteCall{portalKey: evt.GetPortalKey(), source: source, onlyForMe: evt.DeleteOnlyForMe()})
	if s.result != nil {
		return *s.result
	}
	return bridgev2.EventHandlingResultSuccess
}

func (s *fakePortalDeleteSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *fakePortalDeleteSink) last(t *testing.T) deleteCall {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		t.Fatal("the framework's deletion path was never invoked")
	}
	return s.calls[len(s.calls)-1]
}

type lifecycleOptions struct {
	probe probeFunc
	// noLogin leaves UserLogin nil (no bridge at all).
	noLogin bool
	// noBridgeConfig attaches a login whose bridge has no config.
	noBridgeConfig bool
	// asyncEvents sets Bridge.Config.AsyncEvents.
	asyncEvents bool
	// inlineEvents sets bridgev2.PortalEventBuffer to 0 for the test.
	inlineEvents bool
}

// lifecycleHarness wires a MattermostClient to a recording event queue, a fake
// portal-deletion sink and a probing stub, with a bridge configuration that
// permits the deferred dequeue decision by default.
type lifecycleHarness struct {
	client *MattermostClient
	events *eventRecorder
	sink   *fakePortalDeleteSink

	probeMu sync.Mutex
	probes  []string
}

func newLifecycleHarness(t *testing.T, opts lifecycleOptions) *lifecycleHarness {
	t.Helper()
	h := &lifecycleHarness{events: &eventRecorder{}, sink: &fakePortalDeleteSink{}}
	c := &MattermostClient{
		Token:     "tok",
		UserID:    "test-user",
		ServerURL: "http://unused.invalid",
		baseURL:   "http://unused.invalid",
	}
	c.queueRemoteEventFn = h.events.queue

	if !opts.noLogin {
		br := &bridgev2.Bridge{Log: zerolog.Nop()}
		if !opts.noBridgeConfig {
			br.Config = &bridgeconfig.BridgeConfig{AsyncEvents: opts.asyncEvents}
		}
		c.UserLogin = &bridgev2.UserLogin{
			UserLogin: &database.UserLogin{ID: "login-test"},
			Bridge:    br,
			Log:       zerolog.Nop(),
		}
	}
	if opts.probe != nil {
		c.probeChannelFn = func(_, _, channelID string) mattermost.ChannelProbe {
			h.recordProbe(channelID)
			return opts.probe(channelID)
		}
	}
	if opts.inlineEvents {
		previous := bridgev2.PortalEventBuffer
		bridgev2.PortalEventBuffer = 0
		t.Cleanup(func() { bridgev2.PortalEventBuffer = previous })
	}
	h.client = c
	return h
}

func (h *lifecycleHarness) recordProbe(channelID string) {
	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	h.probes = append(h.probes, channelID)
}

func (h *lifecycleHarness) probeCount() int {
	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	return len(h.probes)
}

// dequeue models bridgev2's portal lane: take queued events in order and let
// each neutral check make its decision. Anything else on the queue is a
// contract violation by the connector.
func (h *lifecycleHarness) dequeue(t *testing.T) []deferredDeleteResult {
	t.Helper()
	var results []deferredDeleteResult
	for _, ev := range h.events.drain() {
		switch typed := ev.(type) {
		case *mmChannelDeleteCheckEvent:
			results = append(results, h.client.applyDeferredChannelDelete(context.Background(), h.sink, *typed))
		case *mmChatDeleteEvent:
			t.Fatalf("channel_deleted hint queued a RemoteChatDelete (%v); only the neutral check may be queued", ev.GetType())
		default:
			t.Fatalf("unexpected queued event %T", ev)
		}
	}
	return results
}

// ──────────────────────────────────────────────────────────────────────────────
// Probe fixtures
// ──────────────────────────────────────────────────────────────────────────────

// archivedProbe is what ProbeChannel returns for a live server whose channel has
// delete_at != 0: HTTP 200, a decoded channel, and an authenticated caller.
func archivedProbe(channelID string) mattermost.ChannelProbe {
	return mattermost.ChannelProbe{
		State:         mattermost.ChannelStateArchived,
		StatusCode:    http.StatusOK,
		Channel:       &mattermost.Channel{ID: channelID, DeleteAt: 1614556800000},
		Authenticated: true,
	}
}

// activeProbe is a healthy, non-archived channel.
func activeProbe(channelID string) mattermost.ChannelProbe {
	return mattermost.ChannelProbe{
		State:         mattermost.ChannelStateActive,
		StatusCode:    http.StatusOK,
		Channel:       &mattermost.Channel{ID: channelID},
		Authenticated: true,
	}
}

// authoritativeNotFoundProbe is the only shape that may delete a portal:
// authenticated GET /api/v4/channels/{id} → 404 carrying Mattermost's
// canonical error id and a consistent body status_code.
func authoritativeNotFoundProbe(channelID string) mattermost.ChannelProbe {
	return mattermost.ChannelProbe{
		State:               mattermost.ChannelStateMissing,
		StatusCode:          http.StatusNotFound,
		Authenticated:       true,
		AuthenticatedUserID: "test-user",
		ErrorID:             mattermost.ErrIDChannelGetExisting,
		Err: &mattermost.HTTPError{
			StatusCode:     http.StatusNotFound,
			Path:           "/api/v4/channels/" + channelID,
			Body:           `{"id":"` + mattermost.ErrIDChannelGetExisting + `","message":"Unable to find the existing channel.","status_code":404}`,
			ErrorID:        mattermost.ErrIDChannelGetExisting,
			BodyStatusCode: http.StatusNotFound,
		},
	}
}

// httpProbe models any non-2xx response for an authenticated probe.
func httpProbe(status int) mattermost.ChannelProbe {
	state := mattermost.ChannelStateUnknown
	if status == http.StatusNotFound {
		state = mattermost.ChannelStateMissing
	}
	return mattermost.ChannelProbe{
		State:         state,
		StatusCode:    status,
		Authenticated: true,
		Err: &mattermost.HTTPError{
			StatusCode:     status,
			Path:           "/api/v4/channels/ch1",
			Body:           "denied",
			BodyStatusCode: status,
		},
	}
}

// transportErrProbe models "the server never answered" (token was sent).
func transportErrProbe() mattermost.ChannelProbe {
	return mattermost.ChannelProbe{
		State:         mattermost.ChannelStateUnknown,
		Authenticated: true,
		Err:           errors.New("dial tcp: connection refused"),
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// queueEvent
// ──────────────────────────────────────────────────────────────────────────────

func TestQueueEventUsesOverride(t *testing.T) {
	rec := &eventRecorder{}
	c := &MattermostClient{queueRemoteEventFn: rec.queue}
	c.queueEvent(&mmChatDeleteEvent{portalKey: networkid.PortalKey{ID: "ch"}})
	if rec.len() != 1 {
		t.Fatalf("recorded %d events, want 1", rec.len())
	}
}

func TestQueueEventWithoutLoginDoesNotPanic(t *testing.T) {
	c := &MattermostClient{}
	c.queueEvent(&mmChatDeleteEvent{portalKey: networkid.PortalKey{ID: "ch"}}) // no login attached
	c.queueEvent(nil)
}

// ──────────────────────────────────────────────────────────────────────────────
// decideChannelDeletion (pure policy)
// ──────────────────────────────────────────────────────────────────────────────

func TestDecideChannelDeletion(t *testing.T) {
	tests := []struct {
		name      string
		channelID string
		probe     mattermost.ChannelProbe
		want      channelLifecycleAction
	}{
		{
			name:      "archived channel is restorable and preserves the portal",
			channelID: "ch1",
			probe:     archivedProbe("ch1"),
			want:      lifecyclePreserve,
		},
		{
			name:      "archived is ignored when the channel id is empty",
			channelID: "",
			probe:     archivedProbe(""),
			want:      lifecyclePreserve,
		},
		{
			name:      "authoritative 404 deletes the portal",
			channelID: "ch1",
			probe:     authoritativeNotFoundProbe("ch1"),
			want:      lifecycleDeletePortal,
		},
		{
			name:      "authoritative 404 for a different channel id preserves",
			channelID: "ch1",
			probe:     authoritativeNotFoundProbe("other"),
			want:      lifecyclePreserve,
		},
		{
			name:      "generic 404 without a Mattermost error id preserves",
			channelID: "ch1",
			probe:     httpProbe(http.StatusNotFound),
			want:      lifecyclePreserve,
		},
		{
			name:      "404 with an unexpected error id preserves",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State:         mattermost.ChannelStateMissing,
				StatusCode:    http.StatusNotFound,
				Authenticated: true,
				Err: &mattermost.HTTPError{
					StatusCode:     http.StatusNotFound,
					Path:           "/api/v4/channels/ch1",
					Body:           `{"id":"app.channel.get.existing.app_error"}`,
					ErrorID:        "app.channel.app_error",
					BodyStatusCode: http.StatusNotFound,
				},
			},
			want: lifecyclePreserve,
		},
		{
			name:      "404 from a non-canonical path preserves",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State:         mattermost.ChannelStateMissing,
				StatusCode:    http.StatusNotFound,
				Authenticated: true,
				Err: &mattermost.HTTPError{
					StatusCode:     http.StatusNotFound,
					Path:           "/api/v4/channels/ch1/members",
					ErrorID:        mattermost.ErrIDChannelGetExisting,
					BodyStatusCode: http.StatusNotFound,
				},
			},
			want: lifecyclePreserve,
		},
		{
			name:      "404 with an inconsistent body status_code preserves",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State:         mattermost.ChannelStateMissing,
				StatusCode:    http.StatusNotFound,
				Authenticated: true,
				Err: &mattermost.HTTPError{
					StatusCode:     http.StatusNotFound,
					Path:           "/api/v4/channels/ch1",
					ErrorID:        mattermost.ErrIDChannelGetExisting,
					BodyStatusCode: http.StatusOK,
				},
			},
			want: lifecyclePreserve,
		},
		{
			name:      "404 with an untyped error preserves",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State:         mattermost.ChannelStateMissing,
				StatusCode:    http.StatusNotFound,
				Authenticated: true,
				Err:           errors.New("404 body not JSON"),
			},
			want: lifecyclePreserve,
		},
		{
			name:      "authoritative 404 from an unauthenticated probe preserves",
			channelID: "ch1",
			probe: func() mattermost.ChannelProbe {
				p := authoritativeNotFoundProbe("ch1")
				p.Authenticated = false
				return p
			}(),
			want: lifecyclePreserve,
		},
		{
			name:      "missing probe that carried a channel preserves",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State:         mattermost.ChannelStateMissing,
				StatusCode:    http.StatusNotFound,
				Authenticated: true,
				Channel:       &mattermost.Channel{ID: "ch1"},
				Err:           &mattermost.HTTPError{StatusCode: http.StatusNotFound, Path: "/api/v4/channels/ch1", ErrorID: mattermost.ErrIDChannelGetExisting, BodyStatusCode: http.StatusNotFound},
			},
			want: lifecyclePreserve,
		},
		{
			name:      "http 403 is not proof",
			channelID: "ch1",
			probe:     httpProbe(http.StatusForbidden),
			want:      lifecyclePreserve,
		},
		{
			name:      "http 401 is not proof",
			channelID: "ch1",
			probe:     httpProbe(http.StatusUnauthorized),
			want:      lifecyclePreserve,
		},
		{
			name:      "http 500 is not proof",
			channelID: "ch1",
			probe:     httpProbe(http.StatusInternalServerError),
			want:      lifecyclePreserve,
		},
		{
			name:      "unexpected http status is not proof",
			channelID: "ch1",
			probe:     httpProbe(http.StatusTeapot),
			want:      lifecyclePreserve,
		},
		{
			name:      "transport error is not proof",
			channelID: "ch1",
			probe:     transportErrProbe(),
			want:      lifecyclePreserve,
		},
		{
			name:      "active channel is preserved",
			channelID: "ch1",
			probe:     activeProbe("ch1"),
			want:      lifecyclePreserve,
		},
		{
			name:      "archived without a channel payload is not proof",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State: mattermost.ChannelStateArchived, StatusCode: http.StatusOK, Authenticated: true,
			},
			want: lifecyclePreserve,
		},
		{
			name:      "archived with a mismatched id is not proof",
			channelID: "ch1",
			probe:     archivedProbe("other"),
			want:      lifecyclePreserve,
		},
		{
			name:      "archived with zero delete_at is preserved",
			channelID: "ch1",
			probe: mattermost.ChannelProbe{
				State: mattermost.ChannelStateArchived, StatusCode: http.StatusOK, Authenticated: true,
				Channel: &mattermost.Channel{ID: "ch1", DeleteAt: 0},
			},
			want: lifecyclePreserve,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := decideChannelDeletion(tt.channelID, tt.probe)
			if got != tt.want {
				t.Fatalf("action = %s (%s), want %s", got, reason, tt.want)
			}
			if reason == "" {
				t.Error("reason must not be empty")
			}
		})
	}
}

func TestDecideChannelDeletionReasons(t *testing.T) {
	tests := []struct {
		name      string
		channelID string
		probe     mattermost.ChannelProbe
		want      string
	}{
		{"deleted", "ch1", authoritativeNotFoundProbe("ch1"), "channel-permanently-deleted"},
		{"archived", "ch1", archivedProbe("ch1"), "channel-archived"},
		{"active", "ch1", activeProbe("ch1"), "channel-active"},
		{"unauthenticated", "ch1", func() mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("ch1")
			p.Authenticated = false
			return p
		}(), "probe-unauthenticated"},
		{"unexpected missing status", "ch1", func() mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("ch1")
			p.StatusCode = http.StatusBadGateway
			return p
		}(), "missing-probe-unexpected-status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, reason := decideChannelDeletion(tt.channelID, tt.probe)
			if reason != tt.want {
				t.Fatalf("reason = %q, want %q", reason, tt.want)
			}
			if tt.want == "channel-permanently-deleted" && action != lifecycleDeletePortal {
				t.Fatalf("action = %s, want delete-portal", action)
			}
			if tt.want != "channel-permanently-deleted" && action != lifecyclePreserve {
				t.Fatalf("action = %s, want preserve", action)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Hint handling: a channel_deleted hint queues a neutral check, nothing else
// ──────────────────────────────────────────────────────────────────────────────

func TestChannelDeletedHintQueuesOnlyNeutralCheck(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-hint")

	if n := h.events.len(); n != 1 {
		t.Fatalf("hint queued %d events, want exactly 1 neutral check", n)
	}
	if deletes := h.events.chatDeletes(); len(deletes) != 0 {
		t.Fatalf("hint queued %d RemoteChatDelete events, want 0", len(deletes))
	}
	checks := h.events.checks()
	if len(checks) != 1 {
		t.Fatalf("queued %d checks, want 1", len(checks))
	}
	ev := checks[0]
	if ev.GetType() != bridgev2.RemoteEventUnknown {
		t.Errorf("check type = %v, want RemoteEventUnknown so bridgev2 ignores it after PreHandle", ev.GetType())
	}
	if _, ok := bridgev2.RemoteEvent(ev).(bridgev2.RemoteChatDelete); ok {
		t.Error("the neutral check must not be a RemoteChatDelete")
	}
	if _, ok := bridgev2.RemoteEvent(ev).(bridgev2.RemotePreHandler); !ok {
		t.Error("the neutral check must implement RemotePreHandler (the dequeue hook)")
	}
	if ev.channelID != "chan-hint" {
		t.Errorf("check channel = %q, want chan-hint", ev.channelID)
	}
	wantKey := networkid.PortalKey{ID: "chan-hint", Receiver: "login-test"}
	if ev.GetPortalKey() != wantKey {
		t.Errorf("portal key = %+v, want %+v", ev.GetPortalKey(), wantKey)
	}
	if ev.generation != h.client.channelGeneration("chan-hint") {
		t.Errorf("check generation = %d, want current generation %d", ev.generation, h.client.channelGeneration("chan-hint"))
	}
	if !h.client.isCheckQueued("chan-hint") {
		t.Error("the queued check must hold the per-channel slot")
	}
	// Delivering the hint must do no I/O and take no decision.
	if n := h.probeCount(); n != 0 {
		t.Errorf("hint issued %d probes, want 0 (the probe belongs to the dequeue)", n)
	}
	if n := h.sink.count(); n != 0 {
		t.Errorf("hint invoked the deletion path %d times, want 0", n)
	}
	if h.client.isChannelDeleted("chan-hint") {
		t.Error("a hint alone must never mark the channel deleted")
	}
}

// TestChannelDeletedArchivedPreservesPortal pins the headline regression: a
// channel_deleted event for an archived (delete_at != 0) channel must not
// destroy the portal, because archiving is reversible in Mattermost.
func TestChannelDeletedArchivedPreservesPortal(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-archived")
	results := h.dequeue(t)

	if len(results) != 1 || results[0].Applied {
		t.Fatalf("results = %+v, want one unapplied verdict", results)
	}
	if results[0].Reason != "channel-archived" {
		t.Errorf("reason = %q, want channel-archived", results[0].Reason)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times for an archived channel, want 0", n)
	}
	if h.client.isChannelDeleted("chan-archived") {
		t.Error("archived channel must not be marked as deleted")
	}
	if n := h.probeCount(); n != 1 {
		t.Errorf("issued %d probes, want exactly 1", n)
	}
}

func TestChannelDeletedActivePreservesPortal(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: activeProbe})
	h.client.handleChannelDeleted("chan-live")
	results := h.dequeue(t)

	if len(results) != 1 || results[0].Applied {
		t.Fatalf("results = %+v, want one unapplied verdict", results)
	}
	if results[0].Reason != "channel-active" {
		t.Errorf("reason = %q, want channel-active", results[0].Reason)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times for a live channel, want 0", n)
	}
	if h.client.isChannelDeleted("chan-live") {
		t.Error("live channel must not be marked as deleted")
	}
}

func TestChannelDeletedAuthoritative404DeletesPortal(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.handleChannelDeleted("chan-gone")
	results := h.dequeue(t)

	if len(results) != 1 {
		t.Fatalf("got %d verdicts, want 1", len(results))
	}
	if !results[0].Applied || results[0].Reason != "channel-permanently-deleted" {
		t.Fatalf("verdict = %+v, want applied channel-permanently-deleted", results[0])
	}
	if n := h.sink.count(); n != 1 {
		t.Fatalf("framework deletion path invoked %d times, want 1", n)
	}
	call := h.sink.last(t)
	wantKey := networkid.PortalKey{ID: "chan-gone", Receiver: "login-test"}
	if call.portalKey != wantKey {
		t.Errorf("deleted portal key = %+v, want %+v", call.portalKey, wantKey)
	}
	if call.source != h.client.UserLogin {
		t.Error("deletion must be attributed to the attached user login")
	}
	if call.onlyForMe {
		t.Error("Mattermost channel deletion is for everyone, not only this login")
	}
	if !h.client.isChannelDeleted("chan-gone") {
		t.Error("channel should be marked as deleted after an authoritative 404")
	}
	if n := h.probeCount(); n != 1 {
		t.Errorf("issued %d probes, want exactly %d", n, maxDeferredDeletionProbes)
	}
}

func TestChannelDeletedUnproven404PreservesPortal(t *testing.T) {
	tests := map[string]probeFunc{
		"no error id":             func(string) mattermost.ChannelProbe { return httpProbe(http.StatusNotFound) },
		"proxy 404 with wrong id": func(string) mattermost.ChannelProbe { return httpProbe(http.StatusNotFound) },
		"canonical id but wrong path": func(string) mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("chan-404")
			p.Err.(*mattermost.HTTPError).Path = "/api/v4/channels/chan-404/members"
			return p
		},
		"canonical fields but unauthenticated": func(string) mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("chan-404")
			p.Authenticated = false
			return p
		},
		"canonical id for another channel": func(string) mattermost.ChannelProbe {
			return authoritativeNotFoundProbe("chan-other")
		},
		"canonical id with inconsistent body status": func(string) mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("chan-404")
			p.Err.(*mattermost.HTTPError).BodyStatusCode = http.StatusOK
			return p
		},
		"canonical fields but missing status code": func(string) mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe("chan-404")
			p.StatusCode = 0
			return p
		},
		"missing state that carried a channel": func(channelID string) mattermost.ChannelProbe {
			p := authoritativeNotFoundProbe(channelID)
			p.Channel = &mattermost.Channel{ID: channelID}
			return p
		},
		"untyped 404 error": func(string) mattermost.ChannelProbe {
			return mattermost.ChannelProbe{
				State: mattermost.ChannelStateMissing, StatusCode: http.StatusNotFound, Authenticated: true,
				Err: errors.New("404 page not found"),
			}
		},
	}
	for name, probe := range tests {
		t.Run(name, func(t *testing.T) {
			h := newLifecycleHarness(t, lifecycleOptions{probe: probe})
			h.client.handleChannelDeleted("chan-404")
			results := h.dequeue(t)

			if len(results) != 1 || results[0].Applied {
				t.Fatalf("results = %+v, want one unapplied verdict", results)
			}
			if n := h.sink.count(); n != 0 {
				t.Fatalf("deletion path invoked %d times for an unproven 404, want 0", n)
			}
			if h.client.isChannelDeleted("chan-404") {
				t.Error("channel must not be marked deleted when the deletion is unproven")
			}
		})
	}
}

func TestChannelDeletedAmbiguousFailurePreservesPortal(t *testing.T) {
	tests := map[string]probeFunc{
		"403": func(string) mattermost.ChannelProbe { return httpProbe(http.StatusForbidden) },
		"401": func(string) mattermost.ChannelProbe { return httpProbe(http.StatusUnauthorized) },
		"500": func(string) mattermost.ChannelProbe { return httpProbe(http.StatusInternalServerError) },
		"502": func(string) mattermost.ChannelProbe { return httpProbe(http.StatusBadGateway) },
		"unexpected 418": func(string) mattermost.ChannelProbe {
			return mattermost.ChannelProbe{State: mattermost.ChannelStateUnknown, StatusCode: http.StatusTeapot, Authenticated: true}
		},
		"transport": func(string) mattermost.ChannelProbe { return transportErrProbe() },
		"unauthenticated": func(string) mattermost.ChannelProbe {
			return mattermost.ChannelProbe{State: mattermost.ChannelStateUnknown, Authenticated: false, Err: errors.New("channel probe: empty bearer token")}
		},
		"archived": func(string) mattermost.ChannelProbe { return archivedProbe("chan-ambig") },
		"malformed 200": func(string) mattermost.ChannelProbe {
			return mattermost.ChannelProbe{State: mattermost.ChannelStateUnknown, StatusCode: http.StatusOK, Authenticated: true, Err: errors.New("malformed 200")}
		},
		"zero probe": func(string) mattermost.ChannelProbe { return mattermost.ChannelProbe{} },
	}
	for name, probe := range tests {
		t.Run(name, func(t *testing.T) {
			h := newLifecycleHarness(t, lifecycleOptions{probe: probe})
			h.client.handleChannelDeleted("chan-ambig")
			results := h.dequeue(t)

			if len(results) != 1 || results[0].Applied {
				t.Fatalf("results = %+v, want one unapplied verdict", results)
			}
			if n := h.sink.count(); n != 0 {
				t.Errorf("deletion path invoked %d times, want 0", n)
			}
			if h.client.isChannelDeleted("chan-ambig") {
				t.Error("channel must not be marked deleted")
			}
		})
	}
}

func TestChannelDeletedEmptyIDIsIgnored(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: func(string) mattermost.ChannelProbe {
		t.Error("probe must not run without a channel ID")
		return archivedProbe("")
	}})
	h.client.handleChannelDeleted("")
	if n := h.events.len(); n != 0 {
		t.Fatalf("queued %d events for an empty channel id, want 0", n)
	}
	if n := h.probeCount(); n != 0 {
		t.Fatalf("issued %d probes, want 0", n)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times, want 0", n)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Duplicates and the per-channel check bound
// ──────────────────────────────────────────────────────────────────────────────

func TestDuplicateHintsQueueOneCheckAndDeleteOnce(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.handleChannelDeleted("chan-dup")
	h.client.handleChannelDeleted("chan-dup")
	h.client.handleChannelDeleted("chan-dup")

	if n := h.events.len(); n != 1 {
		t.Fatalf("a burst of 3 hints queued %d events, want 1", n)
	}
	h.dequeue(t)
	if n := h.sink.count(); n != 1 {
		t.Fatalf("burst deleted %d portals, want 1", n)
	}
	if n := h.probeCount(); n != 1 {
		t.Fatalf("burst issued %d probes, want 1", n)
	}

	// A later duplicate (after the deletion was applied) must not queue again.
	h.client.handleChannelDeleted("chan-dup")
	if n := h.events.len(); n != 0 {
		t.Fatalf("hint after an applied deletion queued %d events, want 0", n)
	}
	h.dequeue(t)
	if n := h.sink.count(); n != 1 {
		t.Fatalf("deletion path invoked %d times total, want 1", n)
	}
	if !h.client.isChannelDeleted("chan-dup") {
		t.Error("channel should stay marked as deleted")
	}
}

func TestPreserveVerdictReleasesTheCheckSlot(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-again")
	h.dequeue(t)

	if h.client.isCheckQueued("chan-again") {
		t.Fatal("the slot must be released once the check was evaluated")
	}
	h.client.handleChannelDeleted("chan-again")
	if n := h.events.len(); n != 1 {
		t.Fatalf("hint after a preserved verdict queued %d events, want 1", n)
	}
	h.dequeue(t)
	if n := h.probeCount(); n != 2 {
		t.Fatalf("issued %d probes over two hints, want 2", n)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times, want 0", n)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Ordering against restores
// ──────────────────────────────────────────────────────────────────────────────

// TestStaleCheckDroppedBeforeProbe covers a restore that landed after the hint
// was queued but before the check was dequeued: no probe, no deletion.
func TestStaleCheckDroppedBeforeProbe(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.handleChannelDeleted("chan-stale")
	h.client.noteChannelAlive("chan-stale") // restore bumps the generation

	results := h.dequeue(t)
	if len(results) != 1 || results[0].Applied {
		t.Fatalf("results = %+v, want one unapplied verdict", results)
	}
	if results[0].Reason != "superseded-before-probe" {
		t.Errorf("reason = %q, want superseded-before-probe", results[0].Reason)
	}
	if n := h.probeCount(); n != 0 {
		t.Errorf("stale check issued %d probes, want 0", n)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times, want 0", n)
	}
	if h.client.isChannelDeleted("chan-stale") {
		t.Error("stale verdict must not mark the channel deleted")
	}
}

// TestRestoreDuringProbeDropsVerdict models the race the design has to survive:
// the check is dequeued, its probe is in flight, and a restore lands before the
// probe answers. The verdict is about a state the channel has left, so it must
// be dropped even though the probe itself proved a 404.
func TestRestoreDuringProbeDropsVerdict(t *testing.T) {
	probeEntered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	h := newLifecycleHarness(t, lifecycleOptions{probe: func(channelID string) mattermost.ChannelProbe {
		once.Do(func() { close(probeEntered) })
		<-release // hold the probe open while the restore happens
		return authoritativeNotFoundProbe(channelID)
	}})
	h.client.handleChannelDeleted("chan-race")

	resultCh := make(chan deferredDeleteResult, 1)
	go func() {
		checks := h.events.drain()
		if len(checks) != 1 {
			resultCh <- deferredDeleteResult{Reason: "unexpected-queue"}
			return
		}
		check := checks[0].(*mmChannelDeleteCheckEvent)
		resultCh <- h.client.applyDeferredChannelDelete(context.Background(), h.sink, *check)
	}()

	select {
	case <-probeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("probe never started")
	}
	h.client.noteChannelAlive("chan-race") // restore arrives mid-probe
	close(release)

	var result deferredDeleteResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("dequeue never settled")
	}
	if result.Applied {
		t.Fatal("verdict was applied although a restore superseded the probe")
	}
	if result.Reason != "superseded-during-probe" {
		t.Errorf("reason = %q, want superseded-during-probe", result.Reason)
	}
	if n := h.sink.count(); n != 0 {
		t.Fatalf("deletion path invoked %d times after a racing restore, want 0", n)
	}
	if h.client.isChannelDeleted("chan-race") {
		t.Error("stale verdict must not mark the channel deleted")
	}
}

// TestRestoreAfterAppliedDeletionAllowsLaterDeletion covers the other ordering:
// the portal was deleted, the channel is restored, and a second deletion must
// still be able to delete the re-created portal.
func TestRestoreAfterAppliedDeletionAllowsLaterDeletion(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	h.client.handleChannelDeleted("chan-cycle")
	h.dequeue(t)
	if n := h.sink.count(); n != 1 {
		t.Fatalf("first deletion invoked the sink %d times, want 1", n)
	}

	h.client.noteChannelAlive("chan-cycle") // restore
	if h.client.isChannelDeleted("chan-cycle") {
		t.Fatal("restore must clear the deletion mark")
	}

	h.client.handleChannelDeleted("chan-cycle")
	h.dequeue(t)
	if n := h.sink.count(); n != 2 {
		t.Fatalf("deletion after restore invoked the sink %d times total, want 2", n)
	}
}

func TestChannelDeletedArchivedThenRestoreDoesNotDelete(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-restore")
	h.dequeue(t)
	h.client.noteChannelAlive("chan-restore")
	h.client.handleChannelDeleted("chan-restore")
	h.dequeue(t)

	if n := h.sink.count(); n != 0 {
		t.Fatalf("archived/restored channel invoked the deletion path %d times, want 0", n)
	}
	if h.client.isChannelDeleted("chan-restore") {
		t.Error("restored channel must not be marked deleted")
	}
}

func TestChannelDeletedProbesOnlyTheHintedChannel(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-target")
	h.dequeue(t)

	h.probeMu.Lock()
	defer h.probeMu.Unlock()
	if len(h.probes) != 1 || h.probes[0] != "chan-target" {
		t.Fatalf("probed %v, want [chan-target]", h.probes)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// applyDeferredChannelDelete guards
// ──────────────────────────────────────────────────────────────────────────────

func TestApplyDeferredChannelDeleteNilSink(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
	ev := mmChannelDeleteCheckEvent{
		client:     h.client,
		portalKey:  networkid.PortalKey{ID: "chan-nil", Receiver: "login-test"},
		channelID:  "chan-nil",
		generation: h.client.channelGeneration("chan-nil"),
	}
	result := h.client.applyDeferredChannelDelete(context.Background(), nil, ev)
	if result.Applied || result.Reason != "no-portal-sink" {
		t.Fatalf("result = %+v, want no-portal-sink", result)
	}
	if n := h.probeCount(); n != 0 {
		t.Errorf("issued %d probes without a sink, want 0", n)
	}
}

func TestApplyDeferredChannelDeleteRejectsKeyMismatch(t *testing.T) {
	tests := map[string]struct {
		channelID string
		key       networkid.PortalKey
	}{
		"empty channel id": {
			channelID: "",
			key:       networkid.PortalKey{ID: "", Receiver: "login-test"},
		},
		"channel id differs from portal key": {
			channelID: "chan-a",
			key:       networkid.PortalKey{ID: "chan-b", Receiver: "login-test"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h := newLifecycleHarness(t, lifecycleOptions{probe: authoritativeNotFoundProbe})
			ev := mmChannelDeleteCheckEvent{
				client:     h.client,
				portalKey:  tt.key,
				channelID:  tt.channelID,
				generation: h.client.channelGeneration(tt.channelID),
			}
			result := h.client.applyDeferredChannelDelete(context.Background(), h.sink, ev)
			if result.Applied || result.Reason != "channel-portal-mismatch" {
				t.Fatalf("result = %+v, want channel-portal-mismatch", result)
			}
			if n := h.probeCount(); n != 0 {
				t.Errorf("issued %d probes on a mismatched check, want 0", n)
			}
			if n := h.sink.count(); n != 0 {
				t.Errorf("deletion path invoked %d times, want 0", n)
			}
		})
	}
}

// TestUnsupportedBridgeConfigPreservesPortal pins the fail-closed configuration
// refusals: with AsyncEvents the portal lane cannot be ordered against restores,
// with PortalEventBuffer == 0 events are handled inline, and a client without a
// bridge/config cannot reason about ordering at all.
func TestUnsupportedBridgeConfigPreservesPortal(t *testing.T) {
	tests := map[string]struct {
		opts lifecycleOptions
		want string
	}{
		"async events":          {opts: lifecycleOptions{asyncEvents: true}, want: "async-events-unsupported"},
		"inline event handling": {opts: lifecycleOptions{inlineEvents: true}, want: "portal-event-buffer-inline"},
		"no bridge config":      {opts: lifecycleOptions{noBridgeConfig: true}, want: "bridge-config-unavailable"},
		"no login":              {opts: lifecycleOptions{noLogin: true}, want: "bridge-config-unavailable"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tt.opts.probe = authoritativeNotFoundProbe
			h := newLifecycleHarness(t, tt.opts)

			// The hint must be refused at queue time and must not leave the
			// channel marked as having a pending check.
			h.client.handleChannelDeleted("chan-refused")
			if n := h.events.len(); n != 0 {
				t.Fatalf("refused config queued %d events, want 0", n)
			}
			if h.client.isCheckQueued("chan-refused") {
				t.Error("refused config must not leave a pending check behind")
			}

			// And a check that reached the dequeue anyway (e.g. queued before a
			// config change) must be refused there as well.
			ev := mmChannelDeleteCheckEvent{
				client:     h.client,
				portalKey:  networkid.PortalKey{ID: "chan-refused", Receiver: "login-test"},
				channelID:  "chan-refused",
				generation: h.client.channelGeneration("chan-refused"),
			}
			result := h.client.applyDeferredChannelDelete(context.Background(), h.sink, ev)
			if result.Applied || result.Reason != tt.want {
				t.Fatalf("result = %+v, want reason %q", result, tt.want)
			}
			if n := h.probeCount(); n != 0 {
				t.Errorf("issued %d probes under a refused config, want 0", n)
			}
			if n := h.sink.count(); n != 0 {
				t.Errorf("deletion path invoked %d times under a refused config, want 0", n)
			}
			if h.client.isChannelDeleted("chan-refused") {
				t.Error("refused config must preserve the portal")
			}
		})
	}
}

// TestCommitChannelDeleteGuards covers the commit critical section directly:
// the generation must still match and a deletion is committed at most once.
func TestCommitChannelDeleteGuards(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{})

	gen := h.client.bumpChannelGeneration("chan-commit")
	if !h.client.commitChannelDelete("chan-commit", gen) {
		t.Fatal("commit with the current generation must succeed")
	}
	if h.client.commitChannelDelete("chan-commit", gen) {
		t.Fatal("a second commit for the same channel must fail")
	}

	restoredGen := h.client.noteChannelAlive("chan-commit")
	if restoredGen == gen {
		t.Fatal("restore must bump the generation")
	}
	if h.client.commitChannelDelete("chan-commit", gen) {
		t.Fatal("commit with a superseded generation must fail")
	}
	if h.client.isChannelDeleted("chan-commit") {
		t.Error("a failed commit must not leave the deletion mark")
	}
	if !h.client.commitChannelDelete("chan-commit", restoredGen) {
		t.Fatal("commit with the restored generation must succeed")
	}
}

// TestNoteChannelAliveClearsQueuedCheck covers the restore path releasing the
// per-channel slot so a later hint can be evaluated.
func TestNoteChannelAliveClearsQueuedCheck(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-slot")
	if !h.client.isCheckQueued("chan-slot") {
		t.Fatal("hint did not queue a check")
	}
	h.client.noteChannelAlive("chan-slot")
	if h.client.isCheckQueued("chan-slot") {
		t.Error("restore must clear the queued check")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// PreHandle plumbing (the framework's dequeue hook)
// ──────────────────────────────────────────────────────────────────────────────

func TestPreHandleIsNilSafe(t *testing.T) {
	var nilEvent *mmChannelDeleteCheckEvent
	nilEvent.PreHandle(context.Background(), &bridgev2.Portal{}) // must not panic

	ev := &mmChannelDeleteCheckEvent{}
	ev.PreHandle(context.Background(), &bridgev2.Portal{}) // nil client
	ev.PreHandle(context.Background(), nil)                // nil portal

	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-prehandle")
	if !h.client.isCheckQueued("chan-prehandle") {
		t.Fatal("hint did not queue a check")
	}
	h.client.applyDeferredChannelDelete(context.Background(), h.sink, mmChannelDeleteCheckEvent{
		client:     h.client,
		portalKey:  networkid.PortalKey{ID: "chan-nil-portal", Receiver: "login-test"},
		channelID:  "chan-nil-portal",
		generation: h.client.channelGeneration("chan-nil-portal"),
	})
}

// TestPreHandleRunsTheDequeueDecision pins that PreHandle (not the hint) is what
// takes the decision, by invoking it for an archived channel through a
// zero-value portal. The preserve verdict is safe without a real portal,
// because the framework's deletion path is only reached on proven deletion.
func TestPreHandleRunsTheDequeueDecision(t *testing.T) {
	h := newLifecycleHarness(t, lifecycleOptions{probe: archivedProbe})
	h.client.handleChannelDeleted("chan-hook")

	checks := h.events.checks()
	if len(checks) != 1 {
		t.Fatalf("queued %d checks, want 1", len(checks))
	}
	if n := h.probeCount(); n != 0 {
		t.Fatal("probe must not run before the check is dequeued")
	}

	checks[0].PreHandle(context.Background(), &bridgev2.Portal{})

	if h.client.isCheckQueued("chan-hook") {
		t.Error("dequeue must release the per-channel slot")
	}
	if n := h.probeCount(); n != 1 {
		t.Errorf("issued %d probes, want 1 (PreHandle performs the probe)", n)
	}
	if h.client.isChannelDeleted("chan-hook") {
		t.Error("archived channel must not be marked deleted")
	}
}
