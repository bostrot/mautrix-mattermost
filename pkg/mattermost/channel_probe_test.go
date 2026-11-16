package mattermost

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestProbeChannelDoesNotProveAuthenticationFromBearerPresence(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"id":"app.channel.get.existing.app_error","status_code":404}`))
	}))
	defer srv.Close()
	p := ProbeChannel(srv.URL, "invalid-token", "ch1")
	if p.Authenticated {
		t.Fatal("unverified bearer token was treated as authenticated")
	}
}

func TestProbeChannelConfirmsAuthenticatedIdentityOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v4/users/me" {
			w.Write([]byte(`{"id":"user123"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"id":"app.channel.get.existing.app_error","status_code":404}`))
	}))
	defer srv.Close()
	p := ProbeChannel(srv.URL, "token", "ch1")
	if !p.Authenticated || p.AuthenticatedUserID != "user123" {
		t.Fatalf("authentication was not confirmed: %+v", p)
	}
}

func TestProbeChannelRejectsMismatchedID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"id":"different","delete_at":0}`)) }))
	defer srv.Close()
	p := ProbeChannel(srv.URL, "tok", "requested")
	if p.State != ChannelStateUnknown {
		t.Fatalf("mismatched ID state = %v, want unknown", p.State)
	}
}

func TestProbeChannelActive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/channels/ch-active" {
			t.Errorf("path = %q, want /api/v4/channels/ch-active", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
		}
		json.NewEncoder(w).Encode(map[string]any{
			"id": "ch-active", "type": "O", "display_name": "General", "delete_at": 0,
		})
	}))
	defer srv.Close()

	p := ProbeChannel(srv.URL, "tok", "ch-active")
	if p.State != ChannelStateActive {
		t.Fatalf("State = %v, want active (err=%v)", p.State, p.Err)
	}
	if p.Err != nil {
		t.Errorf("Err = %v, want nil", p.Err)
	}
	if p.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", p.StatusCode)
	}
	if p.Channel == nil || p.Channel.DisplayName != "General" {
		t.Fatalf("Channel = %#v, want decoded General channel", p.Channel)
	}
}

func TestProbeChannelArchived(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"id": "ch-arch", "type": "O", "display_name": "Old", "delete_at": 1234567890123,
		})
	}))
	defer srv.Close()

	p := ProbeChannel(srv.URL, "tok", "ch-arch")
	if p.State != ChannelStateArchived {
		t.Fatalf("State = %v, want archived (err=%v)", p.State, p.Err)
	}
	if p.Channel == nil || p.Channel.DeleteAt != 1234567890123 {
		t.Errorf("Channel = %#v, want DeleteAt 1234567890123", p.Channel)
	}
	if p.Err != nil {
		t.Errorf("Err = %v, want nil for archived", p.Err)
	}
}

func TestProbeChannelStatusClassification(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantState   ChannelState
		wantCode    int
		wantHTTPErr bool
		wantUnauth  bool
	}{
		{"missing_404", http.StatusNotFound, `{"id":"api.context.404.app_error"}`, ChannelStateMissing, 404, true, false},
		{"forbidden_403", http.StatusForbidden, `{"id":"api.context.permissions.app_error"}`, ChannelStateUnknown, 403, true, false},
		{"unauthorized_401", http.StatusUnauthorized, `{"id":"api.context.session_expired.app_error"}`, ChannelStateUnknown, 401, true, true},
		{"server_error_500", http.StatusInternalServerError, "boom", ChannelStateUnknown, 500, true, false},
		{"unavailable_503", http.StatusServiceUnavailable, "down", ChannelStateUnknown, 503, true, false},
		{"bad_request_400", http.StatusBadRequest, "nope", ChannelStateUnknown, 400, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			p := ProbeChannel(srv.URL, "tok", "ch1")
			if p.State != tt.wantState {
				t.Errorf("State = %v, want %v", p.State, tt.wantState)
			}
			if p.StatusCode != tt.wantCode {
				t.Errorf("StatusCode = %d, want %d", p.StatusCode, tt.wantCode)
			}
			if p.Channel != nil {
				t.Errorf("Channel = %#v, want nil", p.Channel)
			}
			if (p.Err != nil) != tt.wantHTTPErr {
				t.Fatalf("Err = %v, want error present = %v", p.Err, tt.wantHTTPErr)
			}
			var httpErr *HTTPError
			if !errors.As(p.Err, &httpErr) {
				t.Fatalf("Err = %v, want *HTTPError", p.Err)
			}
			if httpErr.StatusCode != tt.wantCode {
				t.Errorf("HTTPError.StatusCode = %d, want %d", httpErr.StatusCode, tt.wantCode)
			}
			if httpErr.Path != "/api/v4/channels/ch1" {
				t.Errorf("HTTPError.Path = %q, want /api/v4/channels/ch1", httpErr.Path)
			}
			if httpErr.Body != tt.body {
				t.Errorf("HTTPError.Body = %q, want %q", httpErr.Body, tt.body)
			}
			if got := errors.Is(p.Err, ErrUnauthorized); got != tt.wantUnauth {
				t.Errorf("errors.Is(Err, ErrUnauthorized) = %v, want %v", got, tt.wantUnauth)
			}
			// 404 must not be reported as a token/permission failure.
			if tt.status == http.StatusNotFound && errors.Is(p.Err, ErrUnauthorized) {
				t.Error("404 must not unwrap to ErrUnauthorized")
			}
		})
	}
}

func TestProbeChannelTransportErrorIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // no listener now: connection refused

	p := ProbeChannel(url, "tok", "ch1")
	if p.State != ChannelStateUnknown {
		t.Fatalf("State = %v, want unknown", p.State)
	}
	if p.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want 0 for transport error", p.StatusCode)
	}
	if p.Err == nil {
		t.Fatal("Err = nil, want transport error preserved")
	}
	var httpErr *HTTPError
	if errors.As(p.Err, &httpErr) {
		t.Errorf("Err = %v, want non-HTTPError for transport failure", p.Err)
	}
}

func TestProbeChannelUndecodableBodyIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("<html>not json</html>"))
	}))
	defer srv.Close()

	p := ProbeChannel(srv.URL, "tok", "ch1")
	if p.State != ChannelStateUnknown {
		t.Fatalf("State = %v, want unknown", p.State)
	}
	if p.Err == nil {
		t.Fatal("Err = nil, want decode error")
	}
}

func TestProbeChannelMalformed200IsUnknown(t *testing.T) {
	for _, body := range []string{"null", `{}`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(body))
		}))
		p := ProbeChannel(srv.URL, "tok", "ch1")
		srv.Close()

		if p.State != ChannelStateUnknown {
			t.Errorf("body %q: State = %v, want unknown", body, p.State)
		}
		if p.Err == nil {
			t.Errorf("body %q: Err = nil, want malformed-response error", body)
		}
		if p.Channel != nil {
			t.Errorf("body %q: Channel = %#v, want nil", body, p.Channel)
		}
	}
}

func TestProbeChannelEmptyIDMakesNoRequest(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		json.NewEncoder(w).Encode(Channel{ID: "x"})
	}))
	defer srv.Close()

	for _, id := range []string{"", "   "} {
		p := ProbeChannel(srv.URL, "tok", id)
		if p.State != ChannelStateUnknown {
			t.Errorf("ProbeChannel(%q).State = %v, want unknown", id, p.State)
		}
		if p.Err == nil {
			t.Errorf("ProbeChannel(%q).Err = nil, want error", id)
		}
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Errorf("server received %d requests, want 0", got)
	}
}

func TestGetChannelPreservesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "channel not found", http.StatusNotFound)
	}))
	defer srv.Close()

	ch, err := GetChannel(srv.URL, "tok", "gone")
	if err == nil {
		t.Fatal("GetChannel error = nil, want 404 error")
	}
	if ch != nil {
		t.Errorf("channel = %#v, want nil on error", ch)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("err = %v, want *HTTPError", err)
	}
	if httpErr.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode = %d, want 404", httpErr.StatusCode)
	}
	if httpErr.Body != "channel not found\n" {
		t.Errorf("Body = %q, want preserved body", httpErr.Body)
	}
	if errors.Is(err, ErrUnauthorized) {
		t.Error("404 must not unwrap to ErrUnauthorized")
	}
}

func TestChannelStateString(t *testing.T) {
	want := map[ChannelState]string{
		ChannelStateUnknown:  "unknown",
		ChannelStateActive:   "active",
		ChannelStateArchived: "archived",
		ChannelStateMissing:  "missing",
	}
	for state, s := range want {
		if got := state.String(); got != s {
			t.Errorf("ChannelState(%d).String() = %q, want %q", int(state), got, s)
		}
	}
}
