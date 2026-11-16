package mattermost

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ChannelState is the lifecycle classification of a channel as observed by a probe.
type ChannelState int

const (
	// ChannelStateUnknown means the probe could not determine the channel lifecycle:
	// transport failure, 401/403 permission or token rejection, 5xx, or an
	// unrecognized status/response body. See ChannelProbe.Err for the cause.
	ChannelStateUnknown ChannelState = iota
	// ChannelStateActive means the channel exists and is not archived (HTTP 200, delete_at == 0).
	ChannelStateActive
	// ChannelStateArchived means the channel exists but was deleted/archived
	// (HTTP 200 with delete_at != 0).
	ChannelStateArchived
	// ChannelStateMissing means the server answered HTTP 404 for the channel ID.
	ChannelStateMissing
)

func (s ChannelState) String() string {
	switch s {
	case ChannelStateActive:
		return "active"
	case ChannelStateArchived:
		return "archived"
	case ChannelStateMissing:
		return "missing"
	default:
		return "unknown"
	}
}

// ChannelProbe is the typed outcome of ProbeChannel.
type ChannelProbe struct {
	State ChannelState
	// StatusCode is the HTTP status code, or 0 when no response was received.
	StatusCode int
	// Channel is the decoded channel; set only for Active and Archived states.
	Channel *Channel
	// Err preserves the underlying failure for Missing and Unknown states,
	// nil for Active and Archived. For HTTP failures it is (or wraps) *HTTPError.
	Err error
	// Authenticated indicates a successful /users/me probe using the same token.
	Authenticated       bool
	AuthenticatedUserID string
	// ErrorID is the Mattermost error id from a failed response body, "" when
	// the response carried none.
	ErrorID string
}

// ProbeChannel performs a single GET /api/v4/channels/{id} and classifies the result:
//
//   - HTTP 200, delete_at == 0  -> ChannelStateActive
//   - HTTP 200, delete_at != 0  -> ChannelStateArchived
//   - HTTP 404                  -> ChannelStateMissing
//   - HTTP 401/403/5xx, transport error, undecodable body -> ChannelStateUnknown
//
// A 404 is reported as Missing but is not by itself proof of anything beyond the
// server's answer for this token: depending on server configuration, a caller may
// also see 403/404 for channels it is not allowed to read. This function never
// turns a 404 into a permission verdict — callers decide what Missing means in
// their context. Missing carries a non-nil Err for diagnostics.
//
// ProbeChannel does not log and performs no retries.
func ProbeChannel(serverURL, token, channelID string) ChannelProbe {
	if strings.TrimSpace(channelID) == "" {
		return ChannelProbe{State: ChannelStateUnknown, Err: errors.New("channel probe: empty channel id")}
	}
	// An unauthenticated probe cannot be evidence about a private Mattermost
	// instance; refuse to issue it rather than let a 404 stand in for a verdict.
	if strings.TrimSpace(token) == "" {
		return ChannelProbe{State: ChannelStateUnknown, Err: errors.New("channel probe: empty bearer token")}
	}

	ch, err := GetChannel(serverURL, token, channelID)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			p := ChannelProbe{
				StatusCode: httpErr.StatusCode,
				Err:        err,
				State:      ChannelStateUnknown,
				ErrorID:    httpErr.ErrorID,
			}
			if httpErr.StatusCode == http.StatusNotFound {
				p.State = ChannelStateMissing
				userID, _, authErr := GetSelf(serverURL, token)
				if authErr == nil && userID != "" {
					p.Authenticated = true
					p.AuthenticatedUserID = userID
				}
			}
			return p
		}
		return ChannelProbe{Err: err, State: ChannelStateUnknown}
	}

	if ch == nil || ch.ID != channelID {
		return ChannelProbe{StatusCode: http.StatusOK, State: ChannelStateUnknown, Authenticated: true,
			Err: fmt.Errorf("channel probe %q: malformed 200 response (channel id missing or mismatched)", channelID)}
	}

	state := ChannelStateActive
	if ch.DeleteAt != 0 {
		state = ChannelStateArchived
	}
	return ChannelProbe{State: state, StatusCode: http.StatusOK, Channel: ch, Authenticated: true}
}
