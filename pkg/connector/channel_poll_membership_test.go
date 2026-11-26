package connector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestChannelPollStopsTrackingLeftOrRemovedChannels(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"left public channel", http.StatusOK, `{"id":"channel","type":"O","delete_at":0}`},
		{"left archived public channel", http.StatusOK, `{"id":"channel","type":"O","delete_at":10}`},
		{"removed from private channel", http.StatusForbidden, `{"id":"api.context.permissions.app_error","status_code":403}`},
	} {
		for _, wasArchived := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/previously_archived=%t", tc.name, wasArchived), func(t *testing.T) {
				var present atomic.Bool
				present.Store(true)
				var selfRequests, channelRequests atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/api/v4/users/user/channels":
						if present.Load() {
							deleteAt := 0
							if wasArchived {
								deleteAt = 10
							}
							fmt.Fprintf(w, `[{"id":"channel","delete_at":%d}]`, deleteAt)
						} else {
							fmt.Fprint(w, `[]`)
						}
					case "/api/v4/users/me":
						selfRequests.Add(1)
						fmt.Fprint(w, `{"id":"user"}`)
					case "/api/v4/channels/channel":
						channelRequests.Add(1)
						w.WriteHeader(tc.status)
						fmt.Fprint(w, tc.body)
					default:
						t.Errorf("unexpected request: %s", r.URL)
						http.NotFound(w, r)
					}
				}))
				defer srv.Close()
				c := &MattermostClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}, UserID: "user", Token: "fixture", baseURL: srv.URL}
				var queued []bridgev2.RemoteEvent
				queue := func(ev bridgev2.RemoteEvent) { queued = append(queued, ev) }
				baseline, _ := c.pollChannelLifecycle(t.Context(), nil, queue)
				if len(baseline) != 1 {
					t.Fatalf("missing initial baseline: %v", baseline)
				}
				present.Store(false)
				for poll := 0; poll < 3; poll++ {
					var reconnect bool
					baseline, reconnect = c.pollChannelLifecycle(t.Context(), baseline, queue)
					if len(baseline) != 0 || len(queued) != 0 || reconnect {
						t.Fatalf("poll %d: removed channel must not resync, delete, or reconnect: baseline=%v events=%d reconnect=%t", poll, baseline, len(queued), reconnect)
					}
				}
				if selfRequests.Load() != 1 || channelRequests.Load() != 1 {
					t.Fatalf("left/removed channel probed repeatedly: self=%d channel=%d", selfRequests.Load(), channelRequests.Load())
				}
				// Rejoining reintroduces the channel into the ordinary baseline.
				present.Store(true)
				baseline, _ = c.pollChannelLifecycle(t.Context(), baseline, queue)
				if len(baseline) != 1 || len(queued) != 0 {
					t.Fatalf("rejoin baseline=%v events=%d", baseline, len(queued))
				}
			})
		}
	}
}
