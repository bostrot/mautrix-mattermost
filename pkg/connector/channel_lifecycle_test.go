package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestChannelLifecycleProbe(t *testing.T) {
	const missing = `{"id":"app.channel.get.existing.app_error","status_code":404}`
	for _, tc := range []struct {
		name, self, body   string
		selfStatus, status int
		restore            bool
		want               bridgev2.RemoteEventType
	}{
		{"canonical missing", "user", missing, 200, 404, false, bridgev2.RemoteEventChatDelete},
		{"archived", "user", `{"id":"channel","delete_at":123}`, 200, 200, false, bridgev2.RemoteEventChatResync},
		{"active", "user", `{"id":"channel","delete_at":0}`, 200, 200, false, bridgev2.RemoteEventChatResync},
		{"restored", "user", `{"id":"channel","delete_at":0}`, 200, 200, true, bridgev2.RemoteEventChatResync},
		{"restore missing", "user", missing, 200, 404, true, 0},
		{"unauthorized", "user", missing, 200, 401, false, 0},
		{"forbidden", "user", missing, 200, 403, false, 0},
		{"server failure", "user", missing, 200, 503, false, 0},
		{"unknown 404", "user", `{"id":"other"}`, 200, 404, false, 0},
		{"inconsistent 404", "user", `{"id":"app.channel.get.existing.app_error","status_code":403}`, 200, 404, false, 0},
		{"malformed success", "user", `{"id":`, 200, 200, false, 0},
		{"wrong channel", "user", `{"id":"other"}`, 200, 200, false, 0},
		{"wrong identity", "other", missing, 200, 404, false, 0},
		{"anonymous", "", missing, 401, 404, false, 0},
		{"network failure", "user", "", 200, 0, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("expected the login token on both requests")
				}
				switch r.URL.Path {
				case "/api/v4/users/me":
					w.WriteHeader(tc.selfStatus)
					fmt.Fprintf(w, `{"id":%q}`, tc.self)
				case "/api/v4/channels/channel":
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			if tc.status == 0 {
				srv.Close()
			}
			login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}
			c := &MattermostClient{UserLogin: login, baseURL: srv.URL, Token: "fixture", UserID: "user", channels: map[string]*mattermost.Channel{"channel": {ID: "channel"}}}
			var got bridgev2.RemoteEventType
			c.handleChannelLifecycle(context.Background(), "channel", !tc.restore, func(evt bridgev2.RemoteEvent) {
				got = evt.GetType()
				if evt.GetPortalKey().ID != "channel" {
					t.Errorf("wrong portal: %v", evt.GetPortalKey())
				}
			})
			if got != tc.want || (c.channels["channel"] == nil) != (tc.want == bridgev2.RemoteEventChatDelete) {
				t.Fatalf("event=%v want=%v cache=%+v", got, tc.want, c.channels["channel"])
			}
			if tc.name == "archived" && c.channels["channel"].DeleteAt != 123 {
				t.Fatal("archived state was lost")
			}
		})
	}
}

// A channel_restored websocket event must reconnect like a poll-found restore;
// other hints and inconclusive probes must not.
func TestChannelLifecycleWorkerReconnectsOnRestore(t *testing.T) {
	const missing = `{"id":"app.channel.get.existing.app_error","status_code":404}`
	for _, tc := range []struct {
		name          string
		allowDelete   bool
		status        int
		body          string
		wantReconnect bool
	}{
		{"restored active channel", false, 200, `{"id":"channel","delete_at":0}`, true},
		{"restore hint but still archived", false, 200, `{"id":"channel","delete_at":123}`, false},
		{"restore hint, probe inconclusive", false, 503, "", false},
		{"restore hint, channel gone", false, 404, missing, false},
		{"deleted hint on active channel", true, 200, `{"id":"channel","delete_at":0}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v4/users/me":
					fmt.Fprint(w, `{"id":"user"}`)
				case "/api/v4/channels/channel":
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			reconnects := make(chan struct{}, 4)
			login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}
			c := &MattermostClient{
				UserLogin: login, baseURL: srv.URL, Token: "fixture", UserID: "user",
				channels:    map[string]*mattermost.Channel{},
				reconnectFn: func() { reconnects <- struct{}{} },
			}
			c.processLifecycleHint(context.Background(), channelLifecycleHint{channelID: "channel", allowDelete: tc.allowDelete}, func(bridgev2.RemoteEvent) {})
			if got := len(reconnects) > 0; got != tc.wantReconnect {
				t.Fatalf("reconnect=%v want %v", got, tc.wantReconnect)
			}
		})
	}
}
