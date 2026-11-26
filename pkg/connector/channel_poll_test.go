package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestChannelPollConfig(t *testing.T) {
	for name, data := range map[string]string{
		"embedded": ExampleConfig,
		"file":     strings.TrimPrefix(mustReadExampleConfig(t), "network:\n"),
	} {
		var raw map[string]any
		if err := yaml.Unmarshal([]byte(data), &raw); err != nil {
			t.Fatal(err)
		}
		if val, ok := raw["channel_poll_interval"]; !ok || val != "" {
			t.Errorf("%s example has no explicit disabled default: %v", name, val)
		}
	}
	for value, valid := range map[string]bool{"": true, "45s": true, "0s": false, "-1s": false, "45": false} {
		conn := &MattermostConnector{Config: &Config{ChannelPollInterval: value}}
		err := conn.Start(t.Context())
		if valid != (err == nil) || err != nil && !strings.Contains(err.Error(), "network.channel_poll_interval") {
			t.Errorf("config value %q: valid=%v, err=%v", value, valid, err)
		}
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("channel_poll_interval: 45s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	example, _, upgrade := (&MattermostConnector{}).GetConfig()
	data, _, err := configupgrade.Do(path, false, configupgrade.MergeUpgraders(example, upgrade))
	if err != nil {
		t.Fatal(err)
	}
	var opts map[string]any
	if err = yaml.Unmarshal(data, &opts); err != nil {
		t.Fatal(err)
	}
	if opts["channel_poll_interval"] != "45s" {
		t.Fatalf("poll interval lost in upgrade: %s", data)
	}
}

func TestChannelPollIntervalAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var calls atomic.Int32
		done := make(chan struct{})
		go func() {
			defer close(done)
			runChannelPoll(ctx, 0, func() { calls.Add(1) })
		}()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Error("disabled poll did not exit")
		}
		if calls.Load() != 0 {
			t.Errorf("disabled poll ran %d times", calls.Load())
		}

		done = make(chan struct{})
		go func() {
			defer close(done)
			runChannelPoll(ctx, 10*time.Second, func() { calls.Add(1) })
		}()
		synctest.Wait()
		start := calls.Load()
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if start != 1 || calls.Load() != 1 {
			t.Errorf("poll should take one initial baseline then wait, start=%d calls=%d", start, calls.Load())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Errorf("configured interval did not trigger another poll, calls=%d", calls.Load())
		}
		cancel()
		<-done
		time.Sleep(20 * time.Second)
		if calls.Load() != 2 {
			t.Errorf("poll continued after cancellation, calls=%d", calls.Load())
		}
	})
}

func TestChannelPollLifecycle(t *testing.T) {
	type queuedEvent struct {
		id  networkid.PortalID
		typ bridgev2.RemoteEventType
	}
	var step atomic.Int32
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/users/user/channels":
			if r.URL.RawQuery != "include_deleted=true" {
				t.Errorf("unexpected list query: %s", r.URL.RawQuery)
			}
			switch step.Load() {
			case 0:
				fmt.Fprint(w, `[{"id":"archive","delete_at":0},{"id":"restore","delete_at":20},{"id":"missing","delete_at":0},{"id":"denied","delete_at":0},{"id":"unknown","delete_at":0}]`)
			case 1:
				fmt.Fprint(w, `[{"id":"archive","delete_at":10}`) // stream cut off before its closing bracket
			default:
				fmt.Fprint(w, `[{"id":"archive","delete_at":10},{"id":"restore","delete_at":0},{"id":"new","delete_at":0}]`)
			}
		case "/api/v4/users/me":
			requests.Add(1)
			fmt.Fprint(w, `{"id":"user"}`)
		case "/api/v4/channels/archive":
			fmt.Fprint(w, `{"id":"archive","delete_at":10}`)
		case "/api/v4/channels/restore":
			fmt.Fprint(w, `{"id":"restore","delete_at":0}`)
		case "/api/v4/channels/missing":
			w.WriteHeader(404)
			fmt.Fprint(w, `{"id":"app.channel.get.existing.app_error","status_code":404}`)
		case "/api/v4/channels/denied":
			w.WriteHeader(403)
			fmt.Fprint(w, `{"id":"app.channel.get.existing.app_error"}`)
		case "/api/v4/channels/unknown":
			w.WriteHeader(404)
			fmt.Fprint(w, `{"id":"other"}`)
		default:
			t.Errorf("unexpected request: %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := &MattermostClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}},
		UserID:    "user", Token: "fixture", baseURL: srv.URL,
	}
	var queued []queuedEvent
	queue := func(evt bridgev2.RemoteEvent) {
		queued = append(queued, queuedEvent{evt.GetPortalKey().ID, evt.GetType()})
	}
	baseline, reconnect := c.pollChannelLifecycle(t.Context(), nil, queue)
	if len(baseline) != 5 || baseline["archive"] || !baseline["restore"] || len(queued) != 0 || reconnect {
		t.Fatalf("first listing was not baseline-only: %v events=%v reconnect=%v", baseline, queued, reconnect)
	}
	step.Store(1)
	incomplete, reconnect := c.pollChannelLifecycle(t.Context(), baseline, queue)
	if !reflect.DeepEqual(baseline, incomplete) || len(queued) != 0 || reconnect {
		t.Fatalf("incomplete listing disturbed baseline: %v events=%v reconnect=%v", incomplete, queued, reconnect)
	}
	step.Store(2)
	next, reconnect := c.pollChannelLifecycle(t.Context(), incomplete, queue)
	want := []queuedEvent{
		{"archive", bridgev2.RemoteEventChatResync},
		{"missing", bridgev2.RemoteEventChatDelete},
		{"restore", bridgev2.RemoteEventChatResync},
	}
	wantBaseline := map[string]bool{"archive": true, "restore": false, "new": false, "unknown": false}
	if !reflect.DeepEqual(queued, want) || !reconnect || !reflect.DeepEqual(next, wantBaseline) || requests.Load() != 3 {
		t.Fatalf("next=%v events=%v reconnect=%v deletion auth checks=%d", next, queued, reconnect, requests.Load())
	}
	queued = nil
	next, reconnect = c.pollChannelLifecycle(t.Context(), next, queue)
	if len(queued) != 0 || reconnect || !reflect.DeepEqual(next, wantBaseline) || requests.Load() != 4 {
		t.Fatalf("expected only the unknown-404 retry: next=%v events=%v reconnect=%v checks=%d", next, queued, reconnect, requests.Load())
	}
}

func TestChannelPollCancelsInFlightListing(t *testing.T) {
	started, aborted := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(aborted)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := &MattermostClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}},
		UserID:    "user", Token: "fixture", baseURL: srv.URL,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runLifecyclePoll(ctx, time.Hour)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not start")
	}
	cancel()
	for _, ch := range []<-chan struct{}{done, aborted} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("poll or HTTP request survived cancellation")
		}
	}
}

func TestChannelPollRetriesAbsentChannel(t *testing.T) {
	for _, reason := range []string{"transient", "unknown 404", "wrong login", "wrong channel", "cancelled", "redirected identity", "redirected channel", "conflicting 403"} {
		t.Run(reason, func(t *testing.T) {
			var step atomic.Int32
			var probes atomic.Int32
			started := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v4/users/user/channels":
					if step.Load() == 0 {
						fmt.Fprint(w, `[{"id":"channel","delete_at":0}]`)
					} else {
						fmt.Fprint(w, `[]`)
					}
				case "/api/v4/users/me":
					if step.Load() == 1 && reason == "redirected identity" {
						http.Redirect(w, r, "/redirect-target", http.StatusFound)
						return
					}
					if step.Load() == 1 && reason == "wrong login" {
						fmt.Fprint(w, `{"id":"other"}`)
					} else {
						fmt.Fprint(w, `{"id":"user"}`)
					}
				case "/api/v4/channels/channel":
					probes.Add(1)
					if step.Load() != 1 {
						w.WriteHeader(http.StatusNotFound)
						fmt.Fprint(w, `{"id":"app.channel.get.existing.app_error","status_code":404}`)
						return
					}
					switch reason {
					case "transient":
						w.WriteHeader(http.StatusServiceUnavailable)
						fmt.Fprint(w, `{"id":"temporary"}`)
					case "unknown 404":
						w.WriteHeader(http.StatusNotFound)
						fmt.Fprint(w, `{"id":"other"}`)
					case "wrong channel":
						fmt.Fprint(w, `{"id":"other","delete_at":10}`)
					case "cancelled":
						close(started)
						<-r.Context().Done()
					case "redirected channel":
						http.Redirect(w, r, "/redirect-target", http.StatusFound)
					case "conflicting 403":
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `{"id":"api.context.permissions.app_error","status_code":500}`)
					default:
						t.Errorf("unexpected channel probe for %s", reason)
						http.NotFound(w, r)
					}
				default:
					t.Errorf("unexpected request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			c := &MattermostClient{
				UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}},
				UserID:    "user", Token: "fixture", baseURL: srv.URL,
			}
			var queued []bridgev2.RemoteEventType
			queue := func(evt bridgev2.RemoteEvent) {
				if evt.GetPortalKey().ID != "channel" {
					t.Errorf("unexpected event portal: %v", evt.GetPortalKey())
				}
				queued = append(queued, evt.GetType())
			}
			baseline, _ := c.pollChannelLifecycle(t.Context(), nil, queue)
			if len(baseline) != 1 || baseline["channel"] || len(queued) != 0 {
				t.Fatalf("initial baseline=%v, events=%v", baseline, queued)
			}
			step.Store(1)
			if reason == "cancelled" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan map[string]bool, 1)
				go func() {
					next, _ := c.pollChannelLifecycle(ctx, baseline, queue)
					done <- next
				}()
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("absent-channel probe did not start")
				}
				cancel()
				select {
				case baseline = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("absent-channel probe survived cancellation")
				}
			} else {
				baseline, _ = c.pollChannelLifecycle(t.Context(), baseline, queue)
			}
			var wantEvents []bridgev2.RemoteEventType
			if len(baseline) != 1 || baseline["channel"] || !reflect.DeepEqual(queued, wantEvents) {
				t.Errorf("after %s baseline=%v, events=%v", reason, baseline, queued)
			}
			step.Store(2)
			baseline, _ = c.pollChannelLifecycle(t.Context(), baseline, queue)
			afterDelete := probes.Load()
			baseline, _ = c.pollChannelLifecycle(t.Context(), baseline, queue)
			wantEvents = append(wantEvents, bridgev2.RemoteEventChatDelete)
			if len(baseline) != 0 || !reflect.DeepEqual(queued, wantEvents) || probes.Load() != afterDelete {
				t.Errorf("canonical 404 should queue deletion exactly once: baseline=%v, events=%v, probes=%d", baseline, queued, probes.Load())
			}
		})
	}
}
