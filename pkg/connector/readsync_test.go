package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

var readNow = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func readClient() (*MattermostClient, *[]string, *[]string) {
	var views, threads []string
	c := &MattermostClient{
		UserID:    "me",
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
		channels: map[string]*mattermost.Channel{
			"chan": {ID: "chan", TeamID: "teamB", Type: "O"},
			"dm":   {ID: "dm", Type: "D"},
			"x":    {ID: "x", TeamID: "teamA", Type: "O"},
		},
		readNowFn:     func() time.Time { return readNow },
		viewChannelFn: func(id string) error { views = append(views, id); return nil },
		markThreadReadFn: func(team, root string, ms int64) error {
			threads = append(threads, team+"/"+root+"/"+time.UnixMilli(ms).UTC().Format("15:04:05"))
			return nil
		},
		threadRootFn: func(mxid string) string {
			if mxid == "$root" {
				return "rootpost"
			}
			return ""
		},
	}
	return c, &views, &threads
}

func receipt(channel string, t time.Time, thread event.ThreadID, implicit bool) *bridgev2.MatrixReadReceipt {
	return &bridgev2.MatrixReadReceipt{
		Portal:   &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(channel)}}},
		ReadUpTo: t,
		Receipt:  event.ReadReceipt{ThreadID: thread},
		Implicit: implicit,
	}
}

func TestMatrixReadViewsChannel(t *testing.T) {
	c, views, _ := readClient()
	ctx := context.Background()
	t1 := readNow.Add(-time.Minute)
	for _, r := range []*bridgev2.MatrixReadReceipt{
		receipt("chan", t1, "", false),
		receipt("chan", t1, "main", false),                    // same point again: no request
		receipt("chan", t1.Add(-time.Second), "", false),      // older: no request
		receipt("chan", t1.Add(time.Second), "", true),        // implicit (own message): no request
		receipt("chan", t1.Add(2*time.Second), "main", false), // newer: request
		receipt("dm", t1, "", false),
	} {
		if err := c.HandleMatrixReadReceipt(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(*views); got != 3 || (*views)[0] != "chan" || (*views)[1] != "chan" || (*views)[2] != "dm" {
		t.Fatalf("views = %v, want [chan chan dm]", *views)
	}
}

func TestMatrixThreadReadMarksThread(t *testing.T) {
	c, views, threads := readClient()
	ctx := context.Background()
	at := time.Date(2026, 11, 17, 10, 0, 5, 0, time.UTC)
	for _, r := range []*bridgev2.MatrixReadReceipt{
		receipt("chan", at, "$root", false),
		receipt("dm", at, "$root", false),            // DM: any team, smallest ID
		receipt("chan", at, "$unknown", false),       // not a bridged thread: nothing
		receipt("chan", time.Time{}, "$root", false), // no time: now
	} {
		if err := c.HandleMatrixReadReceipt(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"teamB/rootpost/10:00:05", "teamA/rootpost/10:00:05", "teamB/rootpost/" + readNow.UTC().Format("15:04:05")}
	if len(*threads) != len(want) {
		t.Fatalf("threads = %v, want %v", *threads, want)
	}
	for i := range want {
		if (*threads)[i] != want[i] {
			t.Fatalf("threads = %v, want %v", *threads, want)
		}
	}
	if len(*views) != 0 {
		t.Fatalf("a thread receipt viewed the channel: %v", *views)
	}
}

func TestMattermostViewSendsMatrixReceipt(t *testing.T) {
	c, views, _ := readClient()
	var got []*simplevent.Receipt
	c.queueReceiptFn = func(ev bridgev2.RemoteEvent) {
		got = append(got, ev.(*simplevent.Receipt))
	}
	viewed := time.UnixMilli(1794950000000)
	c.handleChannelsViewed(&mattermost.ChannelsViewedEvent{Times: map[string]int64{"chan": viewed.UnixMilli(), "dm": 0}})
	if len(got) != 2 {
		t.Fatalf("receipts = %d, want 2", len(got))
	}
	for _, r := range got {
		if r.GetType() != bridgev2.RemoteEventReadReceipt || !r.Sender.IsFromMe || r.Sender.SenderLogin != "login" || r.PortalKey.Receiver != "login" {
			t.Fatalf("bad receipt %+v", r)
		}
		want := viewed
		if r.PortalKey.ID == "dm" {
			want = readNow
		}
		if !r.ReadUpTo.Equal(want) {
			t.Fatalf("%s read up to %v, want %v", r.PortalKey.ID, r.ReadUpTo, want)
		}
	}
	// The Matrix receipt that follows must not be sent back to Mattermost.
	if err := c.HandleMatrixReadReceipt(context.Background(), receipt("chan", viewed, "", false)); err != nil {
		t.Fatal(err)
	}
	if len(*views) != 0 {
		t.Fatalf("echoed a Mattermost view back: %v", *views)
	}
}

func TestViewChannelRequest(t *testing.T) {
	var path, auth string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.Method+" "+r.URL.Path, r.Header.Get("Authorization")
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		_, _ = w.Write([]byte(`{"status":"OK"}`))
	}))
	defer srv.Close()
	if err := mattermost.ViewChannel(srv.URL, "tok", "chan"); err != nil {
		t.Fatal(err)
	}
	if path != "POST /api/v4/channels/members/me/view" || auth != "Bearer tok" || body["channel_id"] != "chan" || body["collapsed_threads_supported"] != true {
		t.Fatalf("request = %s %s %v", path, auth, body)
	}
	if err := mattermost.MarkThreadRead(srv.URL, "tok", "me", "team", "root", 1234); err != nil {
		t.Fatal(err)
	}
	if path != "PUT /api/v4/users/me/teams/team/threads/root/read/1234" {
		t.Fatalf("thread request = %s", path)
	}
}

func TestProcessEventsChannelsViewedQueuesReceipt(t *testing.T) {
	c, _, _ := readClient()
	var got []bridgev2.RemoteEvent
	c.queueReceiptFn = func(ev bridgev2.RemoteEvent) { got = append(got, ev) }
	feed := make(chan any, 1)
	feed <- &mattermost.ChannelsViewedEvent{Times: map[string]int64{"chan-ws": 1794950000000}}
	close(feed)
	c.processEvents(t.Context(), feed, nil) // returns when the feed closes
	if len(got) != 1 || got[0].GetType() != bridgev2.RemoteEventReadReceipt {
		t.Fatalf("queued %v, want one read receipt", got)
	}
}

func TestMatrixReadSkipsTeamSpace(t *testing.T) {
	requests := 0
	transport := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = transport })
	http.DefaultTransport = readReceiptTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL)
	})

	for _, hooks := range []bool{true, false} {
		c, views, threads := readClient()
		c.ServerURL = "https://example.com"
		if !hooks {
			c.viewChannelFn = nil
			c.markThreadReadFn = nil
		}
		for _, thread := range []event.ThreadID{"", "$root"} {
			if err := c.HandleMatrixReadReceipt(context.Background(), receipt("team:t1", readNow, thread, false)); err != nil {
				t.Error(err)
			}
		}
		if len(*views) != 0 || len(*threads) != 0 {
			t.Errorf("team receipts reached provider hooks: views=%v threads=%v", *views, *threads)
		}
	}
	if requests != 0 {
		t.Errorf("team receipts made %d HTTP requests, want zero", requests)
	}
}

type readReceiptTransport func(*http.Request) (*http.Response, error)

func (transport readReceiptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return transport(req)
}
