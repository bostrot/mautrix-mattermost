package mattermost

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReconnectRequestedOnlyForLiveSocket(t *testing.T) {
	idle := NewClient("", "")
	idle.CloseForReconnect()
	if idle.ReconnectRequested() {
		t.Fatal("an absent socket was marked for quiet reconnect")
	}
	for _, deliberate := range []bool{false, true} {
		name := "transport failure"
		if deliberate {
			name = "deliberate reconnect"
		}
		t.Run(name, func(t *testing.T) {
			upgrader := websocket.Upgrader{}
			ready := make(chan struct{})
			stop := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				if _, _, err = conn.ReadMessage(); err != nil {
					t.Error(err)
					return
				}
				close(ready)
				<-stop
			}))
			defer srv.Close()
			if deliberate {
				defer close(stop)
			}
			client := NewClient(srv.URL, "fixture")
			if err := client.ConnectContext(t.Context()); err != nil {
				if !deliberate {
					close(stop)
				}
				t.Fatal(err)
			}
			defer client.Close()
			<-ready
			if deliberate {
				client.CloseForReconnect()
			} else {
				close(stop)
			}
			select {
			case <-client.Events:
			case <-time.After(3 * time.Second):
				t.Fatal("socket did not close")
			}
			// A poll racing a completed transport failure must leave it transient.
			client.CloseForReconnect()
			if client.ReconnectRequested() != deliberate {
				t.Fatalf("quiet reconnect = %v, want %v", client.ReconnectRequested(), deliberate)
			}
		})
	}
}
