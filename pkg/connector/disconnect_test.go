package connector

import (
	"context"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

func disconnectClient() (*MattermostClient, context.Context, chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	c := &MattermostClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
		cancel:    cancel,
		loopDone:  loopDone,
	}
	return c, ctx, loopDone
}

// Disconnect must not return while the old loop is still running, or a
// replacement client can overlap with it.
func TestDisconnectWaitsForConnectionLoop(t *testing.T) {
	c, ctx, loopDone := disconnectClient()
	exited := make(chan struct{})
	go func() {
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond) // e.g. a sync still winding down
		close(exited)
		close(loopDone)
	}()
	c.Disconnect()
	select {
	case <-exited:
	default:
		t.Fatal("Disconnect returned before the connection loop exited")
	}
}

func TestDisconnectGivesUpOnStuckLoop(t *testing.T) {
	old := disconnectWait
	disconnectWait = 50 * time.Millisecond
	defer func() { disconnectWait = old }()
	c, _, _ := disconnectClient() // loopDone is never closed
	done := make(chan struct{})
	go func() { c.Disconnect(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect hung on a loop that never stops")
	}
}

func TestDisconnectWithoutConnectionIsNoop(t *testing.T) {
	c := &MattermostClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}}
	c.Disconnect()
}

// An auth error is handled inside the loop, which cannot wait for itself.
func TestAuthErrorStopsConnectionWithoutWaiting(t *testing.T) {
	old := disconnectWait
	disconnectWait = time.Minute
	defer func() { disconnectWait = old }()
	c, ctx, _ := disconnectClient() // loopDone is never closed
	feed := make(chan any, 1)
	feed <- &mattermost.AuthErrorEvent{Message: "expired"}
	close(feed)
	done := make(chan struct{})
	go func() { c.processEvents(context.Background(), feed, nil); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processEvents blocked waiting for its own loop")
	}
	if ctx.Err() == nil {
		t.Fatal("auth error did not cancel the connection")
	}
}

// End to end through the real Connect/connectLoop: once Disconnect returns the
// loop and its workers are gone, not merely told to stop.
func TestDisconnectStopsRealConnectionLoop(t *testing.T) {
	c := &MattermostClient{
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login"}},
		Token:     "tok", UserID: "me", baseURL: "http://127.0.0.1:1", // refuses connections
	}
	c.Connect(context.Background())
	c.Disconnect()
	c.mu.RLock()
	running := c.cancel != nil
	c.mu.RUnlock()
	if running {
		t.Fatal("connection loop still registered after Disconnect returned")
	}
}
