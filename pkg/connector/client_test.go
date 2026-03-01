package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// makePortal creates a minimal *bridgev2.Portal for unit tests.
func makePortal(id string) *bridgev2.Portal {
	return &bridgev2.Portal{
		Portal: &database.Portal{
			PortalKey: networkid.PortalKey{ID: networkid.PortalID(id)},
		},
	}
}

// makeMatrixMessage builds a minimal *bridgev2.MatrixMessage for guard-logic tests.
func makeMatrixMessage(portalID string, msgType event.MessageType, body string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Content: &event.MessageEventContent{
				MsgType: msgType,
				Body:    body,
			},
			Portal: makePortal(portalID),
		},
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// IsThisUser
// ──────────────────────────────────────────────────────────────────────────────

func TestIsThisUser_MatchByUserID(t *testing.T) {
	c := &MattermostClient{UserID: "user123"}
	if !c.IsThisUser(context.Background(), "user123") {
		t.Fatal("should return true when userID matches UserID")
	}
}

func TestIsThisUser_MismatchByUserID(t *testing.T) {
	c := &MattermostClient{UserID: "user123"}
	if c.IsThisUser(context.Background(), "other456") {
		t.Fatal("should return false for a different userID")
	}
}

func TestIsThisUser_FallbackToUsername_Match(t *testing.T) {
	c := &MattermostClient{Username: "alice", UserID: ""}
	if !c.IsThisUser(context.Background(), "alice") {
		t.Fatal("should fall back to Username when UserID is empty")
	}
}

func TestIsThisUser_FallbackToUsername_NoMatch(t *testing.T) {
	c := &MattermostClient{Username: "alice", UserID: ""}
	if c.IsThisUser(context.Background(), "bob") {
		t.Fatal("should return false when neither ID matches")
	}
}

func TestIsThisUser_UserIDSupersedesUsername(t *testing.T) {
	c := &MattermostClient{Username: "alice", UserID: "rawid"}
	if c.IsThisUser(context.Background(), "alice") {
		t.Fatal("UserID should take precedence over Username")
	}
	if !c.IsThisUser(context.Background(), "rawid") {
		t.Fatal("should match UserID when it is set")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// IsLoggedIn
// ──────────────────────────────────────────────────────────────────────────────

func TestIsLoggedIn_WithToken(t *testing.T) {
	c := &MattermostClient{Token: "tok_abc"}
	if !c.IsLoggedIn() {
		t.Fatal("IsLoggedIn should return true when Token is set")
	}
}

func TestIsLoggedIn_EmptyToken(t *testing.T) {
	c := &MattermostClient{Token: ""}
	if c.IsLoggedIn() {
		t.Fatal("IsLoggedIn should return false when Token is empty")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// mmTypingEvent interface methods
// ──────────────────────────────────────────────────────────────────────────────

func TestTypingEvent_GetPortalKey(t *testing.T) {
	e := &mmTypingEvent{
		channelID: "chan1",
		receiver:  "login1",
		senderID:  "user1",
	}
	pk := e.GetPortalKey()
	if pk.ID != "chan1" {
		t.Errorf("PortalKey.ID = %q, want %q", pk.ID, "chan1")
	}
	if pk.Receiver != "login1" {
		t.Errorf("PortalKey.Receiver = %q, want %q", pk.Receiver, "login1")
	}
}

func TestTypingEvent_GetType(t *testing.T) {
	e := &mmTypingEvent{}
	if e.GetType() != bridgev2.RemoteEventTyping {
		t.Errorf("GetType = %v, want RemoteEventTyping", e.GetType())
	}
}

func TestTypingEvent_ShouldCreatePortal(t *testing.T) {
	e := &mmTypingEvent{}
	if e.ShouldCreatePortal() {
		t.Error("ShouldCreatePortal should be false for typing events")
	}
}

func TestTypingEvent_Timeout(t *testing.T) {
	e := &mmTypingEvent{}
	if e.GetTimeout() == 0 {
		t.Error("typing timeout should be non-zero")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// mmChatDeleteEvent interface methods
// ──────────────────────────────────────────────────────────────────────────────

func TestChatDeleteEvent_GetType(t *testing.T) {
	e := &mmChatDeleteEvent{}
	if e.GetType() != bridgev2.RemoteEventChatDelete {
		t.Errorf("unexpected event type: %v", e.GetType())
	}
}

func TestChatDeleteEvent_DeleteOnlyForMe(t *testing.T) {
	e := &mmChatDeleteEvent{}
	if e.DeleteOnlyForMe() {
		t.Error("DeleteOnlyForMe should be false (delete for everyone)")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// dmOtherUserID
// ──────────────────────────────────────────────────────────────────────────────

func TestDMOtherUserID_FirstPart(t *testing.T) {
	// Mattermost DM channel name: "<id1>__<id2>"
	id1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 28 chars (fake MM user ID)
	id2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	name := id1 + "__" + id2
	got := dmOtherUserID(name, id2) // own ID is id2 → other is id1
	if got != id1 {
		t.Errorf("dmOtherUserID = %q, want %q", got, id1)
	}
}

func TestDMOtherUserID_SecondPart(t *testing.T) {
	id1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	id2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	name := id1 + "__" + id2
	got := dmOtherUserID(name, id1) // own ID is id1 → other is id2
	if got != id2 {
		t.Errorf("dmOtherUserID = %q, want %q", got, id2)
	}
}

func TestDMOtherUserID_TooShort(t *testing.T) {
	got := dmOtherUserID("short", "me")
	if got != "" {
		t.Errorf("dmOtherUserID for short name should be empty, got %q", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// makeMatrixMessage helper
// ──────────────────────────────────────────────────────────────────────────────

func TestMakePortal(t *testing.T) {
	p := makePortal("ch1")
	if p == nil {
		t.Fatal("makePortal returned nil")
	}
	if p.ID != "ch1" {
		t.Errorf("portal ID = %q, want %q", p.ID, "ch1")
	}
}

func TestMakeMatrixMessage(t *testing.T) {
	msg := makeMatrixMessage("ch1", event.MsgText, "hello")
	if msg == nil {
		t.Fatal("makeMatrixMessage returned nil")
	}
	if msg.Content.Body != "hello" {
		t.Errorf("body = %q, want %q", msg.Content.Body, "hello")
	}
}
