package connector

import (
	"testing"

	"maunium.net/go/mautrix/event"
)

// TestGetCapabilities verifies the advertised room features are set.
func TestGetCapabilities_Delete(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps == nil {
		t.Fatal("GetCapabilities returned nil")
	}
	if caps.Delete != event.CapLevelFullySupported {
		t.Errorf("Delete capability = %v, want FullySupported", caps.Delete)
	}
}

func TestGetCapabilities_Edit(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps.Edit != event.CapLevelFullySupported {
		t.Errorf("Edit capability = %v, want FullySupported", caps.Edit)
	}
}

func TestGetCapabilities_Reply(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps.Reply != event.CapLevelFullySupported {
		t.Errorf("Reply capability = %v, want FullySupported", caps.Reply)
	}
}

func TestGetCapabilities_Reaction(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if caps.Reaction != event.CapLevelFullySupported {
		t.Errorf("Reaction capability = %v, want FullySupported", caps.Reaction)
	}
}

func TestGetCapabilities_Typing(t *testing.T) {
	c := &MattermostClient{}
	caps := c.GetCapabilities(nil, makePortal("ch1"))
	if !caps.TypingNotifications {
		t.Error("TypingNotifications should be true")
	}
}

// TestHandleMatrixMessage_NotConnected ensures a guard error is returned when
// the client has no active WS connection.
func TestHandleMatrixMessage_NotConnected(t *testing.T) {
	c := &MattermostClient{UserID: "u1", Token: "tok"}
	msg := makeMatrixMessage("ch1", event.MsgText, "hello")
	_, err := c.HandleMatrixMessage(nil, msg)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

// TestHandleMatrixEdit_NotConnected ensures a guard error is returned.
func TestHandleMatrixEdit_NotConnected(t *testing.T) {
	c := &MattermostClient{UserID: "u1"}
	err := c.HandleMatrixEdit(nil, nil)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

// TestHandleMatrixMessageRemove_NotConnected ensures a guard error is returned.
func TestHandleMatrixMessageRemove_NotConnected(t *testing.T) {
	c := &MattermostClient{UserID: "u1"}
	err := c.HandleMatrixMessageRemove(nil, nil)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

// TestHandleMatrixReaction_NotConnected ensures a guard error is returned.
func TestHandleMatrixReaction_NotConnected(t *testing.T) {
	c := &MattermostClient{UserID: "u1"}
	_, err := c.HandleMatrixReaction(nil, nil)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

// TestHandleMatrixReactionRemove_NotConnected ensures a guard error is returned.
func TestHandleMatrixReactionRemove_NotConnected(t *testing.T) {
	c := &MattermostClient{UserID: "u1"}
	err := c.HandleMatrixReactionRemove(nil, nil)
	if err == nil {
		t.Fatal("expected error when not connected")
	}
}

// TestWrapSendErr_Nil passes through nil unchanged.
func TestWrapSendErr_Nil(t *testing.T) {
	if wrapSendErr(nil) != nil {
		t.Error("wrapSendErr(nil) should return nil")
	}
}

// TestWrapSendErr_NonNil wraps a non-nil error.
func TestWrapSendErr_NonNil(t *testing.T) {
	err := wrapSendErr(errSendTest("boom"))
	if err == nil {
		t.Fatal("wrapSendErr should not return nil for non-nil input")
	}
}

type errSendTest string

func (e errSendTest) Error() string { return string(e) }
