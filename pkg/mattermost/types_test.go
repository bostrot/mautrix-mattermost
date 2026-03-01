package mattermost

import (
	"encoding/json"
	"testing"
	"time"
)

// TestPostTimestamp verifies CreateAt is converted to the correct time.
func TestPostTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	p := &Post{CreateAt: ts.UnixMilli()}
	got := p.Timestamp()
	if !got.Equal(ts) {
		t.Errorf("Timestamp() = %v, want %v", got, ts)
	}
}

// TestUserDisplayName checks the ordering: nickname > full name > username.
func TestUserDisplayName_Nickname(t *testing.T) {
	u := User{Nickname: "nick", FirstName: "First", LastName: "Last", Username: "user"}
	if u.DisplayName() != "nick" {
		t.Errorf("DisplayName() = %q, want %q", u.DisplayName(), "nick")
	}
}

func TestUserDisplayName_FullName(t *testing.T) {
	u := User{FirstName: "First", LastName: "Last", Username: "user"}
	if u.DisplayName() != "First Last" {
		t.Errorf("DisplayName() = %q, want %q", u.DisplayName(), "First Last")
	}
}

func TestUserDisplayName_UsernameFallback(t *testing.T) {
	u := User{Username: "user"}
	if u.DisplayName() != "user" {
		t.Errorf("DisplayName() = %q, want %q", u.DisplayName(), "user")
	}
}

// TestChannelIsDM verifies type classification helpers.
func TestChannelIsDM(t *testing.T) {
	if !(&Channel{Type: "D"}).IsDM() {
		t.Error("type D should be IsDM")
	}
	if (&Channel{Type: "O"}).IsDM() {
		t.Error("type O should not be IsDM")
	}
}

func TestChannelIsGroupDM(t *testing.T) {
	if !(&Channel{Type: "G"}).IsGroupDM() {
		t.Error("type G should be IsGroupDM")
	}
	if (&Channel{Type: "D"}).IsGroupDM() {
		t.Error("type D should not be IsGroupDM")
	}
}

// TestDecodePost_DoubleEncoded verifies the double-JSON decode for WS events.
func TestDecodePost_DoubleEncoded(t *testing.T) {
	inner := Post{ID: "abc", Message: "hello", ChannelID: "ch1", UserID: "u1"}
	innerJSON, _ := json.Marshal(inner)

	data := map[string]interface{}{
		"post": string(innerJSON),
	}

	got := decodePost(data, "post")
	if got == nil {
		t.Fatal("decodePost returned nil")
	}
	if got.ID != "abc" {
		t.Errorf("ID = %q, want %q", got.ID, "abc")
	}
	if got.Message != "hello" {
		t.Errorf("Message = %q, want %q", got.Message, "hello")
	}
}

// TestDecodePost_Missing returns nil for a missing key.
func TestDecodePost_Missing(t *testing.T) {
	got := decodePost(map[string]interface{}{}, "post")
	if got != nil {
		t.Error("expected nil for missing key")
	}
}

// TestDecodeReaction_DoubleEncoded verifies reaction decode.
func TestDecodeReaction_DoubleEncoded(t *testing.T) {
	inner := Reaction{UserID: "u1", PostID: "p1", EmojiName: "thumbsup"}
	innerJSON, _ := json.Marshal(inner)

	data := map[string]interface{}{
		"reaction": string(innerJSON),
	}

	got := decodeReaction(data, "reaction")
	if got == nil {
		t.Fatal("decodeReaction returned nil")
	}
	if got.EmojiName != "thumbsup" {
		t.Errorf("EmojiName = %q, want %q", got.EmojiName, "thumbsup")
	}
}

// TestHTTPToWS verifies scheme translation.
func TestHTTPToWS_HTTPS(t *testing.T) {
	if got := httpToWS("https://mm.example.com"); got != "wss://mm.example.com" {
		t.Errorf("httpToWS = %q", got)
	}
}

func TestHTTPToWS_HTTP(t *testing.T) {
	if got := httpToWS("http://mm.example.com"); got != "ws://mm.example.com" {
		t.Errorf("httpToWS = %q", got)
	}
}

func TestHTTPToWS_AlreadyWS(t *testing.T) {
	if got := httpToWS("ws://mm.example.com"); got != "ws://mm.example.com" {
		t.Errorf("httpToWS = %q", got)
	}
}
