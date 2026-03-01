package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// minClient returns a MattermostClient with enough state for convertPost to work
// on text-only messages (no attachments).
func minClient() *MattermostClient {
	return &MattermostClient{
		UserID: "myuserid",
		users:  map[string]*mattermost.User{},
	}
}

func TestConvertPost_TextOnly(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "p1", Message: "hello world"}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if cm.Parts[0].Content.MsgType != event.MsgText {
		t.Fatalf("expected MsgText, got %v", cm.Parts[0].Content.MsgType)
	}
	if cm.Parts[0].Content.Body != "hello world" {
		t.Fatalf("unexpected body: %q", cm.Parts[0].Content.Body)
	}
}

func TestConvertPost_MarkdownBold(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "p1", Message: "**bold text**"}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cm.Parts) == 0 {
		t.Fatal("expected at least 1 part")
	}
	part := cm.Parts[0].Content
	if part.Format == event.FormatHTML && part.FormattedBody == "" {
		t.Fatal("expected non-empty FormattedBody for bold markdown")
	}
}

func TestConvertPost_EmptyMessage_FallbackPart(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "p1", Message: ""}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 fallback part, got %d", len(cm.Parts))
	}
	if cm.Parts[0].Content.MsgType != event.MsgText {
		t.Fatalf("expected MsgText fallback, got %v", cm.Parts[0].Content.MsgType)
	}
}

func TestConvertPost_WithThreadReply(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{
		ID:      "p1",
		Message: "reply text",
		RootID:  "rootPostID",
	}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cm.ReplyTo == nil {
		t.Fatal("expected ReplyTo to be set for thread reply")
	}
	if cm.ReplyTo.MessageID != networkid.MessageID("rootPostID") {
		t.Fatalf("unexpected ReplyTo ID: %v", cm.ReplyTo.MessageID)
	}
}

func TestConvertPost_NoReply(t *testing.T) {
	c := minClient()
	post := &mattermost.Post{ID: "p1", Message: "no reply"}

	cm, err := c.convertPost(context.Background(), nil, id.RoomID("!r:host"), post)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cm.ReplyTo != nil {
		t.Fatalf("expected nil ReplyTo, got: %+v", cm.ReplyTo)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// mediaMsgType
// ──────────────────────────────────────────────────────────────────────────────

func TestMediaMsgType_Image(t *testing.T) {
	if got := mediaMsgType("image/png"); got != event.MsgImage {
		t.Errorf("mediaMsgType(image/png) = %v, want MsgImage", got)
	}
}

func TestMediaMsgType_Video(t *testing.T) {
	if got := mediaMsgType("video/mp4"); got != event.MsgVideo {
		t.Errorf("mediaMsgType(video/mp4) = %v, want MsgVideo", got)
	}
}

func TestMediaMsgType_Audio(t *testing.T) {
	if got := mediaMsgType("audio/ogg"); got != event.MsgAudio {
		t.Errorf("mediaMsgType(audio/ogg) = %v, want MsgAudio", got)
	}
}

func TestMediaMsgType_Fallback(t *testing.T) {
	if got := mediaMsgType("application/zip"); got != event.MsgFile {
		t.Errorf("mediaMsgType(application/zip) = %v, want MsgFile", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// sniffMimeType
// ──────────────────────────────────────────────────────────────────────────────

func TestSniffMimeType_PNG(t *testing.T) {
	png := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	if got := sniffMimeType(png); got != "image/png" {
		t.Errorf("sniffMimeType(PNG) = %q, want %q", got, "image/png")
	}
}

func TestSniffMimeType_JPEG(t *testing.T) {
	jpeg := []byte{0xFF, 0xD8, 0xFF, 0xE0}
	if got := sniffMimeType(jpeg); got != "image/jpeg" {
		t.Errorf("sniffMimeType(JPEG) = %q, want %q", got, "image/jpeg")
	}
}

func TestSniffMimeType_GIF(t *testing.T) {
	gif := []byte{'G', 'I', 'F', '8', '9', 'a', 0x00}
	if got := sniffMimeType(gif); got != "image/gif" {
		t.Errorf("sniffMimeType(GIF) = %q, want %q", got, "image/gif")
	}
}

func TestSniffMimeType_Unknown(t *testing.T) {
	unknown := []byte{0x00, 0x01, 0x02, 0x03}
	if got := sniffMimeType(unknown); got != "" {
		t.Errorf("sniffMimeType(unknown) = %q, want empty", got)
	}
}

func TestSniffMimeType_TooShort(t *testing.T) {
	if got := sniffMimeType([]byte{0xFF, 0xD8}); got != "" {
		t.Errorf("sniffMimeType(too short) = %q, want empty", got)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// filenameForMime
// ──────────────────────────────────────────────────────────────────────────────

func TestFilenameForMime_Known(t *testing.T) {
	if got := filenameForMime("image/png"); got != "image.png" {
		t.Errorf("filenameForMime(image/png) = %q", got)
	}
}

func TestFilenameForMime_Unknown(t *testing.T) {
	if got := filenameForMime("application/x-custom"); got != "file" {
		t.Errorf("filenameForMime(unknown) = %q, want %q", got, "file")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// resolveFiles
// ──────────────────────────────────────────────────────────────────────────────

func TestResolveFiles_FromMetadata(t *testing.T) {
	post := &mattermost.Post{
		ID: "p1",
		Metadata: &mattermost.PostMetadata{
			Files: []*mattermost.FileInfo{{ID: "f1", Name: "img.png"}},
		},
	}
	files := resolveFiles(post)
	if len(files) != 1 || files[0].ID != "f1" {
		t.Errorf("unexpected files: %+v", files)
	}
}

func TestResolveFiles_FromFileIDs(t *testing.T) {
	post := &mattermost.Post{
		ID:      "p1",
		FileIDs: []string{"f1", "f2"},
	}
	files := resolveFiles(post)
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
}

func TestResolveFiles_Empty(t *testing.T) {
	post := &mattermost.Post{ID: "p1"}
	if files := resolveFiles(post); len(files) != 0 {
		t.Errorf("expected empty files, got %+v", files)
	}
}
