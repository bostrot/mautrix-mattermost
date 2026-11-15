package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type testUploadIntent struct{ bridgev2.MatrixAPI }

func (testUploadIntent) UploadMedia(context.Context, id.RoomID, []byte, string, string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	return "mxc://example/test", nil, nil
}

func TestAttachmentRootFirstPartUsesCanonicalID(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("test file")) }))
	defer s.Close()
	c := newBackfillClient(s.URL)
	post := &mattermost.Post{ID: "root", FileIDs: []string{"f1", "f2"}}
	result, err := c.convertPost(context.Background(), testUploadIntent{}, "!room:example", post)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Parts) != 2 {
		t.Fatalf("got %d parts, want 2", len(result.Parts))
	}
	if result.Parts[0].ID != "" {
		t.Errorf("first part ID = %q, want canonical empty ID", result.Parts[0].ID)
	}
	if result.Parts[1].ID != "att_f2" {
		t.Errorf("second part ID = %q, want att_f2", result.Parts[1].ID)
	}
}
