package connector

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type testUploadIntent struct {
	bridgev2.MatrixAPI
	failFilename string
}

func (intent testUploadIntent) UploadMedia(_ context.Context, _ id.RoomID, _ []byte, filename, _ string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	if filename == intent.failFilename {
		return "", nil, errors.New("test upload failure")
	}
	return "mxc://example/test", nil, nil
}

func TestAttachmentFirstSuccessfulUploadUsesCanonicalID(t *testing.T) {
	for _, tc := range []struct {
		name, failFilename, root string
	}{
		{name: "thread root"},
		{name: "reply with failed first upload", failFilename: "failed-file", root: "root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeThreadHTTP(t, func(req *http.Request) (int, string) {
				switch req.Method + " " + req.URL.Path {
				case "GET /api/v4/files/failed-file", "GET /api/v4/files/first-file", "GET /api/v4/files/second-file":
					return 200, "test file"
				default:
					t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
					return 404, ""
				}
			})
			files := []string{"first-file", "second-file"}
			if tc.failFilename != "" {
				files = append([]string{tc.failFilename}, files...)
			}
			post := &mattermost.Post{ID: "post", FileIDs: files, RootID: tc.root, Message: "attachment caption"}
			c := newThreadTestClient()
			result, err := c.convertPost(context.Background(), testUploadIntent{failFilename: tc.failFilename}, "!room:example", post)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Parts) != 2 {
				t.Fatalf("got %d parts, want 2", len(result.Parts))
			}
			if first := result.Parts[0]; first.ID != "" || first.Content.Body != "attachment caption" || first.Content.FileName != "first-file" {
				t.Errorf("first part = %q, body %q, filename %q", first.ID, first.Content.Body, first.Content.FileName)
			}
			if result.Parts[1].ID != "att_second-file" {
				t.Errorf("second part ID = %q, want att_second-file", result.Parts[1].ID)
			}
			if tc.root == "" {
				if result.ThreadRoot != nil {
					t.Errorf("root post has thread root %q", *result.ThreadRoot)
				}
			} else if result.ThreadRoot == nil || string(*result.ThreadRoot) != tc.root {
				t.Errorf("thread root = %v, want %q", result.ThreadRoot, tc.root)
			}
		})
	}
}
