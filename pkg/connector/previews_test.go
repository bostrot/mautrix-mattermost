package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

const testPostID = "6iipo5hskp89bqqxoekgbhnoje"

func TestFindPermalinks(t *testing.T) {
	const server = "https://mm.fpl.dev"
	cases := []struct {
		name string
		text string
		want []string // post IDs
	}{
		{"plain permalink", "https://mm.fpl.dev/fpl/pl/" + testPostID, []string{testPostID}},
		{"with query and fragment", "https://mm.fpl.dev/fpl/pl/" + testPostID + "?focus=true#abc", []string{testPostID}},
		{"sentence punctuation", "see https://mm.fpl.dev/fpl/pl/" + testPostID + ".", []string{testPostID}},
		{"http and port", "http://mm.fpl.dev:8065/fpl/pl/" + testPostID, []string{testPostID}},
		{"other host untouched", "https://example.com/fpl/pl/" + testPostID, nil},
		{"same host, different path", "https://mm.fpl.dev/fpl/channels/town-square", nil},
		{"bare path without host", "/fpl/pl/" + testPostID, nil},
		{"short id is not a post", "https://mm.fpl.dev/fpl/pl/abc123", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := findPermalinks(tc.text, server)
			var ids []string
			for _, g := range got {
				ids = append(ids, g.PostID)
			}
			if fmt.Sprint(ids) != fmt.Sprint(tc.want) {
				t.Fatalf("findPermalinks = %v, want %v", ids, tc.want)
			}
			for _, g := range got {
				if !strings.Contains(tc.text, g.URL) {
					t.Errorf("URL %q is not a substring of the message", g.URL)
				}
			}
		})
	}
}

func TestFindPermalinksDedupesAndCaps(t *testing.T) {
	ids := []string{
		testPostID,
		"aaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccc",
	}
	var parts []string
	for i, id := range ids {
		parts = append(parts, "https://mm.fpl.dev/fpl/pl/"+id)
		if i == 0 {
			parts = append(parts, "https://mm.fpl.dev/fpl/pl/"+id) // duplicate, early
		}
	}
	got := findPermalinks(strings.Join(parts, " "), "https://mm.fpl.dev")
	if len(got) != maxPreviewsPerMessage {
		t.Fatalf("got %d permalinks, want the cap of %d", len(got), maxPreviewsPerMessage)
	}
	seen := map[string]bool{}
	for _, g := range got {
		if seen[g.PostID] {
			t.Fatalf("duplicate preview for %s", g.PostID)
		}
		seen[g.PostID] = true
	}
}

// previewServer serves the three endpoints a card needs and counts requests.
func previewServer(t *testing.T, requests *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*requests++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v4/posts/"+testPostID:
			_ = json.NewEncoder(w).Encode(mattermost.Post{
				ID: testPostID, UserID: "u1", ChannelID: "c1",
				Message: "The backup worked.\n\nLinks to MM messages are pretty slick.",
			})
		case r.URL.Path == "/api/v4/users/u1":
			_ = json.NewEncoder(w).Encode(mattermost.User{ID: "u1", FirstName: "Avery", LastName: "Wagar"})
		case r.URL.Path == "/api/v4/channels/c1":
			_ = json.NewEncoder(w).Encode(mattermost.Channel{ID: "c1", Name: "town-square", DisplayName: "Town Square"})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"id":"app.post.get.app_error","message":"not found","status_code":404}`))
		}
	}))
}

func TestResolvePermalinkBuildsCard(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}

	p := c.resolvePermalink(testPostID)
	if p == nil {
		t.Fatal("no preview built")
	}
	if p.Title != "Avery Wagar in Town Square" {
		t.Errorf("title = %q, want the author and channel", p.Title)
	}
	if !strings.Contains(p.Description, "The backup worked.") || strings.Contains(p.Description, "\n") {
		t.Errorf("description = %q, want the message flattened onto one line", p.Description)
	}
	if p.SiteName != "Mattermost" || p.Type != "article" {
		t.Errorf("site/type = %q/%q", p.SiteName, p.Type)
	}

	before := requests
	if second := c.resolvePermalink(testPostID); second == nil || second.Title != p.Title {
		t.Fatal("cached lookup did not return the same card")
	}
	if requests != before {
		t.Errorf("cache miss: %d extra requests", requests-before)
	}
}

func TestResolvePermalinkFailsQuietly(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}

	// A post the server does not have, and one the user cannot read.
	if p := c.resolvePermalink("zzzzzzzzzzzzzzzzzzzzzzzzzz"); p != nil {
		t.Errorf("got a preview for a missing post: %+v", p)
	}
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"id":"app.post.get.app_error","status_code":403}`))
	}))
	defer forbidden.Close()
	c.baseURL = forbidden.URL
	if p := c.resolvePermalink(testPostID); p != nil {
		t.Errorf("got a preview for a forbidden post: %+v", p)
	}
	// A soft-deleted post yields nothing.
	deleted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mattermost.Post{ID: testPostID, UserID: "u1", DeleteAt: 12345, Message: "gone"})
	}))
	defer deleted.Close()
	c.baseURL = deleted.URL
	if p := c.resolvePermalink(testPostID); p != nil {
		t.Errorf("built a preview for a deleted post: %+v", p)
	}

	// No token at all: nothing is attempted and nothing panics.
	noTok := &MattermostClient{}
	if p := noTok.resolvePermalink(testPostID); p != nil {
		t.Errorf("built a preview without a token: %+v", p)
	}
}

func TestAttachPermalinkPreviewsMatchesURL(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}

	// The permalink host must be the bridge's own server, as it is in production.
	url := srv.URL + "/fpl/pl/" + testPostID
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "look: " + url}
	c.attachPermalinkPreviews(context.Background(), content, "look: "+url)
	if len(content.BeeperLinkPreviews) != 1 {
		t.Fatalf("attached %d previews, want 1", len(content.BeeperLinkPreviews))
	}
	if got := content.BeeperLinkPreviews[0].MatchedURL; got != url {
		t.Errorf("MatchedURL = %q, want the link exactly as written", got)
	}
	// A cancelled context attaches nothing.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	other := &event.MessageEventContent{}
	c.attachPermalinkPreviews(cancelled, other, url)
	if len(other.BeeperLinkPreviews) != 0 {
		t.Error("attached previews with a cancelled context")
	}
}

// The integration point: a text message with a permalink must reach the part
// that becomes the Matrix event, with the preview attached.
func TestConvertPostAttachesPermalinkPreview(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	url := srv.URL + "/fpl/pl/" + testPostID
	c := &MattermostClient{Token: "tok", baseURL: srv.URL, UserID: "me"}
	post := &mattermost.Post{ID: "p1", UserID: "u2", ChannelID: "c1", Message: url}

	cm, err := c.convertPost(context.Background(), nil, "!room:test", post)
	if err != nil || cm == nil || len(cm.Parts) == 0 {
		t.Fatalf("convertPost = %v, %v", cm, err)
	}
	if len(cm.Parts[0].Content.BeeperLinkPreviews) != 1 {
		t.Fatalf("parts carry %d previews, want 1", len(cm.Parts[0].Content.BeeperLinkPreviews))
	}
	// A message without a permalink is untouched.
	plain, _ := c.convertPost(context.Background(), nil, "!room:test", &mattermost.Post{ID: "p2", Message: "no links here"})
	if len(plain.Parts[0].Content.BeeperLinkPreviews) != 0 {
		t.Error("attached a preview to a message with no permalink")
	}
}

func TestPreviewExcerpt(t *testing.T) {
	if got := previewExcerpt("short\nmessage"); got != "short message" {
		t.Errorf("excerpt = %q", got)
	}
	long := strings.Repeat("a", maxPreviewDescription+50)
	got := previewExcerpt(long)
	if !strings.HasSuffix(got, "…") || len([]rune(got)) > maxPreviewDescription+1 {
		t.Errorf("long excerpt not trimmed: %d runes", len([]rune(got)))
	}
}

var _ = bridgev2.ConvertedMessage{}
