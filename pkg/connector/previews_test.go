package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

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

func TestResolvePermalinkBuildsQuote(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}

	q := c.resolvePermalink(testPostID)
	if q == nil {
		t.Fatal("nothing resolved")
	}
	if q.Title != "Avery Wagar in Town Square" {
		t.Errorf("title = %q, want the author and channel", q.Title)
	}
	if q.Text != "The backup worked.\n\nLinks to MM messages are pretty slick." {
		t.Errorf("text = %q, want the whole post with its line breaks", q.Text)
	}

	before := requests
	if again := c.resolvePermalink(testPostID); again == nil || again.Title != q.Title {
		t.Fatal("cached lookup did not return the same post")
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

	if q := c.resolvePermalink("zzzzzzzzzzzzzzzzzzzzzzzzzz"); q != nil {
		t.Errorf("resolved a missing post: %+v", q)
	}
	// A failure is cached too: the second lookup makes no request.
	before := requests
	c.resolvePermalink("zzzzzzzzzzzzzzzzzzzzzzzzzz")
	if requests != before {
		t.Errorf("failed lookup was not cached: %d extra requests", requests-before)
	}

	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"id":"app.post.get.app_error","status_code":403}`))
	}))
	defer forbidden.Close()
	c2 := &MattermostClient{Token: "tok", baseURL: forbidden.URL}
	if q := c2.resolvePermalink(testPostID); q != nil {
		t.Errorf("resolved a forbidden post: %+v", q)
	}

	deleted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mattermost.Post{ID: testPostID, UserID: "u1", DeleteAt: 12345, Message: "gone"})
	}))
	defer deleted.Close()
	c3 := &MattermostClient{Token: "tok", baseURL: deleted.URL}
	if q := c3.resolvePermalink(testPostID); q != nil {
		t.Errorf("resolved a deleted post: %+v", q)
	}

	if q := (&MattermostClient{}).resolvePermalink(testPostID); q != nil {
		t.Errorf("resolved without a token: %+v", q)
	}
}

func TestAttachPermalinkQuotes(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}

	url := srv.URL + "/fpl/pl/" + testPostID
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "look: " + url}
	c.attachPermalinkQuotes(context.Background(), content, content.Body)

	if len(content.BeeperLinkPreviews) != 0 {
		t.Errorf("attached %d preview cards; the quote replaces the card", len(content.BeeperLinkPreviews))
	}
	wantPlain := "look: " + url + "\n\n> Avery Wagar in Town Square\n> The backup worked.\n> \n> Links to MM messages are pretty slick."
	if content.Body != wantPlain {
		t.Errorf("body =\n%q\nwant\n%q", content.Body, wantPlain)
	}
	if content.Format != event.FormatHTML {
		t.Errorf("format = %q, want HTML", content.Format)
	}
	for _, want := range []string{
		"look: " + url, // the original message comes first
		"<blockquote>",
		`<a href="` + url + `"><strong>Avery Wagar in Town Square</strong></a>`,
		"The backup worked.",
		"Links to MM messages are pretty slick.",
		"</blockquote>",
	} {
		if !strings.Contains(content.FormattedBody, want) {
			t.Errorf("HTML body missing %q:\n%s", want, content.FormattedBody)
		}
	}
	if !strings.HasPrefix(content.FormattedBody, "look: ") {
		t.Errorf("HTML body does not start with the original message: %q", content.FormattedBody)
	}

	// A cancelled context leaves the message untouched.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	other := &event.MessageEventContent{Body: url}
	c.attachPermalinkQuotes(cancelled, other, url)
	if other.Body != url || other.FormattedBody != "" {
		t.Error("changed the message with a cancelled context")
	}
	// A link that does not resolve leaves the message untouched.
	dead := &event.MessageEventContent{Body: srv.URL + "/fpl/pl/zzzzzzzzzzzzzzzzzzzzzzzzzz"}
	c.attachPermalinkQuotes(context.Background(), dead, dead.Body)
	if dead.FormattedBody != "" || strings.Contains(dead.Body, ">") {
		t.Errorf("changed a message whose link did not resolve: %+v", dead)
	}
}

func TestAttachPermalinkQuotesRendersMarkdownAndEscapes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v4/posts/" + testPostID:
			_ = json.NewEncoder(w).Encode(mattermost.Post{ID: testPostID, UserID: "u1",
				Message: "**Nightly backup OK**\n- Archive: `mm.tar`\n<script>x</script>"})
		case "/api/v4/users/u1":
			_ = json.NewEncoder(w).Encode(mattermost.User{ID: "u1", FirstName: "A <b>", LastName: "&"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}
	url := srv.URL + "/fpl/pl/" + testPostID
	// An existing HTML body is kept and the quote is appended to it.
	content := &event.MessageEventContent{Body: url, Format: event.FormatHTML, FormattedBody: "<em>see</em> " + url}
	c.attachPermalinkQuotes(context.Background(), content, url)

	if !strings.HasPrefix(content.FormattedBody, "<em>see</em> ") {
		t.Errorf("existing HTML body was replaced: %q", content.FormattedBody)
	}
	for _, want := range []string{"<strong>Nightly backup OK</strong>", "<code>mm.tar</code>", "A &lt;b&gt; &amp;"} {
		if !strings.Contains(content.FormattedBody, want) {
			t.Errorf("HTML body missing %q:\n%s", want, content.FormattedBody)
		}
	}
	if strings.Contains(content.FormattedBody, "<script>") || strings.Contains(content.FormattedBody, "<b>") {
		t.Errorf("unescaped HTML from the post or the name reached the body:\n%s", content.FormattedBody)
	}
}

func TestAttachPermalinkQuotesBudget(t *testing.T) {
	long := strings.Repeat("é", maxQuotedBytes) // 2 bytes each: twice the budget
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		id := strings.TrimPrefix(r.URL.Path, "/api/v4/posts/")
		if len(id) != 26 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(mattermost.Post{ID: id, Message: long})
	}))
	defer srv.Close()
	c := &MattermostClient{Token: "tok", baseURL: srv.URL}
	text := srv.URL + "/fpl/pl/aaaaaaaaaaaaaaaaaaaaaaaaaa " + srv.URL + "/fpl/pl/bbbbbbbbbbbbbbbbbbbbbbbbbb"
	content := &event.MessageEventContent{Body: text}
	c.attachPermalinkQuotes(context.Background(), content, text)

	added := len(content.Body) - len(text)
	// Two titles, quote markers and ellipses on top of the budget, nothing more.
	if added > maxQuotedBytes+200 {
		t.Errorf("quotes added %d bytes, budget is %d", added, maxQuotedBytes)
	}
	if !utf8.ValidString(content.Body) || !utf8.ValidString(content.FormattedBody) {
		t.Error("a quote was cut in the middle of a UTF-8 sequence")
	}
	if !strings.Contains(content.Body, "…") {
		t.Error("an over-budget post was not marked as shortened")
	}
}

// The integration points: new messages and edits both carry the quote.
func TestConvertPostAndEditQuotePermalink(t *testing.T) {
	var requests int
	srv := previewServer(t, &requests)
	defer srv.Close()
	url := srv.URL + "/fpl/pl/" + testPostID
	c := &MattermostClient{Token: "tok", baseURL: srv.URL, UserID: "me"}

	cm, err := c.convertPost(context.Background(), nil, "!room:test", &mattermost.Post{ID: "p1", UserID: "u2", ChannelID: "c1", Message: url})
	if err != nil || cm == nil || len(cm.Parts) == 0 {
		t.Fatalf("convertPost = %v, %v", cm, err)
	}
	if !strings.Contains(cm.Parts[0].Content.FormattedBody, "<blockquote>") {
		t.Fatalf("converted message has no quote: %q", cm.Parts[0].Content.FormattedBody)
	}
	if len(cm.Parts[0].Content.BeeperLinkPreviews) != 0 {
		t.Error("converted message still carries a preview card")
	}
	edited := c.editContent(context.Background(), &mattermost.Post{ID: "p1", Message: "edited: " + url})
	if !strings.HasPrefix(edited.Body, "edited: ") || !strings.Contains(edited.FormattedBody, "<blockquote>") {
		t.Errorf("an edit dropped the quote: %+v", edited)
	}
	plain, _ := c.convertPost(context.Background(), nil, "!room:test", &mattermost.Post{ID: "p2", Message: "no links here"})
	if strings.Contains(plain.Parts[0].Content.FormattedBody, "<blockquote>") {
		t.Error("quoted something in a message with no permalink")
	}
}

func TestNormalizePostText(t *testing.T) {
	post := "✅ **Nightly backup OK** — 2026-11-16 03:43 PST\r\n" +
		"- Archive: `mm-20261116T114242Z.tar` (584.3 MiB), checksum verified\n" +
		"- VM copy: removed   \n\n\n\n" +
		"- Backups on TrueNAS: 3 · oldest 2026-11-14"
	want := "✅ **Nightly backup OK** — 2026-11-16 03:43 PST\n" +
		"- Archive: `mm-20261116T114242Z.tar` (584.3 MiB), checksum verified\n" +
		"- VM copy: removed\n\n" +
		"- Backups on TrueNAS: 3 · oldest 2026-11-14"
	if got := normalizePostText(post); got != want {
		t.Errorf("normalized =\n%q\nwant\n%q", got, want)
	}
	medium := strings.Repeat("word ", 800)
	if got := normalizePostText(medium); got != strings.TrimSpace(medium) {
		t.Errorf("a %d-byte post was shortened to %d bytes", len(medium), len(got))
	}
}

func TestTruncateUTF8(t *testing.T) {
	text := "aé€😀z" // 1, 2, 3, 4 and 1 bytes
	for limit := 0; limit <= len(text)+1; limit++ {
		got := truncateUTF8(text, limit)
		if !utf8.ValidString(got) {
			t.Errorf("limit %d: invalid UTF-8 %q", limit, got)
		}
		if limit >= len(text) && got != text {
			t.Errorf("limit %d: shortened a text that fits: %q", limit, got)
		}
		if limit < len(text) && (!strings.HasSuffix(got, "…") || len(strings.TrimSuffix(got, "…")) > limit) {
			t.Errorf("limit %d: %q is over the limit or unmarked", limit, got)
		}
	}
}
