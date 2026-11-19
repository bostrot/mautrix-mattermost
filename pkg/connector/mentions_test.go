package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

const (
	selfMXID  = id.UserID("@me:example.org")
	ghostHost = ":example.org"
)

var mentionUsers = []*mattermost.User{
	{ID: "uself", Username: "carol", FirstName: "Carol", LastName: "Self"},
	{ID: "ualice", Username: "alice", FirstName: "Alice", LastName: "Example"},
	{ID: "ubob", Username: "bob.smith", Nickname: "Bobby"},
	{ID: "uund", Username: "dee_dee", FirstName: "Dee"},
}

// mentionServer answers the username batch lookup and single-user lookups,
// recording each batch request.
type mentionServer struct {
	*httptest.Server
	mu      sync.Mutex
	batches [][]string
	fail    bool
}

func newMentionServer(t *testing.T) *mentionServer {
	t.Helper()
	ms := &mentionServer{}
	ms.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ms.mu.Lock()
		fail := ms.fail
		ms.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v4/users/usernames":
			var names []string
			_ = json.NewDecoder(r.Body).Decode(&names)
			ms.mu.Lock()
			ms.batches = append(ms.batches, names)
			ms.mu.Unlock()
			var out []*mattermost.User
			for _, n := range names {
				for _, u := range mentionUsers {
					if u.Username == n {
						out = append(out, u)
					}
				}
			}
			if out == nil {
				out = []*mattermost.User{}
			}
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v4/users/"):
			uid := strings.TrimPrefix(r.URL.Path, "/api/v4/users/")
			for _, u := range mentionUsers {
				if u.ID == uid {
					_ = json.NewEncoder(w).Encode(u)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ms.Close)
	return ms
}

func (ms *mentionServer) batchCount() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.batches)
}

func ghostMXID(mmID string) id.UserID { return id.UserID("@mm_" + mmID + ghostHost) }

func newMentionClient(srv *mentionServer) *MattermostClient {
	return &MattermostClient{
		nameFormat: mattermost.NameFormatNicknameFullName,
		Token:      "tok",
		baseURL:    srv.URL,
		UserID:     "uself",
		Username:   "carol",
		UserLogin:  &bridgev2.UserLogin{UserLogin: &database.UserLogin{UserMXID: selfMXID}},
		ghostMXIDFn: func(mmID string) id.UserID {
			return ghostMXID(mmID)
		},
		parseGhostFn: func(mxid id.UserID) string {
			s := string(mxid)
			if strings.HasPrefix(s, "@mm_") && strings.HasSuffix(s, ghostHost) {
				return strings.TrimSuffix(strings.TrimPrefix(s, "@mm_"), ghostHost)
			}
			return ""
		},
	}
}

func pill(mxid id.UserID, name string) string {
	return `<a href="` + mxid.URI().MatrixToURL() + `">` + name + `</a>`
}

func TestInboundMentions(t *testing.T) {
	cases := []struct {
		name      string
		message   string
		wantHTML  []string // substrings of FormattedBody
		notHTML   []string // must not appear in FormattedBody
		wantUsers []id.UserID
		wantRoom  bool
		noChange  bool // content must equal plain RenderMarkdown output
	}{
		{name: "ghost", message: "@alice can you look?",
			wantHTML: []string{pill(ghostMXID("ualice"), "Alice Example") + " can you look?"}, wantUsers: []id.UserID{ghostMXID("ualice")}},
		{name: "logged-in user is mentioned as their real Matrix account", message: "thanks @carol",
			wantHTML: []string{pill(selfMXID, "Carol Self")}, wantUsers: []id.UserID{selfMXID}},
		{name: "case-insensitive", message: "@Alice hi",
			wantHTML: []string{pill(ghostMXID("ualice"), "Alice Example")}, wantUsers: []id.UserID{ghostMXID("ualice")}},
		{name: "trailing period stays text", message: "ask @alice.",
			wantHTML: []string{pill(ghostMXID("ualice"), "Alice Example") + "."}, wantUsers: []id.UserID{ghostMXID("ualice")}},
		{name: "username with a dot", message: "@bob.smith, ping",
			wantHTML: []string{pill(ghostMXID("ubob"), "Bobby") + ", ping"}, wantUsers: []id.UserID{ghostMXID("ubob")}},
		{name: "username with an underscore", message: "hey @dee_dee",
			wantHTML: []string{pill(ghostMXID("uund"), "Dee")}, wantUsers: []id.UserID{ghostMXID("uund")}},
		{name: "two users, each listed once", message: "@alice and @bob.smith and @alice again",
			wantUsers: []id.UserID{ghostMXID("ualice"), ghostMXID("ubob")}},
		{name: "markdown kept", message: "**urgent** @alice",
			wantHTML: []string{"<strong>urgent</strong> " + pill(ghostMXID("ualice"), "Alice Example")}, wantUsers: []id.UserID{ghostMXID("ualice")}},
		{name: "text is escaped, not injected", message: "a <b> & @alice <i> & x",
			wantHTML: []string{"&lt;b&gt; &amp; " + pill(ghostMXID("ualice"), "Alice Example") + " &lt;i&gt; &amp; x"}, notHTML: []string{"<b>", "<i>"}, wantUsers: []id.UserID{ghostMXID("ualice")}},
		{name: "unknown user stays text", message: "@nobody hi", noChange: true},
		{name: "email is not a mention", message: "mail x@alice.com", noChange: true},
		{name: "address whose domain is a username is not a mention", message: "mail x@alice or me@carol", noChange: true},
		{name: "path is not a mention", message: "see docs/@alice", noChange: true},
		{name: "inline code untouched", message: "run `@alice` now", noChange: true},
		{name: "code block untouched", message: "```\n@alice\n```", noChange: true},
		{name: "channel-wide mention", message: "@channel standup in 5", wantRoom: true},
		{name: "here", message: "@here anyone?", wantRoom: true},
		{name: "channel-wide plus user", message: "@all and @alice", wantRoom: true, wantUsers: []id.UserID{ghostMXID("ualice")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newMentionServer(t)
			c := newMentionClient(srv)
			got := c.renderPostText(context.Background(), tc.message)
			plain := renderWithoutMentions(tc.message)
			if tc.noChange {
				if got.Body != plain.Body || got.FormattedBody != plain.FormattedBody {
					t.Fatalf("content changed:\n got  %q / %q\n want %q / %q", got.Body, got.FormattedBody, plain.Body, plain.FormattedBody)
				}
				if got.Mentions != nil && (len(got.Mentions.UserIDs) > 0 || got.Mentions.Room) {
					t.Fatalf("unexpected mentions %+v", got.Mentions)
				}
				return
			}
			for _, s := range tc.wantHTML {
				if !strings.Contains(got.FormattedBody, s) {
					t.Errorf("FormattedBody %q lacks %q", got.FormattedBody, s)
				}
			}
			for _, s := range tc.notHTML {
				if strings.Contains(got.FormattedBody, s) {
					t.Errorf("FormattedBody %q contains %q", got.FormattedBody, s)
				}
			}
			if len(tc.wantUsers) > 0 && got.Format != event.FormatHTML {
				t.Errorf("Format = %q, want HTML", got.Format)
			}
			if got.Mentions == nil {
				t.Fatalf("Mentions is nil")
			}
			if strings.Join(idStrings(got.Mentions.UserIDs), ",") != strings.Join(idStrings(tc.wantUsers), ",") {
				t.Errorf("Mentions.UserIDs = %v, want %v", got.Mentions.UserIDs, tc.wantUsers)
			}
			if got.Mentions.Room != tc.wantRoom {
				t.Errorf("Mentions.Room = %v, want %v", got.Mentions.Room, tc.wantRoom)
			}
			if strings.Contains(got.Body, "<a ") {
				t.Errorf("plain body contains HTML: %q", got.Body)
			}
		})
	}
}

// renderWithoutMentions is the content a message got before mentions existed.
func renderWithoutMentions(message string) event.MessageEventContent {
	c := &MattermostClient{}
	content := c.renderPostTextNoMentions(message)
	return content
}

func idStrings(ids []id.UserID) []string {
	out := make([]string, len(ids))
	for i, u := range ids {
		out[i] = string(u)
	}
	return out
}

func TestInboundMentionsBatchAndCache(t *testing.T) {
	srv := newMentionServer(t)
	c := newMentionClient(srv)
	ctx := context.Background()

	c.renderPostText(ctx, "@alice @bob.smith @nobody")
	if n := srv.batchCount(); n != 1 {
		t.Fatalf("first message made %d username lookups, want 1 batch", n)
	}
	c.renderPostText(ctx, "@alice @bob.smith @nobody again")
	if n := srv.batchCount(); n != 1 {
		t.Fatalf("known and recently-missing names were looked up again: %d batches", n)
	}
	// Users already known from channel membership need no request at all.
	c2 := newMentionClient(srv)
	c2.users = map[string]*mattermost.User{"uund": mentionUsers[3]}
	got := c2.renderPostText(ctx, "@dee_dee")
	if n := srv.batchCount(); n != 1 {
		t.Fatalf("a cached member was looked up over REST: %d batches", n)
	}
	if got.Mentions == nil || len(got.Mentions.UserIDs) != 1 {
		t.Fatalf("cached member not mentioned: %+v", got.Mentions)
	}
}

func TestInboundMentionsLookupFailure(t *testing.T) {
	srv := newMentionServer(t)
	c := newMentionClient(srv)
	ctx := context.Background()

	srv.mu.Lock()
	srv.fail = true
	srv.mu.Unlock()
	got := c.renderPostText(ctx, "@alice hi")
	if got.FormattedBody != "" || got.Body != "@alice hi" {
		t.Fatalf("failed lookup changed the message: %q / %q", got.Body, got.FormattedBody)
	}

	// A failed request is not remembered as "no such user".
	srv.mu.Lock()
	srv.fail = false
	srv.mu.Unlock()
	got = c.renderPostText(ctx, "@alice hi")
	if got.Mentions == nil || len(got.Mentions.UserIDs) != 1 {
		t.Fatalf("mention not resolved after the server recovered: %+v", got.Mentions)
	}
}

func TestOutboundMentions(t *testing.T) {
	cases := []struct {
		name    string
		content event.MessageEventContent
		want    string
	}{
		{name: "ghost pill becomes @username",
			content: htmlContent(pill(ghostMXID("ualice"), "Alice Example") + ": can you look?"),
			want:    "@alice: can you look?"},
		{name: "own account pill",
			content: htmlContent("note to " + pill(selfMXID, "me")),
			want:    "note to @carol"},
		{name: "username with underscore is not escaped",
			content: htmlContent(pill(ghostMXID("uund"), "Dee") + " hi"),
			want:    "@dee_dee hi"},
		{name: "username with a dot",
			content: htmlContent("hey " + pill(ghostMXID("ubob"), "Bobby")),
			want:    "hey @bob.smith"},
		{name: "Matrix user with no Mattermost account keeps their name",
			content: htmlContent(pill("@stranger:example.com", "Stranger") + " hi"),
			want:    "Stranger hi"},
		{name: "typed mention passes through",
			content: event.MessageEventContent{MsgType: event.MsgText, Body: "@alice hi"},
			want:    "@alice hi"},
		{name: "@room becomes @channel when the room is mentioned",
			content: event.MessageEventContent{MsgType: event.MsgText, Body: "@room standup", Mentions: &event.Mentions{Room: true}},
			want:    "@channel standup"},
		{name: "@room left alone without a room mention",
			content: event.MessageEventContent{MsgType: event.MsgText, Body: "@room standup"},
			want:    "@room standup"},
		{name: "@room inside an address is not a room mention",
			content: event.MessageEventContent{MsgType: event.MsgText, Body: "mail me@room.example", Mentions: &event.Mentions{Room: true}},
			want:    "mail me@room.example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newMentionServer(t)
			c := newMentionClient(srv)
			content := tc.content
			if got := c.outboundMarkdown(context.Background(), &content); got != tc.want {
				t.Fatalf("outboundMarkdown = %q, want %q", got, tc.want)
			}
		})
	}
}

func htmlContent(formatted string) event.MessageEventContent {
	return event.MessageEventContent{MsgType: event.MsgText, Body: "fallback", Format: event.FormatHTML, FormattedBody: formatted}
}

func TestSplitHTMLTextProtectsCodeAndLinks(t *testing.T) {
	in := `a <code>@x</code> b <pre><code>@y</code></pre> c <a href="u">@z</a> d`
	var text []string
	var joined strings.Builder
	for _, s := range splitHTMLText(in) {
		joined.WriteString(s.s)
		if s.text {
			text = append(text, s.s)
		}
	}
	if joined.String() != in {
		t.Fatalf("segments do not reassemble the input: %q", joined.String())
	}
	for _, s := range text {
		if strings.Contains(s, "@") {
			t.Fatalf("protected text returned as rewritable: %q (all: %q)", s, text)
		}
	}
	if strings.Join(text, "|") != "a | b | c | d" {
		t.Fatalf("rewritable text = %q", text)
	}
}

// TestOutboundPlainTextMentions covers how Beeper actually sends a mention:
// plain "@Display Name" text plus m.mentions, with no HTML pill.
func TestOutboundPlainTextMentions(t *testing.T) {
	ghostNames := map[string]string{
		"ualice": "Alice Example",
		"ubob":   "Bobby ",  // stale ghost name with a trailing space
		"uund":   "Dee Dee", // ghost name differs from the Mattermost display name
	}
	cases := []struct {
		name     string
		body     string
		mentions []id.UserID
		want     string
	}{
		{"display name differs from username", "@Alice Example and hi", []id.UserID{ghostMXID("ualice")}, "@alice and hi"},
		{"two people, as Beeper sends them", "@Alice Example and @Bobby  testing", []id.UserID{ghostMXID("ualice"), ghostMXID("ubob")}, "@alice and @bob.smith  testing"},
		{"ghost name or current display name", "@Dee Dee / @Dee", []id.UserID{ghostMXID("uund")}, "@dee_dee / @dee_dee"},
		{"at end of text", "thanks @Alice Example", []id.UserID{ghostMXID("ualice")}, "thanks @alice"},
		{"punctuation after the name", "@Alice Example, look", []id.UserID{ghostMXID("ualice")}, "@alice, look"},
		{"case-insensitive", "@alice example hi", []id.UserID{ghostMXID("ualice")}, "@alice hi"},
		{"same person twice in a row", "@Alice Example @Alice Example", []id.UserID{ghostMXID("ualice")}, "@alice @alice"},
		{"own account", "note to @Carol Self", []id.UserID{selfMXID}, "note to @carol"},
		{"not listed in m.mentions: untouched", "@Alice Example hi", nil, "@Alice Example hi"},
		{"longer name is not cut short", "@Alice Examples hi", []id.UserID{ghostMXID("ualice")}, "@Alice Examples hi"},
		{"email address untouched", "mail x@Alice Example", []id.UserID{ghostMXID("ualice")}, "mail x@Alice Example"},
		{"unknown Matrix user untouched", "@Stranger hi", []id.UserID{"@stranger:example.com"}, "@Stranger hi"},
		{"username starting with the name is replaced once", "@Bobby. hi @Bobby", []id.UserID{ghostMXID("ubob")}, "@bob.smith. hi @bob.smith"},
		{"non-ASCII letter after the name is not a boundary", "@Alice Exampleé hi", []id.UserID{ghostMXID("ualice")}, "@Alice Exampleé hi"},
		{"doubled @ is not a mention", "@@Alice Example hi", []id.UserID{ghostMXID("ualice")}, "@@Alice Example hi"},
		{"non-ASCII letter before the @ is not a boundary", "é@Alice Example hi", []id.UserID{ghostMXID("ualice")}, "é@Alice Example hi"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newMentionServer(t)
			c := newMentionClient(srv)
			c.ghostNameFn = func(mmID string) string { return ghostNames[mmID] }
			content := event.MessageEventContent{MsgType: event.MsgText, Body: tc.body}
			if tc.mentions != nil {
				content.Mentions = &event.Mentions{UserIDs: tc.mentions}
			}
			if got := c.outboundMarkdown(context.Background(), &content); got != tc.want {
				t.Fatalf("outboundMarkdown = %q, want %q", got, tc.want)
			}
		})
	}
}

func plainMention(body string, users ...id.UserID) event.MessageEventContent {
	return event.MessageEventContent{MsgType: event.MsgText, Body: body, Mentions: &event.Mentions{UserIDs: users}}
}

// Mentions inside an integration's attachments are shown as pills but only
// the post's own message pings anyone.
func TestAttachmentMentionsDoNotPing(t *testing.T) {
	srv := newMentionServer(t)
	c := newMentionClient(srv)
	attach := map[string]any{"attachments": []any{map[string]any{"text": "@channel ping @alice"}}}

	got := c.renderPost(context.Background(), &mattermost.Post{Message: "Deploy done", Props: attach})
	if !strings.Contains(got.FormattedBody, string(ghostMXID("ualice"))) {
		t.Errorf("attachment mention not rendered as a pill: %q", got.FormattedBody)
	}
	if got.Mentions == nil || got.Mentions.Room || len(got.Mentions.UserIDs) > 0 {
		t.Fatalf("attachment mentions ping: %+v", got.Mentions)
	}

	got = c.renderPost(context.Background(), &mattermost.Post{Message: "cc @alice", Props: attach})
	if got.Mentions == nil || got.Mentions.Room || len(got.Mentions.UserIDs) != 1 || got.Mentions.UserIDs[0] != ghostMXID("ualice") {
		t.Fatalf("message mention lost or attachment @channel pings: %+v", got.Mentions)
	}
}
