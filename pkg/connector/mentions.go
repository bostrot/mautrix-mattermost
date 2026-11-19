package connector

import (
	"context"
	"html"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Mentions in both directions.
//
// Mattermost -> Matrix: "@username" in a post becomes a Matrix mention pill
// (a matrix.to link to the user's ghost, or to the logged-in Matrix user when
// they are the one tagged) and is listed in m.mentions, so the client
// highlights and notifies. "@channel", "@here" and "@all" become a room
// mention. Text inside code, pre and existing links is left alone.
//
// Matrix -> Mattermost: a pill becomes "@username", and "@room" becomes
// "@channel" when the event mentions the room.

// renderPostText converts a Mattermost post's text to Matrix message content:
// emoji shortcodes, Markdown, then mentions. New posts, captions and edits all
// go through here so they render the same way.
func (c *MattermostClient) renderPostText(ctx context.Context, message string) event.MessageEventContent {
	content := c.renderPostTextNoMentions(message)
	c.applyInboundMentions(ctx, &content)
	return content
}

// renderPostTextNoMentions is renderPostText without the mention pass.
func (c *MattermostClient) renderPostTextNoMentions(message string) event.MessageEventContent {
	content := format.RenderMarkdown(replaceEmojiShortcodes(message), true, false)
	content.MsgType = event.MsgText
	return content
}

// mentionCandidate matches a Mattermost-style mention. The character before
// the "@" is checked separately (Go regexp has no lookbehind), so that email
// addresses and the like are not treated as mentions.
var mentionCandidate = regexp.MustCompile(`@([a-zA-Z][a-zA-Z0-9._-]*)`)

// roomMentionWords are Mattermost's channel-wide mentions.
var roomMentionWords = map[string]bool{"channel": true, "here": true, "all": true}

// unknownUsernameTTL bounds how long a username that matched no Mattermost
// user is remembered, so a user created later is eventually picked up while
// ordinary "@words" do not trigger a lookup on every message.
const unknownUsernameTTL = 10 * time.Minute

type mentionCache struct {
	mu      sync.Mutex
	byName  map[string]*mattermost.User // lower-case username -> user
	unknown map[string]time.Time        // lower-case username -> when it was found missing
}

// mentionAllowedBefore reports whether r may precede an "@" for it to start a
// mention: start of text, whitespace or punctuation, but not a word character
// (user@example.com) and not part of a path or another mention.
func mentionAllowedBefore(r byte) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return false
	case r == '_', r == '@', r == '.', r == '/', r == '-':
		return false
	}
	return true
}

// mentionSpan is one "@name" occurrence inside a run of text.
type mentionSpan struct {
	start, end int    // byte offsets of "@name" within the text
	name       string // as written, without "@"
}

// findMentionSpans returns the candidate mentions in text. Trailing sentence
// punctuation is not part of the name ("@alice." mentions alice); a name that
// still ends in "." or "-" after the lookup fails is retried shorter by the
// caller via candidateNames.
func findMentionSpans(text string) []mentionSpan {
	var spans []mentionSpan
	for _, m := range mentionCandidate.FindAllStringSubmatchIndex(text, -1) {
		if m[0] > 0 && !mentionAllowedBefore(text[m[0]-1]) {
			continue
		}
		spans = append(spans, mentionSpan{start: m[0], end: m[1], name: text[m[2]:m[3]]})
	}
	return spans
}

// candidateNames lists the names a written mention may refer to, longest
// first: "@alice." may be the user "alice." (not allowed in Mattermost, but
// harmless to try) or "erin".
func candidateNames(name string) []string {
	out := []string{strings.ToLower(name)}
	trimmed := strings.TrimRight(name, ".-_")
	if trimmed != name && trimmed != "" {
		out = append(out, strings.ToLower(trimmed))
	}
	return out
}

// resolveUsernames maps lower-case usernames to Mattermost users, using the
// client's user cache first and one batched REST call for the rest. Lookup
// failures are logged and treated as "no such user": a mention then stays
// plain text, which is what the message looked like before this feature.
func (c *MattermostClient) resolveUsernames(ctx context.Context, names []string) map[string]*mattermost.User {
	found := make(map[string]*mattermost.User, len(names))
	mc := &c.mentions
	now := time.Now()

	mc.mu.Lock()
	if mc.byName == nil {
		mc.byName = make(map[string]*mattermost.User)
		mc.unknown = make(map[string]time.Time)
	}
	var missing []string
	for _, n := range names {
		if u, ok := mc.byName[n]; ok {
			found[n] = u
		} else if t, ok := mc.unknown[n]; ok && now.Sub(t) < unknownUsernameTTL {
			continue
		} else {
			missing = append(missing, n)
		}
	}
	mc.mu.Unlock()

	if len(missing) > 0 {
		// Users already known from channel membership need no request.
		c.mu.RLock()
		for _, u := range c.users {
			if u == nil {
				continue
			}
			ln := strings.ToLower(u.Username)
			for _, n := range missing {
				if n == ln {
					found[n] = u
				}
			}
		}
		c.mu.RUnlock()
		var ask []string
		for _, n := range missing {
			if found[n] == nil {
				ask = append(ask, n)
			}
		}
		var fetched []*mattermost.User
		var err error
		if len(ask) > 0 {
			fetched, err = mattermost.GetUsersByUsernames(c.serverURL(), c.Token, ask)
			if err != nil {
				zerolog.Ctx(ctx).Debug().Err(err).Int("count", len(ask)).Msg("Failed to look up mentioned usernames")
			}
		}
		for _, u := range fetched {
			if u != nil && u.Username != "" {
				found[strings.ToLower(u.Username)] = u
			}
		}
		mc.mu.Lock()
		for _, n := range missing {
			if u := found[n]; u != nil {
				mc.byName[n] = u
				delete(mc.unknown, n)
			} else if err == nil {
				// Only remember misses from a successful lookup; a failed
				// request is retried on the next message.
				mc.unknown[n] = now
			}
		}
		mc.mu.Unlock()
	}
	return found
}

// mentionTarget returns the Matrix user a Mattermost user is mentioned as: the
// logged-in Matrix user for their own account, otherwise the user's ghost.
func (c *MattermostClient) mentionTarget(u *mattermost.User) id.UserID {
	if u.ID == c.UserID && c.UserLogin != nil && c.UserLogin.UserLogin != nil {
		return c.UserLogin.UserMXID
	}
	if c.ghostMXIDFn != nil {
		return c.ghostMXIDFn(u.ID)
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.Matrix == nil {
		return ""
	}
	return c.UserLogin.Bridge.Matrix.GhostIntent(networkid.UserID(u.ID)).GetMXID()
}

// applyInboundMentions rewrites the "@username" mentions in already-rendered
// message content into Matrix pills and fills content.Mentions. It must run
// before anything quoting other posts is appended, so that mentions inside a
// quoted post do not notify anyone.
func (c *MattermostClient) applyInboundMentions(ctx context.Context, content *event.MessageEventContent) {
	htmlBody := content.FormattedBody
	if content.Format != event.FormatHTML || htmlBody == "" {
		// RenderMarkdown leaves FormattedBody empty when the HTML would equal
		// the plain text, so the plain text is valid HTML as it stands.
		htmlBody = html.EscapeString(content.Body)
		htmlBody = strings.ReplaceAll(htmlBody, "\n", "<br>")
	}

	// Collect every candidate name first, so the lookup is one request.
	segments := splitHTMLText(htmlBody)
	var names []string
	seen := map[string]bool{}
	for _, seg := range segments {
		if !seg.text {
			continue
		}
		for _, sp := range findMentionSpans(html.UnescapeString(seg.s)) {
			for _, n := range candidateNames(sp.name) {
				if !seen[n] && !roomMentionWords[n] {
					seen[n] = true
					names = append(names, n)
				}
			}
		}
	}
	if len(names) == 0 && !mentionsRoom(segments) {
		return
	}
	users := c.resolveUsernames(ctx, names)

	var out strings.Builder
	var userIDs []id.UserID
	room := false
	for _, seg := range segments {
		if !seg.text {
			out.WriteString(seg.s)
			continue
		}
		text := html.UnescapeString(seg.s)
		last := 0
		for _, sp := range findMentionSpans(text) {
			lname := strings.ToLower(sp.name)
			if roomMentionWords[lname] {
				room = true
				continue
			}
			var u *mattermost.User
			var used string
			for _, n := range candidateNames(sp.name) {
				if users[n] != nil {
					u, used = users[n], n
					break
				}
			}
			if u == nil {
				continue
			}
			mxid := c.mentionTarget(u)
			if mxid == "" {
				continue
			}
			end := sp.start + 1 + len(used) // "@" + the matched name; the rest stays text
			out.WriteString(html.EscapeString(text[last:sp.start]))
			out.WriteString(`<a href="` + html.EscapeString(mxid.URI().MatrixToURL()) + `">`)
			out.WriteString(html.EscapeString(c.displayName(u)))
			out.WriteString(`</a>`)
			last = end
			if !containsUserID(userIDs, mxid) {
				userIDs = append(userIDs, mxid)
			}
		}
		out.WriteString(html.EscapeString(text[last:]))
	}
	if len(userIDs) == 0 && !room {
		return
	}
	if len(userIDs) > 0 {
		rebuilt := format.HTMLToContent(out.String())
		content.Body = rebuilt.Body
		content.FormattedBody = out.String()
		content.Format = event.FormatHTML
	}
	content.Mentions = &event.Mentions{UserIDs: userIDs, Room: room}
}

func containsUserID(list []id.UserID, u id.UserID) bool {
	for _, x := range list {
		if x == u {
			return true
		}
	}
	return false
}

func mentionsRoom(segments []htmlSegment) bool {
	for _, seg := range segments {
		if !seg.text {
			continue
		}
		for _, sp := range findMentionSpans(html.UnescapeString(seg.s)) {
			if roomMentionWords[strings.ToLower(sp.name)] {
				return true
			}
		}
	}
	return false
}

// htmlSegment is a piece of an HTML string: either markup (tags, and any text
// inside code, pre or a elements, which is never rewritten) or plain text.
type htmlSegment struct {
	s    string
	text bool
}

var htmlTagPattern = regexp.MustCompile(`<(/?)([a-zA-Z][a-zA-Z0-9]*)[^>]*>`)

// splitHTMLText splits the simple HTML mautrix's markdown renderer produces
// into markup and rewritable text. Text nested in <code>, <pre> or <a> is
// returned as markup.
func splitHTMLText(s string) []htmlSegment {
	var segs []htmlSegment
	protected := 0
	last := 0
	for _, m := range htmlTagPattern.FindAllStringSubmatchIndex(s, -1) {
		if m[0] > last {
			segs = append(segs, htmlSegment{s: s[last:m[0]], text: protected == 0})
		}
		tag := strings.ToLower(s[m[4]:m[5]])
		closing := m[3] > m[2]
		if tag == "code" || tag == "pre" || tag == "a" {
			if closing {
				if protected > 0 {
					protected--
				}
			} else if !strings.HasSuffix(s[m[0]:m[1]], "/>") {
				protected++
			}
		}
		segs = append(segs, htmlSegment{s: s[m[0]:m[1]]})
		last = m[1]
	}
	if last < len(s) {
		segs = append(segs, htmlSegment{s: s[last:], text: protected == 0})
	}
	return segs
}

// roomMentionWord matches a literal "@room" in outgoing Markdown.
var roomMentionWord = regexp.MustCompile(`(^|[^\w@])@room\b`)

// outboundMarkdown converts a Matrix message's formatted body to the Markdown
// Mattermost receives, turning user pills into "@username".
func (c *MattermostClient) outboundMarkdown(ctx context.Context, content *event.MessageEventContent) string {
	body := content.Body
	if content.FormattedBody != "" {
		parser := *format.MarkdownHTMLParser
		parser.PillConverter = func(displayname, mxid, eventID string, fctx format.Context) string {
			if strings.HasPrefix(mxid, "@") {
				if name := c.usernameForMXID(ctx, id.UserID(mxid)); name != "" {
					return "@" + name
				}
			}
			return format.DefaultPillConverter(displayname, mxid, eventID, fctx)
		}
		body = parser.Parse(content.FormattedBody, format.NewContext(ctx))
	}
	body = c.replacePlainMentions(ctx, body, content.Mentions)
	if content.Mentions != nil && content.Mentions.Room {
		body = roomMentionWord.ReplaceAllString(body, "${1}@channel")
	}
	return body
}

// mmUserIDForMXID returns the Mattermost user ID behind a Matrix user: the
// logged-in user's own ID for their Matrix account, the ghost's ID for a
// ghost, and "" for anyone else.
func (c *MattermostClient) mmUserIDForMXID(mxid id.UserID) string {
	if c.UserLogin != nil && c.UserLogin.UserLogin != nil && mxid == c.UserLogin.UserMXID {
		return c.UserID
	}
	if c.parseGhostFn != nil {
		return c.parseGhostFn(mxid)
	}
	if c.UserLogin != nil && c.UserLogin.Bridge != nil && c.UserLogin.Bridge.Matrix != nil {
		if uid, ok := c.UserLogin.Bridge.Matrix.ParseGhostMXID(mxid); ok {
			return string(uid)
		}
	}
	return ""
}

// mmUser returns the Mattermost profile for a user ID, from the cache or over
// REST. It returns nil if the user cannot be fetched.
func (c *MattermostClient) mmUser(ctx context.Context, mmID string) *mattermost.User {
	c.mu.RLock()
	u := c.users[mmID]
	c.mu.RUnlock()
	if u != nil {
		return u
	}
	fetched, err := mattermost.GetUser(c.serverURL(), c.Token, mmID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("user_id", mmID).Msg("Failed to look up mentioned user")
		return nil
	}
	c.mu.Lock()
	if c.users == nil {
		c.users = make(map[string]*mattermost.User)
	}
	c.users[mmID] = fetched
	c.mu.Unlock()
	return fetched
}

// usernameForMXID returns the Mattermost username a Matrix user maps to: the
// logged-in user's own username, or the username behind a ghost. It returns ""
// for anyone else.
func (c *MattermostClient) usernameForMXID(ctx context.Context, mxid id.UserID) string {
	mmID := c.mmUserIDForMXID(mxid)
	if mmID == "" {
		return ""
	}
	if mmID == c.UserID && c.Username != "" {
		return c.Username
	}
	if u := c.mmUser(ctx, mmID); u != nil {
		return u.Username
	}
	return ""
}

// mentionNames lists the names a Matrix client may have written after "@" for
// a Mattermost user: the name the user's ghost has in Matrix (what Beeper's
// autocomplete inserts), their name under the active format, their full name
// and their nickname.
func (c *MattermostClient) mentionNames(ctx context.Context, mmID string) []string {
	var names []string
	add := func(n string) {
		n = strings.TrimSpace(n)
		if n == "" {
			return
		}
		for _, x := range names {
			if strings.EqualFold(x, n) {
				return
			}
		}
		names = append(names, n)
	}
	if c.ghostNameFn != nil {
		add(c.ghostNameFn(mmID))
	} else if c.UserLogin != nil && c.UserLogin.Bridge != nil && c.UserLogin.Bridge.DB != nil {
		if ghost, err := c.UserLogin.Bridge.GetExistingGhostByID(ctx, networkid.UserID(mmID)); err == nil && ghost != nil {
			add(ghost.Name)
		}
	}
	if u := c.mmUser(ctx, mmID); u != nil {
		// Every form the name may have had, so a ghost still carrying a name
		// from another display_names setting is matched too.
		add(c.displayName(u))
		add(u.FullName())
		add(u.Nickname)
	}
	// Longest first, so "@Frank Example" is replaced before a shorter name
	// could match its prefix.
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	return names
}

// replaceNameMention replaces each "@name" in body (case-insensitive) with
// "@username", in a single pass over the original text, so a replacement is
// never matched again. A match counts only at a word boundary: the "@" must
// not follow a letter, digit, "_" or "@" (an email address), and the name must
// not continue into a letter, digit or "_" ("@Alice Examples" is not "@Alice
// Example").
func replaceNameMention(body, name, username string) string {
	pattern := regexp.MustCompile(`(?i)@` + regexp.QuoteMeta(name))
	var out strings.Builder
	last := 0
	for _, m := range pattern.FindAllStringIndex(body, -1) {
		if m[0] > 0 {
			if r, _ := utf8.DecodeLastRuneInString(body[:m[0]]); isMentionWordRune(r) || r == '@' {
				continue
			}
		}
		if m[1] < len(body) {
			if r, _ := utf8.DecodeRuneInString(body[m[1]:]); isMentionWordRune(r) {
				continue
			}
		}
		out.WriteString(body[last:m[0]])
		out.WriteString("@" + username)
		last = m[1]
	}
	if last == 0 {
		return body
	}
	out.WriteString(body[last:])
	return out.String()
}

func isMentionWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// replacePlainMentions handles clients that send a mention as plain text
// ("@Frank Example") plus m.mentions instead of an HTML pill, as Beeper does:
// for each mentioned user, "@<name>" becomes "@username". Only users listed in
// m.mentions are touched, so ordinary text that happens to contain "@Name" is
// left alone.
func (c *MattermostClient) replacePlainMentions(ctx context.Context, body string, mentions *event.Mentions) string {
	if mentions == nil {
		return body
	}
	for _, mxid := range mentions.UserIDs {
		mmID := c.mmUserIDForMXID(mxid)
		if mmID == "" {
			continue
		}
		username := c.usernameForMXID(ctx, mxid)
		if username == "" {
			continue
		}
		for _, name := range c.mentionNames(ctx, mmID) {
			if strings.EqualFold(name, username) {
				continue // already "@username"
			}
			body = replaceNameMention(body, name, username)
		}
	}
	return body
}
