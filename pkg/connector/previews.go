package connector

import (
	"context"
	"html"
	"regexp"
	"strings"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Mattermost serves permalinks as a single-page app shell with no OpenGraph
// metadata, so no client can build a preview from them. The bridge can: it has
// an authenticated token. This file quotes the linked post under the link, the
// way Slack unfurls a message link. A quote is used rather than a
// com.beeper.linkpreviews card because clients clamp cards to a few lines with
// no way to expand them, and do not render the post's markdown.

const (
	// maxPreviewsPerMessage bounds the work done for one message.
	maxPreviewsPerMessage = 3
	// maxQuotedBytes is the budget, in bytes of post text, for all quotes in one
	// message. Matrix events are capped at 64 KiB and each quote is carried
	// twice (plain body and HTML), next to the message itself, which can be up
	// to 16,383 characters. 8 KiB holds any ordinary post in full; a longer one
	// is shortened and the quote's title links to the original.
	maxQuotedBytes = 8 * 1024
	// previewCacheTTL keeps repeated permalinks (and backfill) from re-fetching.
	previewCacheTTL = 10 * time.Minute
)

// permalinkPath matches /<team>/pl/<post id>. Mattermost post IDs are 26
// characters of lowercase letters and digits.
var permalinkPath = regexp.MustCompile(`/([A-Za-z0-9._-]+)/pl/([a-z0-9]{26})(?:[?#][^\s<>"']*)?`)

// permalink is one Mattermost permalink found in a message.
type permalink struct {
	URL    string // the link as written, used to match the preview to the text
	PostID string
}

// findPermalinks lists the permalinks in text that point at the bridge's own
// Mattermost server. Links to other hosts are left alone: the bridge cannot
// authenticate against them and should not rewrite what it does not own.
func findPermalinks(text, serverURL string) []permalink {
	host := serverHost(serverURL)
	if host == "" || text == "" {
		return nil
	}
	var out []permalink
	seen := make(map[string]bool)
	for _, field := range strings.Fields(text) {
		trimmed := strings.Trim(field, "<>()[]{}.,;:!?\"'")
		lower := strings.ToLower(trimmed)
		for _, scheme := range []string{"https://", "http://"} {
			if !strings.HasPrefix(lower, scheme) {
				continue
			}
			rest := trimmed[len(scheme):]
			slash := strings.IndexByte(rest, '/')
			if slash < 0 {
				break
			}
			linkHost := rest[:slash]
			// Drop a port and any userinfo so host comparison is on the name.
			if at := strings.LastIndexByte(linkHost, '@'); at >= 0 {
				linkHost = linkHost[at+1:]
			}
			if h, _, ok := strings.Cut(linkHost, ":"); ok {
				linkHost = h
			}
			if !strings.EqualFold(linkHost, host) {
				break
			}
			m := permalinkPath.FindStringSubmatch(rest[slash:])
			if m == nil || seen[m[2]] {
				break
			}
			seen[m[2]] = true
			out = append(out, permalink{URL: trimmed, PostID: m[2]})
			break
		}
		if len(out) >= maxPreviewsPerMessage {
			break
		}
	}
	return out
}

// serverHost extracts the host name from the configured server URL.
func serverHost(serverURL string) string {
	s := strings.TrimSpace(serverURL)
	lower := strings.ToLower(s)
	for _, scheme := range []string{"https://", "http://"} {
		if strings.HasPrefix(lower, scheme) {
			s = s[len(scheme):]
			break
		}
	}
	if slash := strings.IndexByte(s, '/'); slash >= 0 {
		s = s[:slash]
	}
	if at := strings.LastIndexByte(s, '@'); at >= 0 {
		s = s[at+1:]
	}
	if h, _, ok := strings.Cut(s, ":"); ok {
		s = h
	}
	return strings.ToLower(s)
}

// quotedPost is what the bridge shows for one linked post.
type quotedPost struct {
	Title string // "author in channel"
	Text  string // the post's markdown, normalised by normalizePostText
}

type cachedPreview struct {
	quote   *quotedPost // nil caches a failure
	expires time.Time
}

func (c *MattermostClient) cachedQuote(postID string) (*quotedPost, bool) {
	c.previewMu.Lock()
	defer c.previewMu.Unlock()
	e, ok := c.previews[postID]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.quote, true
}

func (c *MattermostClient) storeQuote(postID string, q *quotedPost) {
	c.previewMu.Lock()
	defer c.previewMu.Unlock()
	if c.previews == nil {
		c.previews = make(map[string]cachedPreview)
	}
	c.previews[postID] = cachedPreview{quote: q, expires: time.Now().Add(previewCacheTTL)}
}

// resolvePermalink fetches the linked post. A post the bridge cannot read
// yields nothing rather than an error: a message must never fail to send
// because one of its links could not be resolved.
func (c *MattermostClient) resolvePermalink(postID string) *quotedPost {
	if q, ok := c.cachedQuote(postID); ok {
		return q
	}
	if c.Token == "" {
		return nil
	}
	post, err := mattermost.GetPost(c.serverURL(), c.Token, postID)
	if err != nil || post == nil || post.DeleteAt != 0 {
		c.storeQuote(postID, nil) // remember the failure, briefly
		return nil
	}
	q := &quotedPost{Text: normalizePostText(post.Message)}
	var author, channel string
	if post.UserID != "" {
		if u, err := mattermost.GetUser(c.serverURL(), c.Token, post.UserID); err == nil && u != nil {
			author = u.DisplayName()
		}
	}
	if post.ChannelID != "" {
		if ch, err := mattermost.GetChannel(c.serverURL(), c.Token, post.ChannelID); err == nil && ch != nil {
			channel = ch.DisplayName
			if channel == "" {
				channel = ch.Name
			}
		}
	}
	switch {
	case author != "" && channel != "":
		q.Title = author + " in " + channel
	case author != "":
		q.Title = author
	case channel != "":
		q.Title = channel
	}
	if q.Title == "" && q.Text == "" {
		c.storeQuote(postID, nil)
		return nil
	}
	c.storeQuote(postID, q)
	return q
}

// normalizePostText keeps the post as written: line breaks stay, runs of blank
// lines collapse to one, trailing spaces go.
func normalizePostText(message string) string {
	lines := strings.Split(strings.ReplaceAll(message, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	blank := false
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		if line == "" {
			if blank || len(out) == 0 {
				continue
			}
			blank = true
		} else {
			blank = false
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// truncateUTF8 shortens text to at most limit bytes on a character boundary,
// marking the cut with an ellipsis.
func truncateUTF8(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	if limit <= 0 {
		return "…"
	}
	cut := limit
	for cut > 0 && !utf8Start(text[cut]) {
		cut--
	}
	return strings.TrimRight(text[:cut], " \n") + "…"
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

// attachPermalinkQuotes quotes each linked Mattermost post under the message,
// in both the plain body and the HTML body. It is best effort: if nothing
// resolves, the message is sent exactly as it would have been without this.
func (c *MattermostClient) attachPermalinkQuotes(ctx context.Context, content *event.MessageEventContent, text string) {
	if content == nil || text == "" {
		return
	}
	budget := maxQuotedBytes
	var plain, rich strings.Builder
	for _, link := range findPermalinks(text, c.serverURL()) {
		if ctx.Err() != nil {
			return
		}
		q := c.resolvePermalink(link.PostID)
		if q == nil {
			continue
		}
		body := truncateUTF8(q.Text, budget)
		budget -= len(body)

		title := q.Title
		if title == "" {
			title = "Linked message"
		}
		plain.WriteString("\n\n> " + title)
		if body != "" {
			for _, line := range strings.Split(body, "\n") {
				plain.WriteString("\n> " + line)
			}
		}

		rich.WriteString(`<blockquote><p><a href="` + html.EscapeString(link.URL) + `"><strong>` +
			html.EscapeString(title) + `</strong></a></p>`)
		if body != "" {
			rich.WriteString(renderQuotedMarkdown(body))
		}
		rich.WriteString(`</blockquote>`)
	}
	if plain.Len() == 0 {
		return
	}
	if content.Format != event.FormatHTML || content.FormattedBody == "" {
		content.FormattedBody = plainToHTML(content.Body)
		content.Format = event.FormatHTML
	}
	content.Body += plain.String()
	content.FormattedBody += rich.String()
}

// renderQuotedMarkdown renders a post's markdown the same way the bridge
// renders messages, falling back to escaped text.
func renderQuotedMarkdown(text string) string {
	r := format.RenderMarkdown(replaceEmojiShortcodes(text), true, false)
	if r.FormattedBody != "" {
		return r.FormattedBody
	}
	return "<p>" + plainToHTML(text) + "</p>"
}

func plainToHTML(text string) string {
	return strings.ReplaceAll(html.EscapeString(text), "\n", "<br>")
}
