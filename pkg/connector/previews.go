package connector

import (
	"context"
	"regexp"
	"strings"
	"time"

	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Mattermost serves permalinks as a single-page app shell with no OpenGraph
// metadata, so no client can build a preview from them. The bridge can: it has
// an authenticated token, and Matrix messages support explicit previews through
// com.beeper.linkpreviews. This file turns a pasted permalink into such a card.

const (
	// maxPreviewsPerMessage bounds the work done for one message.
	maxPreviewsPerMessage = 3
	// maxPreviewDescription is a display limit, not a Mattermost limit.
	maxPreviewDescription = 200
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

type cachedPreview struct {
	preview *event.BeeperLinkPreview
	expires time.Time
}

// previewCacheEntry returns a cached preview, or nil. Uses the client mutex that
// already guards the connector's short-lived maps.
func (c *MattermostClient) cachedPreview(postID string) *event.BeeperLinkPreview {
	c.previewMu.Lock()
	defer c.previewMu.Unlock()
	e, ok := c.previews[postID]
	if !ok || time.Now().After(e.expires) {
		return nil
	}
	return e.preview
}

func (c *MattermostClient) storePreview(postID string, p *event.BeeperLinkPreview) {
	c.previewMu.Lock()
	defer c.previewMu.Unlock()
	if c.previews == nil {
		c.previews = make(map[string]cachedPreview)
	}
	c.previews[postID] = cachedPreview{preview: p, expires: time.Now().Add(previewCacheTTL)}
}

// resolvePermalink fetches what is needed for one card. A post the bridge cannot
// read yields nothing rather than an error: a message must never fail to send
// because one of its links could not be previewed.
func (c *MattermostClient) resolvePermalink(postID string) *event.BeeperLinkPreview {
	if p := c.cachedPreview(postID); p != nil {
		return p
	}
	if c.Token == "" {
		return nil
	}
	post, err := mattermost.GetPost(c.serverURL(), c.Token, postID)
	if err != nil || post == nil || post.DeleteAt != 0 {
		c.storePreview(postID, nil) // remember the failure, briefly
		return nil
	}
	preview := &event.BeeperLinkPreview{
		LinkPreview: event.LinkPreview{
			Type:        "article",
			SiteName:    "Mattermost",
			Description: previewExcerpt(post.Message),
		},
	}
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
		preview.Title = author + " in " + channel
	case author != "":
		preview.Title = author
	case channel != "":
		preview.Title = channel
	}
	if preview.Title == "" && preview.Description == "" {
		c.storePreview(postID, nil)
		return nil
	}
	c.storePreview(postID, preview)
	return preview
}

// previewExcerpt is the opening of a post, on one line.
func previewExcerpt(message string) string {
	flat := strings.Join(strings.Fields(message), " ")
	if len(flat) <= maxPreviewDescription {
		return flat
	}
	// Do not cut a UTF-8 sequence in half.
	cut := maxPreviewDescription
	for cut > 0 && !utf8Start(flat[cut]) {
		cut--
	}
	return strings.TrimRight(flat[:cut], " ") + "…"
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

// attachPermalinkPreviews adds a preview for each Mattermost permalink in text.
// It is best effort: on any error the message is sent exactly as it would have
// been without this.
func (c *MattermostClient) attachPermalinkPreviews(ctx context.Context, content *event.MessageEventContent, text string) {
	if content == nil || text == "" {
		return
	}
	for _, link := range findPermalinks(text, c.serverURL()) {
		if err := ctx.Err(); err != nil {
			return
		}
		resolved := c.resolvePermalink(link.PostID)
		if resolved == nil {
			continue
		}
		preview := *resolved // copy: MatchedURL is per-link
		preview.MatchedURL = link.URL
		preview.CanonicalURL = link.URL
		content.BeeperLinkPreviews = append(content.BeeperLinkPreviews, &preview)
	}
}
