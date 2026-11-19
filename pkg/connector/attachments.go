package connector

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Integrations (Google Meet, the calendar and GitHub plugins, incoming
// webhooks) post their content as message attachments in props.attachments,
// often with no message text at all. postMarkdown renders them as Markdown
// after the message, so they reach Matrix instead of an empty message:
//
//	<message>
//
//	<pretext>
//	> **[Title](title_link)**
//	> text
//	> **Field:** value
//	> footer
//
// Interactive buttons cannot work outside Mattermost; their names are listed
// so the reader knows to answer there. Images are linked, not uploaded.

// postMarkdown is a post's full Markdown: its message plus its attachments.
func postMarkdown(post *mattermost.Post) string {
	blocks := make([]string, 0, 4)
	if strings.TrimSpace(post.Message) != "" {
		blocks = append(blocks, post.Message)
	}
	for _, a := range post.Attachments() {
		if restatesMessage(a, post.Message) {
			continue
		}
		if md := attachmentMarkdown(a); md != "" {
			blocks = append(blocks, md)
		}
	}
	return strings.Join(blocks, "\n\n")
}

// restatesMessage reports whether an attachment only repeats the message text,
// as the Calls plugin's "alice started a call" card does.
func restatesMessage(a mattermost.MessageAttachment, message string) bool {
	msg := strings.TrimSpace(message)
	if msg == "" || a.Pretext != "" || a.AuthorName != "" || len(a.Fields) > 0 || len(a.Actions) > 0 || a.ImageURL != "" || a.Footer != "" || a.TitleLink != "" {
		return false
	}
	for _, s := range []string{a.Title, a.Text} {
		if s = strings.TrimSpace(s); s != "" && s != msg {
			return false
		}
	}
	return a.Title != "" || a.Text != ""
}

func attachmentMarkdown(a mattermost.MessageAttachment) string {
	var quoted []string
	if a.AuthorName != "" {
		quoted = append(quoted, "_"+mdLink(a.AuthorName, a.AuthorLink)+"_")
	}
	if a.Title != "" {
		quoted = append(quoted, "**"+mdLink(a.Title, a.TitleLink)+"**")
	}
	if a.Text != "" {
		quoted = append(quoted, linkifyURLs(a.Text))
	}
	for _, f := range a.Fields {
		value := strings.TrimSpace(fmt.Sprint(valueOrEmpty(f.Value)))
		switch {
		case f.Title != "" && value != "":
			quoted = append(quoted, "**"+f.Title+":** "+linkifyURLs(value))
		case value != "":
			quoted = append(quoted, linkifyURLs(value))
		}
	}
	if a.ImageURL != "" {
		quoted = append(quoted, mdLink("Image", a.ImageURL))
	}
	var buttons []string
	for _, act := range a.Actions {
		if act.Name != "" {
			buttons = append(buttons, act.Name)
		}
	}
	if len(buttons) > 0 {
		quoted = append(quoted, "_Buttons in Mattermost: "+strings.Join(buttons, " · ")+"_")
	}
	if a.Footer != "" {
		quoted = append(quoted, "_"+a.Footer+"_")
	}
	if len(quoted) == 0 && a.Pretext == "" {
		// Nothing structured: fall back to the plain-text summary.
		if a.Fallback == "" {
			return ""
		}
		quoted = append(quoted, linkifyURLs(a.Fallback))
	}
	var out []string
	if a.Pretext != "" {
		out = append(out, linkifyURLs(a.Pretext))
	}
	if len(quoted) > 0 {
		out = append(out, blockquote(strings.Join(quoted, "\n\n")))
	}
	return strings.Join(out, "\n\n")
}

func valueOrEmpty(v any) any {
	if v == nil {
		return ""
	}
	return v
}

// mdLink is a Markdown link when url is http(s), else the plain text.
func mdLink(text, url string) string {
	if url == "" || !(strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://")) {
		return text
	}
	return "[" + text + "](" + url + ")"
}

// blockquote prefixes every line with "> ", including blank ones, so the
// quote is not split into several.
func blockquote(s string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l == "" {
			lines[i] = ">"
		} else {
			lines[i] = "> " + l
		}
	}
	return strings.Join(lines, "\n")
}

// bareURL matches an http(s) URL that is not already part of Markdown link or
// autolink syntax (preceded by "(", "<" or "]").
var bareURL = regexp.MustCompile(`(^|[^(<\]\w/])(https?://[^\s<>()\[\]]+)`)

// linkifyURLs turns bare URLs into Markdown autolinks. Mattermost links bare
// URLs itself; the Markdown renderer used for Matrix does not, so without
// this a meeting link would arrive as plain text. Trailing sentence
// punctuation stays outside the link.
func linkifyURLs(s string) string {
	if !strings.Contains(s, "://") {
		return s
	}
	return bareURL.ReplaceAllStringFunc(s, func(m string) string {
		sub := bareURL.FindStringSubmatch(m)
		prefix, url := sub[1], sub[2]
		trail := ""
		for len(url) > 0 && strings.ContainsRune(".,;:!?'\"", rune(url[len(url)-1])) {
			trail = url[len(url)-1:] + trail
			url = url[:len(url)-1]
		}
		return prefix + "<" + url + ">" + trail
	})
}

// renderPost renders a post's full Markdown, attachments included, with
// mention pills. Only the post's own message may ping anyone: an @channel or
// @user inside an integration's attachment is shown but does not notify.
func (c *MattermostClient) renderPost(ctx context.Context, post *mattermost.Post) event.MessageEventContent {
	text := postMarkdown(post)
	content := c.renderPostText(ctx, text)
	if text != post.Message {
		mentions := c.renderPostText(ctx, post.Message).Mentions
		if mentions == nil {
			mentions = &event.Mentions{}
		}
		content.Mentions = mentions
	}
	return content
}
