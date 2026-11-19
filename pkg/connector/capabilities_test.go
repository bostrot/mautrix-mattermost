package connector

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"
)

// TestCapabilitiesDeclareFormatting pins that rooms advertise mention pills;
// without user_link, Beeper sends mentions as plain text.
func TestCapabilitiesDeclareFormatting(t *testing.T) {
	caps := (&MattermostClient{}).GetCapabilities(context.Background(), nil)
	for _, f := range []event.FormattingFeature{event.FmtUserLink, event.FmtBold, event.FmtInlineLink, event.FmtCodeBlock, event.FmtAtRoomMention} {
		if caps.Formatting[f] != event.CapLevelFullySupported {
			t.Errorf("%s = %v, want fully supported", f, caps.Formatting[f])
		}
	}
	for _, f := range []event.FormattingFeature{event.FmtUnderline, event.FmtSpoiler, event.FmtTable, event.FmtTextForegroundColor} {
		if caps.Formatting[f] > event.CapLevelUnsupported {
			t.Errorf("%s is declared supported but has no Mattermost Markdown form", f)
		}
	}
	if caps.MaxTextLength != mattermostMaxPostRunes {
		t.Errorf("MaxTextLength = %d", caps.MaxTextLength)
	}
}

// TestDeclaredFormattingRoundTrips checks that each declared feature becomes
// Mattermost Markdown, not raw HTML or lost text.
func TestDeclaredFormattingRoundTrips(t *testing.T) {
	cases := map[event.FormattingFeature]struct{ html, want string }{
		event.FmtBold:               {"<strong>b</strong>", "**b**"},
		event.FmtItalic:             {"<em>i</em>", "_i_"},
		event.FmtStrikethrough:      {"<del>s</del>", "~~s~~"},
		event.FmtInlineCode:         {"<code>c</code>", "`c`"},
		event.FmtCodeBlock:          {"<pre><code>x\n</code></pre>", "```\nx\n```"},
		event.FmtSyntaxHighlighting: {"<pre><code class=\"language-go\">x\n</code></pre>", "```go\nx\n```"},
		event.FmtBlockquote:         {"<blockquote>q</blockquote>", "> q"},
		event.FmtInlineLink:         {`<a href="https://e.com">l</a>`, "[l](https://e.com)"},
		event.FmtUnorderedList:      {"<ul><li>a</li><li>b</li></ul>", "* a\n* b"},
		event.FmtOrderedList:        {"<ol><li>a</li></ol>", "1. a"},
		event.FmtListStart:          {`<ol start="3"><li>a</li></ol>`, "3. a"},
		event.FmtHeaders:            {"<h2>H</h2>", "## H"},
		event.FmtHorizontalLine:     {"<hr>", "---"},
	}
	c := &MattermostClient{}
	for f := range mattermostFormatting {
		if f == event.FmtUserLink || f == event.FmtAtRoomMention {
			continue // covered by the mention tests
		}
		tc, ok := cases[f]
		if !ok {
			t.Errorf("declared feature %s has no round-trip case", f)
			continue
		}
		content := event.MessageEventContent{Body: "x", Format: event.FormatHTML, FormattedBody: tc.html}
		if got := c.outboundMarkdown(context.Background(), &content); got != tc.want {
			t.Errorf("%s: %q -> %q, want %q", f, tc.html, got, tc.want)
		}
		if strings.Contains(c.outboundMarkdown(context.Background(), &content), "<") {
			t.Errorf("%s leaves HTML in the Markdown", f)
		}
	}
}
