package connector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"maunium.net/go/mautrix/format"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Payloads as Mattermost sends them (from a 12.1 server), names neutralised.
const (
	meetPostJSON = `{"id":"p1","message":"I have started a meeting: **Town Square**","type":"custom_google_meet","props":{"attachments":[{"id":0,"ts":null,"text":"Meeting URL: https://meet.google.com/abc-defg-hij","color":"","title":"Town Square","fields":null,"footer":"","pretext":"","fallback":"Meeting started.\n\nMeeting URL: https://meet.google.com/abc-defg-hij","image_url":"","thumb_url":"","title_link":"","author_icon":"","author_link":"","author_name":"","footer_icon":""}],"from_plugin":"true","meeting_link":"https://meet.google.com/abc-defg-hij","meeting_topic":"Town Square"}}`
	gcalPostJSON = `{"id":"p2","message":"","type":"slack_attachment","props":{"from_bot":"true","attachments":[{"id":0,"ts":null,"text":"Great, you will receive a message before your meetings.","color":"","title":"Receive reminder","fields":null,"footer":"","pretext":"","fallback":"Receive reminder: Great, you will receive a message before your meetings.","image_url":"","thumb_url":"","title_link":"","author_icon":"","author_link":"","author_name":"","footer_icon":""}],"from_plugin":"true"}}`
	callPostJSON = `{"id":"p3","message":"alice started a call","type":"custom_calls","props":{"title":"","start_at":1794953590241,"attachments":[{"id":0,"ts":null,"text":"alice started a call","title":"alice started a call","fallback":"alice started a call"}],"call_status":"calling","from_plugin":"true"}}`
)

func decodeTestPost(t *testing.T, raw string) *mattermost.Post {
	t.Helper()
	var p mattermost.Post
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatal(err)
	}
	return &p
}

func TestGoogleMeetLinkReachesMatrix(t *testing.T) {
	post := decodeTestPost(t, meetPostJSON)
	content := format.RenderMarkdown(postMarkdown(post), true, false)
	const link = `<a href="https://meet.google.com/abc-defg-hij">https://meet.google.com/abc-defg-hij</a>`
	if !strings.Contains(content.FormattedBody, link) {
		t.Fatalf("meeting link is not a link:\n%s", content.FormattedBody)
	}
	if !strings.Contains(content.Body, "https://meet.google.com/abc-defg-hij") {
		t.Fatalf("plain body lacks the link:\n%s", content.Body)
	}
	for _, want := range []string{"I have started a meeting: <strong>Town Square</strong>", "<blockquote>", "<strong>Town Square</strong>"} {
		if !strings.Contains(content.FormattedBody, want) {
			t.Errorf("missing %q in:\n%s", want, content.FormattedBody)
		}
	}
}

func TestAttachmentOnlyPostIsNotEmpty(t *testing.T) {
	post := decodeTestPost(t, gcalPostJSON)
	md := postMarkdown(post)
	want := "> **Receive reminder**\n>\n> Great, you will receive a message before your meetings."
	if md != want {
		t.Fatalf("markdown =\n%q\nwant\n%q", md, want)
	}
}

func TestAttachmentRendering(t *testing.T) {
	cases := []struct {
		name string
		a    mattermost.MessageAttachment
		want string
	}{
		{"title link", mattermost.MessageAttachment{Title: "PR #4", TitleLink: "https://example.com/pr/4"}, "> **[PR #4](https://example.com/pr/4)**"},
		{"non-http title link is not a link", mattermost.MessageAttachment{Title: "T", TitleLink: "javascript:alert(1)"}, "> **T**"},
		{"author", mattermost.MessageAttachment{AuthorName: "bot", AuthorLink: "https://example.com/bot"}, "> _[bot](https://example.com/bot)_"},
		{"pretext outside the quote", mattermost.MessageAttachment{Pretext: "New event", Text: "Standup"}, "New event\n\n> Standup"},
		{"fields", mattermost.MessageAttachment{Fields: []mattermost.MessageAttachmentField{{Title: "When", Value: "10:00"}, {Title: "Count", Value: 3.0}, {Value: "untitled"}, {Title: "Empty"}}}, "> **When:** 10:00\n>\n> **Count:** 3\n>\n> untitled"},
		{"buttons listed", mattermost.MessageAttachment{Text: "Join?", Actions: []mattermost.MessageAttachmentAction{{Name: "Yes"}, {Name: "No"}}}, "> Join?\n>\n> _Buttons in Mattermost: Yes · No_"},
		{"image linked", mattermost.MessageAttachment{ImageURL: "https://example.com/i.png"}, "> [Image](https://example.com/i.png)"},
		{"footer", mattermost.MessageAttachment{Text: "x", Footer: "via webhook"}, "> x\n>\n> _via webhook_"},
		{"multi-line text stays quoted", mattermost.MessageAttachment{Text: "a\n\nb"}, "> a\n>\n> b"},
		{"fallback only", mattermost.MessageAttachment{Fallback: "Summary https://example.com"}, "> Summary <https://example.com>"},
		{"empty", mattermost.MessageAttachment{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attachmentMarkdown(tc.a); got != tc.want {
				t.Fatalf("got\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

func TestLinkifyURLs(t *testing.T) {
	cases := map[string]string{
		"Meeting URL: https://meet.google.com/abc":   "Meeting URL: <https://meet.google.com/abc>",
		"see https://example.com/a.":                 "see <https://example.com/a>.",
		"(https://example.com/a)":                    "(https://example.com/a)",
		"[x](https://example.com/a)":                 "[x](https://example.com/a)",
		"<https://example.com/a>":                    "<https://example.com/a>",
		"two https://a.example and http://b.example": "two <https://a.example> and <http://b.example>",
		"no links here":                              "no links here",
		"https://example.com/q?x=1&y=2":              "<https://example.com/q?x=1&y=2>",
	}
	for in, want := range cases {
		if got := linkifyURLs(in); got != want {
			t.Errorf("linkifyURLs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPostWithoutAttachmentsUnchanged(t *testing.T) {
	for _, p := range []*mattermost.Post{
		{Message: "hello https://example.com"},
		{Message: "hi", Props: map[string]any{"disable_group_highlight": true}},
		{Message: "x", Props: map[string]any{"attachments": "not a list"}},
	} {
		if got := postMarkdown(p); got != p.Message {
			t.Errorf("postMarkdown(%q) = %q", p.Message, got)
		}
	}
	if postMarkdown(&mattermost.Post{}) != "" {
		t.Error("empty post produced text")
	}
}

// The call plugin repeats the message in its attachment; showing it twice
// would be noise.
func TestCallPostIsNotDuplicated(t *testing.T) {
	post := decodeTestPost(t, callPostJSON)
	if got := postMarkdown(post); got != "alice started a call" {
		t.Fatalf("got %q", got)
	}
}

func TestFallbackOnlyAttachmentUnderMessageIsKept(t *testing.T) {
	post := &mattermost.Post{Message: "Deploy", Props: map[string]any{"attachments": []any{map[string]any{"fallback": "Build 12 passed"}}}}
	if got := postMarkdown(post); got != "Deploy\n\n> Build 12 passed" {
		t.Fatalf("got %q", got)
	}
}

func TestNewPostCarriesAttachments(t *testing.T) {
	c := &MattermostClient{}
	for _, raw := range []string{meetPostJSON, gcalPostJSON} {
		post := decodeTestPost(t, raw)
		cm, err := c.convertPost(context.Background(), nil, "", post)
		if err != nil || len(cm.Parts) != 1 {
			t.Fatalf("convert: %v %+v", err, cm)
		}
		body := cm.Parts[0].Content.Body
		if body == "" || body != format.RenderMarkdown(replaceEmojiShortcodes(postMarkdown(post)), true, false).Body {
			t.Fatalf("new post body = %q", body)
		}
	}
}

func TestOddlyTypedAttachmentKeepsWhatDecodes(t *testing.T) {
	post := &mattermost.Post{Message: "hi", Props: map[string]any{"attachments": []any{map[string]any{"title": 5, "text": "readable"}}}}
	if got := postMarkdown(post); got != "hi\n\n> readable" {
		t.Fatalf("got %q", got)
	}
	for _, bad := range []any{"not a list", 7, map[string]any{"title": "x"}} {
		p := &mattermost.Post{Message: "hi", Props: map[string]any{"attachments": bad}}
		if got := postMarkdown(p); got != "hi" {
			t.Errorf("attachments=%v: got %q", bad, got)
		}
	}
}
