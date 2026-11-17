package connector

import (
	"context"
	"testing"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/event"
)

func TestLifecycleSystemPostPreservesExactText(t *testing.T) {
	for _, typ := range []string{"system_channel_deleted", "system_channel_restored"} {
		t.Run(typ, func(t *testing.T) {
			text := "@alice archived the channel."
			c := &MattermostClient{}
			got, err := c.convertPost(context.Background(), nil, "", &mattermost.Post{Type: typ, Message: text})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Parts) != 1 {
				t.Fatalf("parts=%d", len(got.Parts))
			}
			content := got.Parts[0].Content
			if content.Body != text {
				t.Errorf("body=%q want=%q", content.Body, text)
			}
			if content.MsgType != event.MsgNotice {
				t.Errorf("type=%q want=%q", content.MsgType, event.MsgNotice)
			}
		})
	}
}
