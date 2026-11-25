package mattermost

import (
	"fmt"
	"reflect"
	"testing"
)

func TestDecodeChannelLifecycle(t *testing.T) {
	for event, want := range map[string]any{
		"channel_deleted":  &ChannelDeletedEvent{ChannelID: "channel"},
		"channel_restored": &ChannelRestoredEvent{ChannelID: "channel"},
	} {
		t.Run(event, func(t *testing.T) {
			c := NewClient("", "")
			c.parseEvent([]byte(fmt.Sprintf(`{"event":%q,"data":{"channel_id":"channel"},"broadcast":{"channel_id":"other"}}`, event)))
			select {
			case actual := <-c.Events:
				if !reflect.DeepEqual(actual, want) {
					t.Fatalf("unexpected event: %T %+v", actual, actual)
				}
			default:
				t.Fatal("no lifecycle event decoded")
			}
			c.parseEvent([]byte(fmt.Sprintf(`{"event":%q,"data":{},"broadcast":{"channel_id":"other"}}`, event)))
			if len(c.Events) != 0 {
				t.Fatal("event without data.channel_id was accepted")
			}
		})
	}
}
