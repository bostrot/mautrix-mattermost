package mattermost

import "testing"

func TestChannelDeletedWirePayload(t *testing.T) {
	tests := []struct{ name, wire string }{
		{"public", `{"event":"channel_deleted","data":{"channel_id":"chan123","delete_at":123},"broadcast":{"team_id":"team123","channel_id":""}}`},
		{"private", `{"event":"channel_deleted","data":{"channel_id":"chan123","delete_at":123},"broadcast":{"team_id":"","channel_id":"chan123"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient("", "")
			c.parseEvent([]byte(tt.wire))
			select {
			case raw := <-c.Events:
				evt, ok := raw.(*ChannelDeletedEvent)
				if !ok {
					t.Fatalf("unexpected event %T", raw)
				}
				if evt.ChannelID != "chan123" {
					t.Errorf("channel id = %q, want chan123", evt.ChannelID)
				}
				if tt.name == "public" && evt.TeamID != "team123" {
					t.Errorf("team id = %q, want team123", evt.TeamID)
				}
			default:
				t.Fatal("no deletion event")
			}
		})
	}
}

func TestChannelDeletedMissingIDDoesNotQueueDeletion(t *testing.T) {
	c := NewClient("", "")
	c.parseEvent([]byte(`{"event":"channel_deleted","data":{},"broadcast":{"channel_id":"untrusted"}}`))
	select {
	case evt := <-c.Events:
		t.Fatalf("unexpected deletion event: %#v", evt)
	default:
	}
}
