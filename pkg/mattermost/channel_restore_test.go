package mattermost

import "testing"

func TestRestoreEventIsDecoded(t *testing.T) {
	cases := []struct{ name, payload, team string }{
		{"public", `{"event":"channel_restored","data":{"channel_id":"channel-id"},"broadcast":{"team_id":"team-id","channel_id":""}}`, "team-id"},
		{"private", `{"event":"channel_restored","data":{"channel_id":"channel-id"},"broadcast":{"team_id":"","channel_id":"channel-id"}}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("", "")
			c.parseEvent([]byte(tc.payload))
			select {
			case raw := <-c.Events:
				ev, ok := raw.(*ChannelRestoredEvent)
				if !ok {
					t.Fatalf("unexpected event %T", raw)
				}
				if ev.ChannelID != "channel-id" || ev.TeamID != tc.team {
					t.Fatalf("event=%+v", ev)
				}
			default:
				t.Fatal("restore event was ignored")
			}
		})
	}
}

func TestRestoreEventMissingIDIsIgnored(t *testing.T) {
	c := NewClient("", "")
	c.parseEvent([]byte(`{"event":"channel_restored","data":{},"broadcast":{"channel_id":"wrong"}}`))
	select {
	case ev := <-c.Events:
		t.Fatalf("unexpected event: %#v", ev)
	default:
	}
}
