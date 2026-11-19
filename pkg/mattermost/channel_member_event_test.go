package mattermost

import "testing"

func TestChannelMemberEventIsDecoded(t *testing.T) {
	cases := []struct {
		name, payload, channel, user string
		added                        bool
	}{
		{"added, sent to the channel", `{"event":"user_added","data":{"user_id":"u1","team_id":"t"},"broadcast":{"channel_id":"c1"}}`, "c1", "u1", true},
		{"removed, sent to the channel", `{"event":"user_removed","data":{"user_id":"u1","remover_id":"r"},"broadcast":{"channel_id":"c1"}}`, "c1", "u1", false},
		{"removed, sent to the removed user", `{"event":"user_removed","data":{"channel_id":"c1","remover_id":"r"},"broadcast":{"user_id":"u1"}}`, "c1", "u1", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("", "")
			c.parseEvent([]byte(tc.payload))
			select {
			case raw := <-c.Events:
				ev, ok := raw.(*ChannelMemberEvent)
				if !ok {
					t.Fatalf("unexpected event %T", raw)
				}
				if ev.ChannelID != tc.channel || ev.UserID != tc.user || ev.Added != tc.added {
					t.Fatalf("event = %+v", ev)
				}
			default:
				t.Fatal("member event was ignored")
			}
		})
	}
}

func TestChannelMemberEventWithoutIDsIsIgnored(t *testing.T) {
	c := NewClient("", "")
	c.parseEvent([]byte(`{"event":"user_added","data":{},"broadcast":{"channel_id":"c1"}}`))
	select {
	case ev := <-c.Events:
		t.Fatalf("unexpected event: %#v", ev)
	default:
	}
}
