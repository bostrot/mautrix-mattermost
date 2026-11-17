package mattermost

import "testing"

func TestChannelsViewedDecoding(t *testing.T) {
	c := &Client{Events: make(chan interface{}, 4)}
	c.parseEvent([]byte(`{"event":"multiple_channels_viewed","data":{"channel_times":{"a":1794950000000,"b":1794950001000,"":5,"bad":"x"}},"broadcast":{"user_id":"me"}}`))
	c.parseEvent([]byte(`{"event":"channel_viewed","data":{"channel_id":"c"}}`))
	c.parseEvent([]byte(`{"event":"multiple_channels_viewed","data":{}}`))
	if len(c.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(c.Events))
	}
	e1 := (<-c.Events).(*ChannelsViewedEvent)
	if len(e1.Times) != 2 || e1.Times["a"] != 1794950000000 || e1.Times["b"] != 1794950001000 {
		t.Fatalf("times = %v", e1.Times)
	}
	e2 := (<-c.Events).(*ChannelsViewedEvent)
	if v, ok := e2.Times["c"]; !ok || v != 0 {
		t.Fatalf("legacy times = %v", e2.Times)
	}
}
