package mattermost

import "testing"

func TestUpdateTeamDecoding(t *testing.T) {
	c := &Client{Events: make(chan interface{}, 3)}
	c.parseEvent([]byte(`{"event":"update_team","data":{"team":"{\"id\":\"t1\",\"display_name\":\"X\",\"last_team_icon_update\":7}"}}`))
	c.parseEvent([]byte(`{"event":"update_team","data":{"team":"not json"}}`))
	c.parseEvent([]byte(`{"event":"update_team","data":{"team":"{}"}}`))
	if len(c.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(c.Events))
	}
	ev := (<-c.Events).(*TeamUpdatedEvent)
	if ev.Team.ID != "t1" || ev.Team.DisplayName != "X" || ev.Team.LastTeamIconUpdate != 7 {
		t.Fatalf("team = %+v", ev.Team)
	}
}
