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

func TestLeaveTeamDecoding(t *testing.T) {
	c := &Client{Events: make(chan interface{}, 4)}
	c.parseEvent([]byte(`{"event":"leave_team","data":{"team_id":"t1","user_id":"u1"}}`))
	c.parseEvent([]byte(`{"event":"leave_team","data":{"user_id":"u1"}}`))
	c.parseEvent([]byte(`{"event":"leave_team","data":{"team_id":"t1"}}`))
	if len(c.Events) != 1 {
		t.Fatalf("events = %d, want 1", len(c.Events))
	}
	ev := (<-c.Events).(*LeaveTeamEvent)
	if ev.TeamID != "t1" || ev.UserID != "u1" {
		t.Fatalf("event = %+v", ev)
	}
}
