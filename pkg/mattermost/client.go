package mattermost

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait     = 90 * time.Second
	pingInterval = 30 * time.Second
	writeWait    = 10 * time.Second
)

// Client is a Mattermost WebSocket client for a single authenticated user.
type Client struct {
	ServerURL string
	Token     string
	WS        *websocket.Conn
	Events    chan any

	wmu sync.Mutex // guards all WS writes
}

// NewClient creates a new Mattermost WS client. serverURL must be the base HTTP(S) URL.
func NewClient(serverURL, token string) *Client {
	return &Client{
		ServerURL: serverURL,
		Token:     token,
		Events:    make(chan any, 100),
	}
}

// Connect opens the WebSocket connection and authenticates.
func (c *Client) Connect() error {
	wsURL := httpToWS(normalizeURL(c.ServerURL)) + "/api/v4/websocket"
	fmt.Printf("[mm-ws] connecting to %s\n", wsURL)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		fmt.Printf("[mm-ws] dial error: %v\n", err)
		return err
	}
	c.WS = conn
	fmt.Printf("[mm-ws] connected at %s\n", time.Now().Format(time.RFC3339))

	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	conn.SetReadDeadline(time.Now().Add(pongWait))

	// Send authentication challenge.
	authMsg, _ := json.Marshal(map[string]any{
		"seq":    1,
		"action": "authentication_challenge",
		"data":   map[string]string{"token": c.Token},
	})
	c.wmu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, authMsg)
	c.wmu.Unlock()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to send auth challenge: %w", err)
	}

	go c.readLoop()
	go c.pingLoop()
	return nil
}

// Close shuts down the WebSocket connection.
func (c *Client) Close() {
	if c.WS != nil {
		c.WS.Close()
	}
}

func (c *Client) readLoop() {
	fmt.Printf("[mm-ws] readLoop started at %s\n", time.Now().Format(time.RFC3339))
	defer func() {
		fmt.Printf("[mm-ws] readLoop exiting at %s\n", time.Now().Format(time.RFC3339))
		c.WS.Close()
		close(c.Events)
	}()
	for {
		_, message, err := c.WS.ReadMessage()
		if err != nil {
			fmt.Printf("[mm-ws] read error: %v\n", err)
			return
		}
		c.WS.SetReadDeadline(time.Now().Add(pongWait))
		c.parseEvent(message)
	}
}

func (c *Client) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.wmu.Lock()
		err := c.WS.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait))
		c.wmu.Unlock()
		if err != nil {
			return
		}
	}
}

// parseEvent decodes a raw WS message and emits a typed event on c.Events.
func (c *Client) parseEvent(raw []byte) {
	var env WSEvent
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}

	// Ignore auth response frames (they have no "event" field but have "status").
	if env.Event == "" {
		return
	}

	switch env.Event {
	case "hello":
		c.Events <- &HelloEvent{ServerVersion: strFromData(env.Data, "server_version")}

	case "posted":
		post := decodePost(env.Data, "post")
		if post == nil {
			return
		}
		c.Events <- &PostedEvent{
			Post:        post,
			ChannelID:   post.ChannelID,
			TeamID:      strFromData(env.Data, "team_id"),
			ChannelType: strFromData(env.Data, "channel_type"),
			ChannelName: strFromData(env.Data, "channel_name"),
		}

	case "post_edited":
		post := decodePost(env.Data, "post")
		if post == nil {
			return
		}
		c.Events <- &PostEditedEvent{Post: post, ChannelID: post.ChannelID}

	case "post_deleted":
		post := decodePost(env.Data, "post")
		if post == nil {
			return
		}
		c.Events <- &PostDeletedEvent{Post: post, ChannelID: post.ChannelID}

	case "reaction_added":
		r := decodeReaction(env.Data, "reaction")
		if r == nil {
			return
		}
		c.Events <- &ReactionEvent{Reaction: r, ChannelID: env.Broadcast.ChannelID, IsAdd: true}

	case "reaction_removed":
		r := decodeReaction(env.Data, "reaction")
		if r == nil {
			return
		}
		c.Events <- &ReactionEvent{Reaction: r, ChannelID: env.Broadcast.ChannelID, IsAdd: false}

	case "typing":
		c.Events <- &TypingEvent{
			UserID:    strFromData(env.Data, "user_id"),
			ChannelID: env.Broadcast.ChannelID,
		}

	case "channel_created":
		ch := decodeChannel(env.Data, "channel")
		if ch != nil {
			c.Events <- &ChannelCreatedEvent{Channel: ch}
		}

	case "channel_updated":
		ch := decodeChannel(env.Data, "channel")
		if ch != nil {
			c.Events <- &ChannelUpdatedEvent{Channel: ch}
		}

	case "channel_deleted":
		channelID := strFromData(env.Data, "channel_id")
		if channelID == "" {
			return
		}
		c.Events <- &ChannelDeletedEvent{
			ChannelID: channelID,
			TeamID:    env.Broadcast.TeamID,
		}

	case "channel_restored":
		channelID := strFromData(env.Data, "channel_id")
		if channelID == "" {
			return
		}
		c.Events <- &ChannelRestoredEvent{ChannelID: channelID, TeamID: env.Broadcast.TeamID}

	case "direct_added":
		c.Events <- &DirectAddedEvent{ChannelID: env.Broadcast.ChannelID}

	case "user_added", "user_removed":
		if evt := channelMemberEvent(env); evt != nil {
			c.Events <- evt
		}

	case "status_change":
		c.Events <- &StatusChangeEvent{
			UserID: strFromData(env.Data, "user_id"),
			Status: strFromData(env.Data, "status"),
		}

	case "user_updated":
		u := decodeUser(env.Data, "user")
		if u != nil {
			c.Events <- &UserUpdatedEvent{User: u}
		}

	case "update_team":
		raw := strFromData(env.Data, "team")
		var t Team
		if raw != "" && json.Unmarshal([]byte(raw), &t) == nil && t.ID != "" {
			c.Events <- &TeamUpdatedEvent{Team: &t}
		}

	case "multiple_channels_viewed":
		if evt := channelsViewedEvent(env.Data); evt != nil {
			c.Events <- evt
		}

	case "channel_viewed": // servers before multiple_channels_viewed
		if id := strFromData(env.Data, "channel_id"); id != "" {
			c.Events <- &ChannelsViewedEvent{Times: map[string]int64{id: 0}}
		}

	case "system_console_noti": // ignore console notifications

	default:
		// Unknown event — silently ignored.
	}
}

// channelsViewedEvent decodes "multiple_channels_viewed": channel_times maps
// channel IDs to the time (ms) they were viewed.
func channelsViewedEvent(data map[string]interface{}) *ChannelsViewedEvent {
	raw, _ := data["channel_times"].(map[string]interface{})
	times := make(map[string]int64, len(raw))
	for id, v := range raw {
		if ms, ok := v.(float64); ok && id != "" {
			times[id] = int64(ms)
		}
	}
	if len(times) == 0 {
		return nil
	}
	return &ChannelsViewedEvent{Times: times}
}

// channelMemberEvent decodes "user_added" and "user_removed". Mattermost
// sends both to the channel (channel in the broadcast, user in the data) and
// sends "user_removed" to the removed user as well (channel in the data, user
// in the broadcast).
func channelMemberEvent(env WSEvent) *ChannelMemberEvent {
	channelID := strFromData(env.Data, "channel_id")
	if channelID == "" {
		channelID = env.Broadcast.ChannelID
	}
	userID := strFromData(env.Data, "user_id")
	if userID == "" {
		userID = env.Broadcast.UserID
	}
	if channelID == "" || userID == "" {
		return nil
	}
	return &ChannelMemberEvent{ChannelID: channelID, UserID: userID, Added: env.Event == "user_added"}
}

// --- helpers ---

func httpToWS(url string) string {
	if len(url) >= 8 && url[:8] == "https://" {
		return "wss://" + url[8:]
	}
	if len(url) >= 7 && url[:7] == "http://" {
		return "ws://" + url[7:]
	}
	return url
}

func strFromData(data map[string]interface{}, key string) string {
	if v, ok := data[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// decodePost decodes a JSON-string-encoded Post from event data.
// Mattermost double-encodes posts as a JSON string inside the data object.
func decodePost(data map[string]interface{}, key string) *Post {
	raw := strFromData(data, key)
	if raw == "" {
		return nil
	}
	var p Post
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil
	}
	return &p
}

// decodeReaction decodes a JSON-string-encoded Reaction from event data.
func decodeReaction(data map[string]interface{}, key string) *Reaction {
	raw := strFromData(data, key)
	if raw == "" {
		return nil
	}
	var r Reaction
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil
	}
	return &r
}

// decodeChannel decodes a JSON-string-encoded Channel from event data.
func decodeChannel(data map[string]interface{}, key string) *Channel {
	raw := strFromData(data, key)
	if raw == "" {
		return nil
	}
	var ch Channel
	if err := json.Unmarshal([]byte(raw), &ch); err != nil {
		return nil
	}
	return &ch
}

// decodeUser decodes a JSON-string-encoded User from event data.
func decodeUser(data map[string]interface{}, key string) *User {
	raw := strFromData(data, key)
	if raw == "" {
		return nil
	}
	var u User
	if err := json.Unmarshal([]byte(raw), &u); err != nil {
		return nil
	}
	return &u
}
