package mattermost

import (
	"fmt"
	"time"
)

// SendPost sends a text message to a channel and returns the new post ID.
// If rootID is non-empty, the post is sent as a thread reply.
func (c *Client) SendPost(channelID, message string, rootID string, fileIDs ...string) (string, error) {
	return SendPost(c.ServerURL, c.Token, channelID, message, rootID, fileIDs...)
}

// SendPost is a standalone function for posting without a Client instance.
func SendPost(serverURL, token, channelID, message, rootID string, fileIDs ...string) (string, error) {
	body := map[string]any{
		"channel_id": channelID,
		"message":    message,
		"create_at":  time.Now().UnixMilli(),
	}
	if rootID != "" {
		body["root_id"] = rootID
	}
	if len(fileIDs) > 0 {
		body["file_ids"] = fileIDs
	}

	var post Post
	if err := doPost(serverURL, token, "/api/v4/posts", body, &post); err != nil {
		return "", err
	}
	return post.ID, nil
}

// EditPost updates the message text of an existing post.
func (c *Client) EditPost(postID, newMessage string) error {
	return EditPost(c.ServerURL, c.Token, postID, newMessage)
}

// EditPost is a standalone function for editing without a Client instance.
func EditPost(serverURL, token, postID, newMessage string) error {
	body := map[string]any{
		"id":      postID,
		"message": newMessage,
	}
	path := fmt.Sprintf("/api/v4/posts/%s", postID)
	return doPut(serverURL, token, path, body)
}

// DeletePost soft-deletes a post.
func (c *Client) DeletePost(postID string) error {
	return DeletePost(c.ServerURL, c.Token, postID)
}

// DeletePost is a standalone function for deleting without a Client instance.
func DeletePost(serverURL, token, postID string) error {
	return doDelete(serverURL, token, "/api/v4/posts/"+postID)
}

// AddReaction adds an emoji reaction to a post.
func (c *Client) AddReaction(userID, postID, emojiName string) error {
	return AddReaction(c.ServerURL, c.Token, userID, postID, emojiName)
}

// AddReaction is a standalone function.
func AddReaction(serverURL, token, userID, postID, emojiName string) error {
	body := map[string]string{
		"user_id":    userID,
		"post_id":    postID,
		"emoji_name": emojiName,
	}
	return doPost(serverURL, token, "/api/v4/reactions", body, nil)
}

// RemoveReaction removes an emoji reaction from a post.
func (c *Client) RemoveReaction(userID, postID, emojiName string) error {
	return RemoveReaction(c.ServerURL, c.Token, userID, postID, emojiName)
}

// RemoveReaction is a standalone function.
func RemoveReaction(serverURL, token, userID, postID, emojiName string) error {
	path := fmt.Sprintf("/api/v4/users/%s/posts/%s/reactions/%s", userID, postID, emojiName)
	return doDelete(serverURL, token, path)
}

// SendTyping sends a typing indicator to a channel.
// The Mattermost REST API is POST /api/v4/users/{user_id}/typing with channel_id in the body.
func (c *Client) SendTyping(userID, channelID string) error {
	// The Mattermost REST API is POST /api/v4/users/{user_id}/typing with channel_id in the body.
	path := fmt.Sprintf("/api/v4/users/%s/typing", userID)
	return doPost(c.ServerURL, c.Token, path, map[string]any{"channel_id": channelID}, nil)
}
