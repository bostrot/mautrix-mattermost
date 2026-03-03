package connector

import (
	"context"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// FetchMessages implements bridgev2.BackfillingNetworkAPI for paginated history.
func (c *MattermostClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	channelID := string(params.Portal.ID)

	var beforePostID string
	if params.AnchorMessage != nil && !params.Forward {
		beforePostID = string(params.AnchorMessage.ID)
	}

	pl, err := mattermost.GetPosts(c.serverURL(), c.Token, channelID, params.Count, beforePostID)
	if err != nil {
		return nil, err
	}

	var intent bridgev2.MatrixAPI
	if c.UserLogin != nil && c.UserLogin.Bridge != nil {
		intent = c.UserLogin.Bridge.Bot
	}

	// pl.Order is newest-first; reverse to oldest-first for backfill.
	backfill := make([]*bridgev2.BackfillMessage, 0, len(pl.Order))
	for i := len(pl.Order) - 1; i >= 0; i-- {
		postID := pl.Order[i]
		post, ok := pl.Posts[postID]
		if !ok || post == nil || post.DeleteAt != 0 {
			continue // skip deleted posts
		}

		isFromMe := c.UserID != "" && post.UserID == c.UserID
		sender := bridgev2.EventSender{
			Sender:   networkid.UserID(post.UserID),
			IsFromMe: isFromMe,
		}
		if isFromMe && c.UserLogin != nil {
			sender.SenderLogin = c.UserLogin.ID
		}

		ts := post.Timestamp()
		if ts.IsZero() {
			ts = time.Now()
		}

		converted, convErr := c.convertPost(ctx, intent, params.Portal.MXID, post)
		if convErr != nil {
			converted = &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				ID:      networkid.PartID(""),
				Type:    event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgText, Body: post.Message},
			}}}
		}

		backfill = append(backfill, &bridgev2.BackfillMessage{
			ConvertedMessage: converted,
			Sender:           sender,
			ID:               networkid.MessageID(post.ID),
			Timestamp:        ts,
		})
	}

	hasMore := len(pl.Order) >= params.Count

	// Mark read on forward backfill OR if every message in this batch is
	// from the logged-in user (e.g. initial portal creation triggered by a
	// self-sent message — we don't want to notify for our own history).
	allFromMe := len(backfill) > 0
	for _, m := range backfill {
		if !m.Sender.IsFromMe {
			allFromMe = false
			break
		}
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: backfill,
		HasMore:  hasMore,
		MarkRead: params.Forward || allFromMe,
	}, nil
}
