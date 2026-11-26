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
	if isTeamPortal(params.Portal.ID) {
		return &bridgev2.FetchMessagesResponse{}, nil // spaces have no timeline
	}
	channelID := string(params.Portal.ID)

	var beforePostID string
	if params.AnchorMessage != nil && !params.Forward {
		beforePostID = string(params.AnchorMessage.ID)
	}

	pl, err := mattermost.GetPosts(c.serverURL(), c.Token, channelID, params.Count, beforePostID)
	if err != nil {
		return nil, err
	}

	// In an archived room the logged-in user's Matrix account is read-only, so
	// their own history is sent by their ghost instead, like everyone else's.
	archived := c.channelArchived(channelID)
	authors := map[string]bool{}

	var intent bridgev2.MatrixAPI
	if c.UserLogin != nil && c.UserLogin.Bridge != nil {
		intent = c.UserLogin.Bridge.Bot
	}

	// pl.Order is newest-first; reverse to oldest-first for backfill.
	backfill := make([]*bridgev2.BackfillMessage, 0, len(pl.Order))
	var fromMe []bool // per message, whatever identity it is sent under
	for i := len(pl.Order) - 1; i >= 0; i-- {
		postID := pl.Order[i]
		post, ok := pl.Posts[postID]
		if !ok || post == nil || post.DeleteAt != 0 {
			continue // skip deleted posts
		}

		isFromMe := c.UserID != "" && post.UserID == c.UserID
		sender := c.backfillSender(post.UserID, archived)
		authors[post.UserID] = true
		fromMe = append(fromMe, isFromMe)

		ts := post.Timestamp()
		if ts.IsZero() {
			ts = time.Now()
		}

		converted, convErr := c.convertPost(ctx, intent, params.Portal.MXID, post)
		if convErr != nil {
			converted = &bridgev2.ConvertedMessage{
				ThreadRoot: postThreadRoot(post),
				Parts: []*bridgev2.ConvertedMessagePart{{
					ID:      networkid.PartID(""),
					Type:    event.EventMessage,
					Content: &event.MessageEventContent{MsgType: event.MsgText, Body: postMarkdown(post)},
				}},
			}
		}

		backfill = append(backfill, &bridgev2.BackfillMessage{
			ConvertedMessage: converted,
			Sender:           sender,
			ID:               networkid.MessageID(post.ID),
			Timestamp:        ts,
			Reactions:        c.backfillReactionsFor(post, archived, authors),
		})
	}

	hasMore := len(pl.Order) >= params.Count

	// Mark read on forward backfill OR if every message in this batch is
	// from the logged-in user (e.g. initial portal creation triggered by a
	// self-sent message — we don't want to notify for our own history).
	allFromMe := len(backfill) > 0
	for _, own := range fromMe {
		if !own {
			allFromMe = false
			break
		}
	}

	if archived {
		c.allowArchivedPosting(ctx, params.Portal.MXID, authors)
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: backfill,
		HasMore:  hasMore,
		MarkRead: params.Forward || allFromMe,
	}, nil
}

// backfillReactions converts the reactions Mattermost returns in a post's
// metadata into backfill reactions, so history that is bridged after the fact
// carries the reactions it had. Live reactions arrive as websocket events and
// take a different path; this only covers what is already on a post when it is
// backfilled.
//
// Removed reactions, entries without a user or emoji, and repeats of the same
// user and emoji are dropped. The emoji is converted exactly as for live
// reactions, and the sender is built the same way as for the message itself.
func (c *MattermostClient) backfillReactions(post *mattermost.Post) []*bridgev2.BackfillReaction {
	return c.backfillReactionsFor(post, false, nil)
}

// backfillReactionsFor is backfillReactions for a room that may be archived
// (see backfillSender). The reacting users are added to authors, if given.
func (c *MattermostClient) backfillReactionsFor(post *mattermost.Post, archived bool, authors map[string]bool) []*bridgev2.BackfillReaction {
	if post == nil || post.Metadata == nil || len(post.Metadata.Reactions) == 0 {
		return nil
	}
	type reactionKey struct{ user, emoji string }
	seen := make(map[reactionKey]bool, len(post.Metadata.Reactions))
	out := make([]*bridgev2.BackfillReaction, 0, len(post.Metadata.Reactions))
	for _, r := range post.Metadata.Reactions {
		if r == nil || r.UserID == "" || r.EmojiName == "" || r.DeleteAt != 0 {
			continue
		}
		key := reactionKey{r.UserID, r.EmojiName}
		if seen[key] {
			continue
		}
		seen[key] = true

		sender := c.backfillSender(r.UserID, archived)
		if authors != nil {
			authors[r.UserID] = true
		}
		var ts time.Time // zero: bridgev2 places it just after the message
		if r.CreateAt > 0 {
			ts = time.UnixMilli(r.CreateAt)
		}
		out = append(out, &bridgev2.BackfillReaction{
			Sender:    sender,
			EmojiID:   networkid.EmojiID(r.EmojiName),
			Emoji:     mmEmojiToUnicode(r.EmojiName),
			Timestamp: ts,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// backfillSender is who a backfilled message or reaction by userID is sent as.
// That is the user's ghost, except for the logged-in user, who is sent as
// themselves. In an archived room the read-only overrides stop that Matrix
// account, which is the point of them, so their own history is sent by their
// ghost instead. The room keeps everything they wrote, under their Mattermost
// name.
func (c *MattermostClient) backfillSender(userID string, archived bool) bridgev2.EventSender {
	sender := bridgev2.EventSender{Sender: networkid.UserID(userID)}
	if !archived && c.UserID != "" && userID == c.UserID {
		sender.IsFromMe = true
		if c.UserLogin != nil {
			sender.SenderLogin = c.UserLogin.ID
		}
	}
	return sender
}
