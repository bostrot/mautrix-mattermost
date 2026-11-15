package connector

import (
	"context"
	"sort"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// defaultChannelPageSize is used when the caller doesn't ask for a specific
// batch size (Mattermost's own default page size).
const defaultChannelPageSize = 60

// FetchMessages implements bridgev2.BackfillingNetworkAPI for paginated history.
//
// It handles the two distinct modes bridgev2 asks for:
//
//   - Channel timeline (ThreadRoot empty): GET /channels/{id}/posts. Every
//     visible post is bridged in the batch, including thread replies. Mattermost
//     threads are flat (a reply only carries root_id), so replies are part of
//     the channel pagination just like roots. This must not depend on thread
//     backfill being enabled: bridgev2 only follows ShouldBackfillThread when
//     backfill.threads.max_initial_messages > 0, which is off by default, and
//     skipping replies here would lose them permanently.
//
//   - Thread (ThreadRoot set): GET /posts/{id}/thread. Mattermost returns the
//     whole thread in one response and the endpoint has no paging parameters.
//     The batch is therefore never truncated and HasMore is always false;
//     bridgev2's own anchor cutoff removes the part that is already bridged.
func (c *MattermostClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.ThreadRoot != "" {
		return c.fetchThreadMessages(ctx, params)
	}
	return c.fetchChannelMessages(ctx, params)
}

// fetchChannelMessages backfills the portal timeline. Replies are included in
// the batch alongside their roots, exactly like the original channel backfill.
func (c *MattermostClient) fetchChannelMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	channelID := string(params.Portal.ID)

	pageSize := params.Count
	if pageSize <= 0 {
		pageSize = defaultChannelPageSize
	}

	// Backwards backfill walks towards older posts. The cursor from the
	// previous batch takes precedence, because it keeps paginating even when
	// the previous page contained nothing that could be stored.
	var beforePostID string
	if !params.Forward {
		beforePostID = string(params.Cursor)
		if beforePostID == "" && params.AnchorMessage != nil {
			beforePostID = string(params.AnchorMessage.ID)
		}
	}

	pl, err := mattermost.GetPosts(c.serverURL(), c.Token, channelID, pageSize, beforePostID)
	if err != nil {
		return nil, err
	}

	intent := c.backfillIntent()

	// pl.Order is newest-first; reverse to oldest-first for backfill.
	msgs := make([]*bridgev2.BackfillMessage, 0, len(pl.Order))
	for i := len(pl.Order) - 1; i >= 0; i-- {
		postID := pl.Order[i]
		post, ok := pl.Posts[postID]
		if !ok || post == nil || post.DeleteAt != 0 {
			continue // skip deleted posts
		}
		msgs = append(msgs, c.newBackfillMessage(ctx, intent, params.Portal, post, ""))
	}

	resp := &bridgev2.FetchMessagesResponse{
		Messages: msgs,
		HasMore:  len(pl.Order) >= pageSize,
		MarkRead: params.Forward || allBackfillFromMe(msgs),
	}
	// The cursor for backwards pagination is the oldest post of the page as
	// returned by the server (last entry in the newest-first order), including
	// deleted posts, so a page with no bridged messages cannot stall the queue.
	if !params.Forward && len(pl.Order) > 0 {
		resp.Cursor = networkid.PaginationCursor(pl.Order[len(pl.Order)-1])
	}
	return resp, nil
}

// fetchThreadMessages backfills one Mattermost thread. The endpoint returns the
// whole thread and has no pagination, so all posts are returned regardless of
// params.Count: truncating here would silently and permanently drop replies,
// since bridgev2 cannot ask for another thread page.
func (c *MattermostClient) fetchThreadMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	pl, err := mattermost.GetPostThread(c.serverURL(), c.Token, string(params.ThreadRoot))
	if err != nil {
		return nil, err
	}

	posts := orderedThreadPosts(pl)
	intent := c.backfillIntent()
	msgs := make([]*bridgev2.BackfillMessage, 0, len(posts))
	for _, post := range posts {
		msgs = append(msgs, c.newBackfillMessage(ctx, intent, params.Portal, post, params.ThreadRoot))
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: msgs,
		// No cursor and no truncation: the whole thread was returned. HasMore
		// must stay false because this endpoint cannot paginate.
		HasMore:  false,
		MarkRead: params.Forward || allBackfillFromMe(msgs),
		// A thread can be backfilled more than once (e.g. re-synced channel
		// batches), so let the bridge drop replies that are already stored.
		AggressiveDeduplication: true,
	}, nil
}

// orderedThreadPosts flattens a thread PostList into a deduplicated,
// oldest-first slice. Mattermost does not guarantee the order of the returned
// thread, so ordering by create_at (ID as a stable tiebreaker) is required for
// the chronological contract of FetchMessagesResponse.
func orderedThreadPosts(pl *mattermost.PostList) []*mattermost.Post {
	if pl == nil {
		return nil
	}
	posts := make([]*mattermost.Post, 0, len(pl.Order))
	seen := make(map[string]struct{}, len(pl.Order))
	add := func(post *mattermost.Post) {
		if post == nil || post.ID == "" || post.DeleteAt != 0 {
			return
		}
		if _, dup := seen[post.ID]; dup {
			return
		}
		seen[post.ID] = struct{}{}
		posts = append(posts, post)
	}
	for _, id := range pl.Order {
		add(pl.Posts[id])
	}
	// Posts that only exist in the map (no Order entry) would otherwise be
	// lost entirely.
	for id, post := range pl.Posts {
		if _, ok := seen[id]; ok {
			continue
		}
		add(post)
	}
	sort.SliceStable(posts, func(i, j int) bool {
		if posts[i].CreateAt != posts[j].CreateAt {
			return posts[i].CreateAt < posts[j].CreateAt
		}
		return posts[i].ID < posts[j].ID
	})
	return posts
}

// newBackfillMessage converts one post into a BackfillMessage. threadRoot is
// set when the post was fetched through the thread endpoint, and is used as a
// fallback when the server omits root_id on a reply.
func (c *MattermostClient) newBackfillMessage(
	ctx context.Context,
	intent bridgev2.MatrixAPI,
	portal *bridgev2.Portal,
	post *mattermost.Post,
	threadRoot networkid.MessageID,
) *bridgev2.BackfillMessage {
	isFromMe := c.UserID != "" && post.UserID == c.UserID
	sender := bridgev2.EventSender{
		Sender:   networkid.UserID(post.UserID),
		IsFromMe: isFromMe,
	}
	if isFromMe && c.UserLogin != nil {
		sender.SenderLogin = c.UserLogin.ID
	}

	converted, err := c.convertPost(ctx, intent, portal.MXID, post)
	if err != nil || converted == nil || len(converted.Parts) == 0 {
		_, postRoot := threadRelation(post)
		converted = &bridgev2.ConvertedMessage{
			ThreadRoot: postRoot,
			Parts: []*bridgev2.ConvertedMessagePart{{
				ID:      networkid.PartID(""),
				Type:    event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgText, Body: post.Message},
			}},
		}
	}

	if threadRoot != "" && post.ID != string(threadRoot) && converted.ThreadRoot == nil {
		root := threadRoot
		converted.ThreadRoot = &root
	}

	ts := post.Timestamp()
	if ts.IsZero() {
		ts = time.Now()
	}

	return &bridgev2.BackfillMessage{
		ConvertedMessage: converted,
		Sender:           sender,
		ID:               networkid.MessageID(post.ID),
		Timestamp:        ts,
	}
}

// backfillIntent returns the Matrix intent used for media uploads during
// backfill, or nil when the client isn't attached to a bridge (unit tests).
func (c *MattermostClient) backfillIntent() bridgev2.MatrixAPI {
	if c.UserLogin != nil && c.UserLogin.Bridge != nil {
		return c.UserLogin.Bridge.Bot
	}
	return nil
}

// allBackfillFromMe reports whether the batch is non-empty and every message
// was sent by the logged-in user, in which case it is safe to mark it read.
func allBackfillFromMe(msgs []*bridgev2.BackfillMessage) bool {
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		if !msg.Sender.IsFromMe {
			return false
		}
	}
	return true
}
