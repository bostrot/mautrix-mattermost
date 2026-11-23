package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// mediaMsgType maps a MIME type prefix → Matrix message type.
func mediaMsgType(contentType string) event.MessageType {
	switch {
	case strings.HasPrefix(contentType, "image/"):
		return event.MsgImage
	case strings.HasPrefix(contentType, "video/"):
		return event.MsgVideo
	case strings.HasPrefix(contentType, "audio/"):
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

// postThreadRoot returns the Matrix thread root for a Mattermost post.
//
// Mattermost only has one level of threading: every reply carries root_id
// pointing at the thread root. That maps to a Matrix thread (ThreadRoot). No
// ReplyTo is set: bridgev2 builds the thread fallback reply itself from the
// thread root and previous thread event, which points at the right event even
// when a later reply in the same thread is the fallback target.
func postThreadRoot(post *mattermost.Post) *networkid.MessageID {
	if post == nil || post.RootID == "" {
		return nil
	}
	root := networkid.MessageID(post.RootID)
	return &root
}

// convertPost converts a Mattermost post (text + file attachments) into Matrix message parts.
// intent is used to upload media to the homeserver; it may be nil (e.g. in unit tests without attachments).
func (c *MattermostClient) convertPost(ctx context.Context, intent bridgev2.MatrixAPI, roomID id.RoomID, post *mattermost.Post) (*bridgev2.ConvertedMessage, error) {
	var parts []*bridgev2.ConvertedMessagePart
	threadRoot := postThreadRoot(post)

	// Convert file attachments.
	files := resolveFiles(post)
	for _, fi := range files {
		if intent == nil {
			continue // skip uploads when no intent available (unit tests)
		}
		fileURL := fmt.Sprintf("%s/api/v4/files/%s", c.serverURL(), fi.ID)
		req, err := http.NewRequestWithContext(ctx, "GET", fileURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)

		resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if err != nil {
			continue
		}
		attData, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil || resp.StatusCode >= 400 {
			continue
		}

		mimeType := fi.MimeType
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		if sniffed := sniffMimeType(attData); sniffed != "" {
			mimeType = sniffed
		}

		filename := fi.Name
		if filename == "" {
			filename = fi.ID
		}

		mxcURL, encFile, uploadErr := intent.UploadMedia(ctx, roomID, attData, filename, mimeType)
		if uploadErr != nil {
			continue
		}

		msgType := mediaMsgType(mimeType)
		content := &event.MessageEventContent{
			MsgType: msgType,
			Body:    filename,
			URL:     mxcURL,
			File:    encFile,
			Info: &event.FileInfo{
				MimeType: mimeType,
				Size:     int(fi.Size),
			},
		}
		if fi.Width > 0 {
			content.Info.Width = fi.Width
		}
		if fi.Height > 0 {
			content.Info.Height = fi.Height
		}
		// bridgev2 synthesizes missing thread-root event IDs with an empty part ID.
		// The first successful upload gets that ID, even if an earlier upload failed.
		// Existing bridged attachment IDs are unaffected.
		partID := networkid.PartID("att_" + fi.ID)
		if len(parts) == 0 {
			partID = ""
		}
		parts = append(parts, &bridgev2.ConvertedMessagePart{
			ID:      partID,
			Type:    event.EventMessage,
			Content: content,
		})
	}

	// Convert text body: the message plus any integration attachments.
	if postMarkdown(post) != "" {
		if len(parts) > 0 {
			// Embed caption into the first attachment (Matrix caption convention / MSC2530).
			firstContent := parts[0].Content
			firstContent.FileName = firstContent.Body
			rendered := c.renderPost(ctx, post)
			firstContent.Mentions = rendered.Mentions
			firstContent.Body = rendered.Body
			if rendered.FormattedBody != "" {
				firstContent.FormattedBody = rendered.FormattedBody
				firstContent.Format = rendered.Format
			}
			c.attachPermalinkQuotes(ctx, firstContent, post.Message)
		} else {
			// Text-only message.
			textContent := c.renderPost(ctx, post)
			// Quote linked Mattermost posts under the link. Best effort: the
			// message is sent either way.
			c.attachPermalinkQuotes(ctx, &textContent, post.Message)
			cm := &bridgev2.ConvertedMessage{
				ThreadRoot: threadRoot,
				Parts: []*bridgev2.ConvertedMessagePart{{
					ID:      networkid.PartID(""),
					Type:    event.EventMessage,
					Content: &textContent,
				}},
			}
			return cm, nil
		}
	}

	// Fallback: ensure at least one part is returned.
	if len(parts) == 0 {
		parts = []*bridgev2.ConvertedMessagePart{{
			ID:   networkid.PartID(""),
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    postMarkdown(post),
			},
		}}
	}

	return &bridgev2.ConvertedMessage{
		ThreadRoot: threadRoot,
		Parts:      parts,
	}, nil
}

// resolveFiles returns the FileInfo list from post.Metadata.Files when present,
// otherwise falls back to constructing minimal FileInfo from post.FileIDs.
func resolveFiles(post *mattermost.Post) []*mattermost.FileInfo {
	if post.Metadata != nil && len(post.Metadata.Files) > 0 {
		return post.Metadata.Files
	}
	if len(post.FileIDs) == 0 {
		return nil
	}
	files := make([]*mattermost.FileInfo, 0, len(post.FileIDs))
	for _, id := range post.FileIDs {
		files = append(files, &mattermost.FileInfo{ID: id})
	}
	return files
}
