package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// GetCapabilities returns the Matrix room features supported by this connector.
func (c *MattermostClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		Delete:              event.CapLevelFullySupported,
		DeleteForMe:         true,
		Edit:                event.CapLevelFullySupported,
		Reply:               event.CapLevelFullySupported,
		Reaction:            event.CapLevelFullySupported,
		TypingNotifications: true,
		File: event.FileFeatureMap{
			event.MsgImage:      &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"image/*": event.CapLevelFullySupported}},
			event.MsgVideo:      &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"video/*": event.CapLevelFullySupported}},
			event.MsgAudio:      &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"audio/*": event.CapLevelFullySupported}},
			event.MsgFile:       &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelFullySupported}},
			event.CapMsgSticker: &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"image/*": event.CapLevelFullySupported}},
			event.CapMsgGIF:     &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"image/*": event.CapLevelFullySupported, "video/*": event.CapLevelFullySupported}},
		},
	}
}

// HandleMatrixMessage bridges a message from Matrix → Mattermost.
func (c *MattermostClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return nil, wrapSendErr(fmt.Errorf("not connected to Mattermost"))
	}

	content := msg.Content
	if content == nil {
		return nil, wrapSendErr(fmt.Errorf("empty message content"))
	}

	channelID := string(msg.Portal.ID)
	var rootID string
	if msg.ReplyTo != nil {
		rootID = string(msg.ReplyTo.ID)
	}

	switch content.MsgType {
	case event.MsgText, event.MsgNotice, event.MsgEmote:
		body := content.Body
		if content.FormattedBody != "" {
			body = format.HTMLToMarkdown(content.FormattedBody)
		}
		// Replace Matrix @mention pills with Mattermost @username mentions.
		if content.Mentions != nil {
			for _, mxid := range content.Mentions.UserIDs {
				ghost, err := c.UserLogin.Bridge.GetGhostByMXID(ctx, mxid)
				if err != nil || ghost == nil {
					continue
				}
				c.mu.RLock()
				u, hasUser := c.users[string(ghost.ID)]
				c.mu.RUnlock()
				if hasUser {
					body = strings.ReplaceAll(body, "@"+u.DisplayName(), "@"+u.Username)
				}
			}
		}
		if content.MsgType == event.MsgEmote {
			body = "_" + body + "_"
		}
		zerolog.Ctx(ctx).Debug().Str("channel", channelID).Msg("Sending message to Mattermost")
		postID, err := mattermost.SendPost(c.serverURL(), c.Token, channelID, body, rootID)
		if err != nil {
			return nil, wrapSendErr(err)
		}
		return &bridgev2.MatrixMessageResponse{
			DB: &database.Message{
				ID:       networkid.MessageID(postID),
				SenderID: networkid.UserID(c.UserID),
			},
		}, nil

	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile, event.CapMsgSticker:
		mxcURI := content.URL
		var encFile *event.EncryptedFileInfo
		if content.File != nil {
			encFile = content.File
			if content.File.URL != "" {
				mxcURI = content.File.URL
			}
		}
		if mxcURI == "" {
			return nil, fmt.Errorf("media message has no URL")
		}

		data, err := c.UserLogin.Bridge.Bot.DownloadMedia(ctx, mxcURI, encFile)
		if err != nil {
			return nil, wrapSendErr(fmt.Errorf("failed to download media: %w", err))
		}

		mimeType := "application/octet-stream"
		if content.Info != nil && content.Info.MimeType != "" {
			mimeType = content.Info.MimeType
		}
		if sniffed := sniffMimeType(data); sniffed != "" {
			mimeType = sniffed
		}

		filename := filenameForMime(mimeType)
		if content.Body != "" && content.FileName != "" {
			filename = content.FileName
		} else if content.Body != "" {
			filename = content.Body
		}

		zerolog.Ctx(ctx).Debug().
			Str("mime", mimeType).
			Str("filename", filename).
			Msg("Uploading media to Mattermost")

		fileID, err := mattermost.UploadFile(c.serverURL(), c.Token, channelID, data, filename, mimeType)
		if err != nil {
			return nil, wrapSendErr(fmt.Errorf("failed to upload media: %w", err))
		}

		// Caption is in Body when FileName is set separately (MSC2530).
		caption := ""
		if content.FileName != "" {
			caption = content.Body
		}

		postID, err := mattermost.SendPost(c.serverURL(), c.Token, channelID, caption, rootID, fileID)
		if err != nil {
			return nil, wrapSendErr(err)
		}
		return &bridgev2.MatrixMessageResponse{
			DB: &database.Message{
				ID:       networkid.MessageID(postID),
				SenderID: networkid.UserID(c.UserID),
			},
		}, nil

	default:
		return nil, wrapSendErr(fmt.Errorf("unsupported message type: %s", content.MsgType))
	}
}

// HandleMatrixEdit bridges a Matrix message edit → Mattermost post edit.
func (c *MattermostClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return wrapSendErr(fmt.Errorf("not connected to Mattermost"))
	}

	src := msg.Content
	if msg.Content.NewContent != nil {
		src = msg.Content.NewContent
	}
	var newBody string
	if src.FormattedBody != "" {
		newBody = format.HTMLToMarkdown(src.FormattedBody)
	} else {
		newBody = src.Body
	}

	return wrapSendErr(mattermost.EditPost(c.serverURL(), c.Token, string(msg.EditTarget.ID), newBody))
}

// HandleMatrixMessageRemove bridges a Matrix redaction → Mattermost post delete.
func (c *MattermostClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return fmt.Errorf("not connected to Mattermost")
	}
	return mattermost.DeletePost(c.serverURL(), c.Token, string(msg.TargetMessage.ID))
}

// PreHandleMatrixReaction validates and extracts the emoji name.
func (c *MattermostClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	emoji := msg.Content.RelatesTo.Key
	return bridgev2.MatrixReactionPreResponse{
		SenderID: networkid.UserID(c.UserID),
		EmojiID:  networkid.EmojiID(emoji),
		Emoji:    emoji,
	}, nil
}

// HandleMatrixReaction adds a reaction on Mattermost.
func (c *MattermostClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return nil, fmt.Errorf("not connected to Mattermost")
	}
	emoji := msg.PreHandleResp.Emoji
	if err := mattermost.AddReaction(c.serverURL(), c.Token, c.UserID, string(msg.TargetMessage.ID), emoji); err != nil {
		return nil, err
	}
	return &database.Reaction{
		EmojiID: msg.PreHandleResp.EmojiID,
		Emoji:   emoji,
	}, nil
}

// HandleMatrixReactionRemove removes a reaction on Mattermost.
func (c *MattermostClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return fmt.Errorf("not connected to Mattermost")
	}
	emoji := msg.TargetReaction.Emoji
	if emoji == "" {
		emoji = string(msg.TargetReaction.EmojiID)
	}
	return mattermost.RemoveReaction(c.serverURL(), c.Token, c.UserID, string(msg.TargetReaction.MessageID), emoji)
}

// HandleMatrixTyping forwards a Matrix typing event to Mattermost.
func (c *MattermostClient) HandleMatrixTyping(ctx context.Context, msg *bridgev2.MatrixTyping) error {
	if !msg.IsTyping {
		return nil // Mattermost has no stop-typing REST endpoint
	}
	c.mu.RLock()
	cl := c.client
	c.mu.RUnlock()
	if cl == nil {
		return nil
	}
	return cl.SendTyping(c.UserID, string(msg.Portal.ID))
}

// --- MIME / filename helpers ---

// mimeFilenames maps MIME types to suitable download filenames.
var mimeFilenames = map[string]string{
	"video/mp4":       "video.mp4",
	"video/webm":      "video.webm",
	"video/quicktime": "video.mov",
	"image/gif":       "image.gif",
	"image/jpeg":      "image.jpg",
	"image/png":       "image.png",
	"image/webp":      "image.webp",
	"audio/mpeg":      "audio.mp3",
	"audio/ogg":       "audio.ogg",
	"audio/opus":      "audio.opus",
}

// filenameForMime returns a clean filename for the given MIME type.
func filenameForMime(mimeType string) string {
	if name, ok := mimeFilenames[mimeType]; ok {
		return name
	}
	return "file"
}

// sniffMimeType detects the MIME type from magic bytes.
func sniffMimeType(data []byte) string {
	if len(data) < 4 {
		return ""
	}
	switch {
	case len(data) >= 6 && data[0] == 'G' && data[1] == 'I' && data[2] == 'F' &&
		data[3] == '8' && (data[4] == '7' || data[4] == '9') && data[5] == 'a':
		return "image/gif"
	case data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF:
		return "image/jpeg"
	case data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47:
		return "image/png"
	case len(data) >= 12 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F' &&
		data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P':
		return "image/webp"
	case len(data) >= 8 && data[4] == 'f' && data[5] == 't' && data[6] == 'y' && data[7] == 'p':
		return "video/mp4"
	case data[0] == 0x1A && data[1] == 0x45 && data[2] == 0xDF && data[3] == 0xA3:
		return "video/webm"
	}
	return ""
}

// wrapSendErr wraps an outbound send error so bridgev2 surfaces a visible notice.
func wrapSendErr(err error) error {
	if err == nil {
		return nil
	}
	return bridgev2.WrapErrorInStatus(err).WithIsCertain(true).WithSendNotice(true)
}
