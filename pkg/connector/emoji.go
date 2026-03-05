package connector

import (
	"strings"

	kEmoji "github.com/kyokomi/emoji/v2"
)

// unicodeToName maps Unicode emoji characters back to their Mattermost shortcode
// names (e.g. "😂" → "joy"). Built once at init from the kyokomi code map.
var unicodeToName map[string]string

func init() {
	unicodeToName = make(map[string]string, len(kEmoji.CodeMap()))
	for code, ch := range kEmoji.CodeMap() {
		// keys are ":joy:", values are "😂"
		name := strings.Trim(code, ":")
		if _, exists := unicodeToName[ch]; !exists {
			unicodeToName[ch] = name
		}
	}
}

// mmEmojiToUnicode converts a Mattermost emoji name (e.g. "joy") to its Unicode
// character (e.g. "😂"). Returns name unchanged if not found.
func mmEmojiToUnicode(name string) string {
	if ch, ok := kEmoji.CodeMap()[":"+name+":"]; ok {
		return ch
	}
	return name
}

// unicodeToMMEmoji converts a Unicode emoji character (e.g. "😂") to its
// Mattermost shortcode name (e.g. "joy"). Returns ch unchanged if not found.
func unicodeToMMEmoji(ch string) string {
	if name, ok := unicodeToName[ch]; ok {
		return name
	}
	return ch
}

// replaceEmojiShortcodes replaces :shortcode: tokens in s with their Unicode
// equivalents, matching Mattermost's own emoji rendering.
func replaceEmojiShortcodes(s string) string {
	return kEmoji.Emojize(s)
}
