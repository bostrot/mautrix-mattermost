package connector

import "testing"

// TestUnicodeToMMEmoji verifies reactions map to names Mattermost accepts.
func TestUnicodeToMMEmoji(t *testing.T) {
	cases := map[string]string{
		"👍":  "+1",
		"👍️": "+1", // with U+FE0F variation selector
		"😂":  "joy",
		"❤️": "heart",
	}
	for in, want := range cases {
		if got := unicodeToMMEmoji(in); got != want {
			t.Errorf("unicodeToMMEmoji(%q) = %q, want %q", in, got, want)
		}
	}
}
