package mattermost

import (
	"strings"
	"testing"
)

func TestDescribeFrameNeverLeaksPayload(t *testing.T) {
	cases := []struct{ name, raw, want string }{
		{"event", `{"event":"posted","seq":7,"data":{"post":"{\"message\":\"SECRET-BODY\"}"}}`, "event=posted seq=7"},
		{"auth ok", `{"status":"OK","seq_reply":1}`, `status="OK" seq_reply=1`},
		{"auth fail", `{"status":"FAIL","seq_reply":1,"error":{"message":"SECRET-ERR"}}`, `status="FAIL" seq_reply=1`},
		{"garbage", `not json`, "unparsable"},
	}
	for _, tc := range cases {
		got := describeFrame([]byte(tc.raw))
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q does not contain %q", tc.name, got, tc.want)
		}
		if strings.Contains(got, "SECRET") {
			t.Errorf("%s: payload leaked into %q", tc.name, got)
		}
	}
}
