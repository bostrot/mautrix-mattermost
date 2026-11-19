package connector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// nameServer serves the client config and the user's name_format preference.
type nameServer struct {
	*httptest.Server
	mu         sync.Mutex
	def        string // TeammateNameDisplay
	locked     bool
	pref       string // "" = no preference set (404)
	failConfig bool
	failPref   bool
	prefHits   int
}

func newNameServer(t *testing.T) *nameServer {
	t.Helper()
	ns := &nameServer{def: mattermost.NameFormatUsername}
	ns.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v4/config/client":
			if ns.failConfig || r.URL.Query().Get("format") != "old" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			locked := "false"
			if ns.locked {
				locked = "true"
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"TeammateNameDisplay": ns.def, "LockTeammateNameDisplay": locked})
		case "/api/v4/users/me/preferences/display_settings/name/name_format":
			ns.prefHits++
			if ns.failPref {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if ns.pref == "" {
				// What Mattermost 12 sends for an unset preference.
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"id":"app.preference.get.app_error","message":"We encountered an error while finding preferences.","status_code":400}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"category": "display_settings", "name": "name_format", "value": ns.pref})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ns.Close)
	return ns
}

func nameClient(srv *nameServer, setting string) *MattermostClient {
	return &MattermostClient{Token: "tok", baseURL: srv.URL, displayNamesFn: func() string { return setting }}
}

var (
	ssoUser  = &mattermost.User{ID: "u1", Username: "dee", FirstName: "Dee"}
	fullUser = &mattermost.User{ID: "u2", Username: "alice", FirstName: "Alice", LastName: "Example"}
)

func TestDisplayNamesSettings(t *testing.T) {
	cases := []struct {
		name          string
		setting       string
		def, pref     string
		locked        bool
		wantSSO, want string // names for ssoUser and fullUser
	}{
		{"server default is username", "server", "username", "", false, "dee", "alice"},
		{"empty setting means server", "", "full_name", "", false, "Dee", "Alice Example"},
		{"server full names", "server", "full_name", "", false, "Dee", "Alice Example"},
		{"own preference overrides the server", "server", "username", "full_name", false, "Dee", "Alice Example"},
		{"locked server ignores the preference", "server", "username", "full_name", true, "dee", "alice"},
		{"unknown server value falls back to username", "server", "weird", "", false, "dee", "alice"},
		{"fixed username ignores the server", "username", "full_name", "full_name", false, "dee", "alice"},
		{"fixed full_name", "full_name", "username", "", false, "Dee", "Alice Example"},
		{"fixed nickname", "nickname", "username", "", false, "Dee", "Alice Example"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newNameServer(t)
			srv.def, srv.pref, srv.locked = tc.def, tc.pref, tc.locked
			c := nameClient(srv, tc.setting)
			c.resolveNameFormat(zerolog.Nop())
			if got := c.displayName(ssoUser); got != tc.wantSSO {
				t.Errorf("SSO user = %q, want %q", got, tc.wantSSO)
			}
			if got := c.displayName(fullUser); got != tc.want {
				t.Errorf("full-name user = %q, want %q", got, tc.want)
			}
			if got := *c.userInfo(fullUser).Name; got != tc.want {
				t.Errorf("ghost name = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFixedDisplayNamesMakeNoRequests(t *testing.T) {
	srv := newNameServer(t)
	srv.failConfig = true
	c := nameClient(srv, "full_name")
	c.resolveNameFormat(zerolog.Nop())
	if c.displayName(fullUser) != "Alice Example" {
		t.Fatal("fixed setting depended on the server")
	}
}

func TestDisplayNamesServerFailureKeepsCurrentFormat(t *testing.T) {
	srv := newNameServer(t)
	srv.def = mattermost.NameFormatFullName
	c := nameClient(srv, "server")
	c.resolveNameFormat(zerolog.Nop())
	if c.displayName(fullUser) != "Alice Example" {
		t.Fatal("setup: full names not applied")
	}
	srv.mu.Lock()
	srv.failConfig = true
	srv.mu.Unlock()
	c.resolveNameFormat(zerolog.Nop())
	if got := c.displayName(fullUser); got != "Alice Example" {
		t.Fatalf("a failed lookup changed the format: %q", got)
	}
	// Before any success, Mattermost's default (username) is used.
	fresh := nameClient(srv, "server")
	fresh.resolveNameFormat(zerolog.Nop())
	if got := fresh.displayName(fullUser); got != "alice" {
		t.Fatalf("unresolved format named the user %q, want the username", got)
	}
}

func TestDisplayNamesPreferenceFailureUsesServerDefault(t *testing.T) {
	srv := newNameServer(t)
	srv.def, srv.failPref = mattermost.NameFormatFullName, true
	c := nameClient(srv, "server")
	c.resolveNameFormat(zerolog.Nop())
	if got := c.displayName(fullUser); got != "Alice Example" {
		t.Fatalf("got %q, want the server default", got)
	}
}

func TestLockedServerSkipsPreferenceRequest(t *testing.T) {
	srv := newNameServer(t)
	srv.locked = true
	nameClient(srv, "server").resolveNameFormat(zerolog.Nop())
	if srv.prefHits != 0 {
		t.Fatalf("preference requested %d times on a locked server", srv.prefHits)
	}
}

// An unset display_names keeps the bridge's original naming: nickname, then
// full name, then username.
func TestUnsetDisplayNamesIsNickname(t *testing.T) {
	got, err := parseDisplayNames("")
	if err != nil || got != mattermost.NameFormatNicknameFullName {
		t.Fatalf("parseDisplayNames(\"\") = %q, %v; want %q", got, err, mattermost.NameFormatNicknameFullName)
	}
}

func TestInvalidDisplayNamesFailsStart(t *testing.T) {
	for _, ok := range []string{"", "server", "username", "full_name", "nickname"} {
		m := &MattermostConnector{Config: &Config{DisplayNames: ok}}
		if err := m.Start(nil); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	m := &MattermostConnector{Config: &Config{DisplayNames: "fullname"}}
	err := m.Start(nil)
	if err == nil || !strings.Contains(err.Error(), "display_names") {
		t.Fatalf("typo accepted or unclear error: %v", err)
	}
}

func TestMentionPillsUseActiveFormat(t *testing.T) {
	srv := newMentionServer(t)
	c := newMentionClient(srv)
	c.nameFormat = mattermost.NameFormatUsername
	got := c.renderPostText(t.Context(), "@alice look")
	if !strings.Contains(got.FormattedBody, ">alice</a>") {
		t.Fatalf("pill text = %q, want the username", got.FormattedBody)
	}
}

func TestPlainMentionMatchesAnyNameForm(t *testing.T) {
	// A ghost may still carry a name from an earlier setting.
	srv := newMentionServer(t)
	c := newMentionClient(srv)
	c.nameFormat = mattermost.NameFormatUsername
	c.ghostNameFn = func(string) string { return "" }
	for _, body := range []string{"@Alice Example hi", "@alice hi"} {
		content := plainMention(body, ghostMXID("ualice"))
		if got := c.outboundMarkdown(t.Context(), &content); got != "@alice hi" {
			t.Errorf("%q -> %q", body, got)
		}
	}
}

func TestExampleConfigsDocumentDisplayNames(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(ExampleConfig), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.DisplayNames != displayNamesNickname {
		t.Errorf("embedded example display_names = %q, want nickname", cfg.DisplayNames)
	}
	var file struct {
		Network Config `yaml:"network"`
	}
	if err := yaml.Unmarshal([]byte(mustReadExampleConfig(t)), &file); err != nil {
		t.Fatal(err)
	}
	if file.Network.DisplayNames != displayNamesNickname {
		t.Errorf("example-config.yaml display_names = %q, want nickname", file.Network.DisplayNames)
	}
}

func TestDisplayNamesSurvivesConfigUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("network:\n    display_names: full_name\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	example, _, networkUpgrader := (&MattermostConnector{}).GetConfig()
	var base strings.Builder
	base.WriteString("network:\n")
	for _, line := range strings.Split(example, "\n") {
		base.WriteString("    " + line + "\n")
	}
	upgrader := configupgrade.MergeUpgraders(base.String(),
		&configupgrade.ProxyUpgrader{Target: networkUpgrader, Prefix: []string{"network"}})
	data, _, err := configupgrade.Do(path, false, upgrader)
	if err != nil {
		t.Fatal(err)
	}
	var full struct {
		Network Config `yaml:"network"`
	}
	if err := yaml.Unmarshal(data, &full); err != nil {
		t.Fatal(err)
	}
	if full.Network.DisplayNames != "full_name" {
		t.Fatalf("display_names lost in upgrade:\n%s", data)
	}
}

func TestFixedFormatsWithNickname(t *testing.T) {
	nick := &mattermost.User{ID: "u3", Username: "bob", FirstName: "Robert", LastName: "Example", Nickname: "Bobby"}
	srv := newNameServer(t)
	for setting, want := range map[string]string{"username": "bob", "full_name": "Robert Example", "nickname": "Bobby"} {
		c := nameClient(srv, setting)
		c.resolveNameFormat(zerolog.Nop())
		if got := c.displayName(nick); got != want {
			t.Errorf("%s: %q, want %q", setting, got, want)
		}
	}
}

func TestNameFormatPreferenceRequests(t *testing.T) {
	srv := newNameServer(t)
	pref, err := mattermost.GetNameFormatPreference(srv.URL, "tok")
	if err != nil || pref != "" {
		t.Fatalf("no preference set: got %q, %v; want \"\" and no error", pref, err)
	}
	srv.pref = mattermost.NameFormatFullName
	if pref, err = mattermost.GetNameFormatPreference(srv.URL, "tok"); err != nil || pref != "full_name" {
		t.Fatalf("got %q, %v", pref, err)
	}
	srv.failPref = true // a 500 with no Mattermost error id
	if _, err = mattermost.GetNameFormatPreference(srv.URL, "tok"); err == nil {
		t.Fatal("a server error must be returned, not read as \"no preference\"")
	}
}

func TestUnsetPreferenceIsNotAWarning(t *testing.T) {
	srv := newNameServer(t)
	srv.def = mattermost.NameFormatFullName
	var buf strings.Builder
	c := nameClient(srv, "server")
	c.resolveNameFormat(zerolog.New(&buf))
	if strings.Contains(buf.String(), `"level":"warn"`) {
		t.Fatalf("an unset preference logged a warning:\n%s", buf.String())
	}
	if c.displayName(fullUser) != "Alice Example" {
		t.Fatal("server default not applied")
	}
}

func mustReadExampleConfig(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("../../example-config.yaml")
	if err != nil {
		t.Fatalf("read example-config.yaml: %v", err)
	}
	return string(body)
}
