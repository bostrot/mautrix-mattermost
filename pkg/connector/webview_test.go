package connector

import (
	"context"
	"regexp"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestWebviewCookieStep(t *testing.T) {
	step, serverURL, err := webviewCookieStep(" https://mm.example.com/ ")
	if err != nil {
		t.Fatal(err)
	}
	if serverURL != "https://mm.example.com" {
		t.Errorf("serverURL = %q", serverURL)
	}
	if step.Type != bridgev2.LoginStepTypeCookies {
		t.Errorf("type = %q", step.Type)
	}
	p := step.CookiesParams
	if p.URL != "https://mm.example.com/login" {
		t.Errorf("url = %q", p.URL)
	}
	src := p.Fields[0].Sources[0]
	if p.Fields[0].ID != "token" || src.Name != "MMAUTHTOKEN" || src.CookieDomain != "mm.example.com" {
		t.Errorf("unexpected field %+v / %+v", p.Fields[0], src)
	}

	re := regexp.MustCompile(p.WaitForURLPattern)
	for _, u := range []string{
		"https://mm.example.com/myteam/channels/town-square",
		"https://mm.example.com/myteam/messages/@alice",
	} {
		if !re.MatchString(u) {
			t.Errorf("pattern should match %q", u)
		}
	}
	for _, u := range []string{
		"https://mm.example.com/login",
		"https://mm.example.com/oauth/github/complete",
		"https://evil.example/mm.example.com/myteam/channels/x",
	} {
		if re.MatchString(u) {
			t.Errorf("pattern should not match %q", u)
		}
	}
}

func TestWebviewCookieStep_KeepsSubpath(t *testing.T) {
	_, serverURL, err := webviewCookieStep("https://example.com/mattermost/")
	if err != nil || serverURL != "https://example.com/mattermost" {
		t.Errorf("serverURL = %q, err = %v", serverURL, err)
	}
}

func TestWebviewStartWithOverride(t *testing.T) {
	p := &WebviewLoginProcess{}
	old := &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		Metadata: &UserLoginMetadata{ServerURL: "https://mm.example.com"},
	}}
	step, err := p.StartWithOverride(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	if step.Type != bridgev2.LoginStepTypeCookies || p.serverURL != "https://mm.example.com" {
		t.Errorf("type = %q, serverURL = %q; want cookies step for stored server", step.Type, p.serverURL)
	}

	// No stored server URL: fall back to asking for it.
	p = &WebviewLoginProcess{}
	old = &bridgev2.UserLogin{UserLogin: &database.UserLogin{Metadata: &UserLoginMetadata{}}}
	step, err = p.StartWithOverride(context.Background(), old)
	if err != nil || step.Type != bridgev2.LoginStepTypeUserInput {
		t.Errorf("type = %q, err = %v; want user input step", step.Type, err)
	}
}

func TestWebviewCookieStep_InvalidURL(t *testing.T) {
	for _, in := range []string{"", "mm.example.com", "ftp://mm.example.com", "https://"} {
		if _, _, err := webviewCookieStep(in); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}
