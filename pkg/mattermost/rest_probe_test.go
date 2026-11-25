package mattermost

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type probeTestTransport func(*http.Request) (*http.Response, error)

func (f probeTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOrdinaryGetAcceptsResponseWithoutRequest(t *testing.T) {
	old := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = old })
	http.DefaultTransport = probeTestTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"post"}`))}, nil
	})
	post, err := GetPost("https://mm.example.com", "test-token", "post")
	if err != nil || post.ID != "post" {
		t.Fatalf("post=%+v err=%v", post, err)
	}
}

func TestOrdinaryGetsFollowRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/final/") {
			http.Redirect(w, r, "/final"+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}
		fmt.Fprint(w, `{"id":"object","username":"alice"}`)
	}))
	defer srv.Close()
	for name, call := range map[string]func() error{
		"post":    func() error { _, e := GetPost(srv.URL, "token", "post"); return e },
		"team":    func() error { _, e := GetTeam(srv.URL, "token", "team"); return e },
		"channel": func() error { _, e := GetChannel(srv.URL, "token", "channel"); return e },
		"self":    func() error { _, _, e := GetSelf(srv.URL, "token"); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeletionProbeDoesNotFollowRedirects(t *testing.T) {
	followed := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/final/") {
			followed++
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"id":"app.channel.get.existing.app_error","status_code":404}`)
			return
		}
		http.Redirect(w, r, "/final"+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	for name, call := range map[string]func() error{
		"channel": func() error { _, e := GetChannelContext(context.Background(), srv.URL, "token", "channel"); return e },
		"self":    func() error { _, _, e := GetSelfContext(context.Background(), srv.URL, "token"); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); err == nil {
				t.Fatal("redirected probe accepted")
			}
		})
	}
	if followed != 0 {
		t.Fatalf("probe followed %d redirects; must refuse before contacting redirect target", followed)
	}
}
