package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchImage_Success(t *testing.T) {
	want := []byte{0x89, 0x50, 0x4E, 0x47}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()

	got, err := fetchImage(context.Background(), srv.URL+"/img.png", "")
	if err != nil {
		t.Fatalf("fetchImage error: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("fetchImage body mismatch: got %v, want %v", got, want)
	}
}

func TestFetchImage_WithToken(t *testing.T) {
	want := []byte{0x01, 0x02}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer mytoken" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()

	got, err := fetchImage(context.Background(), srv.URL+"/img.png", "mytoken")
	if err != nil {
		t.Fatalf("fetchImage error: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("fetchImage body mismatch: got %v, want %v", got, want)
	}
}

func TestFetchImage_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := fetchImage(context.Background(), srv.URL+"/missing.png", "")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}

func TestFetchImage_InvalidURL(t *testing.T) {
	_, err := fetchImage(context.Background(), "://invalid", "")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}
