package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// fetchImage downloads bytes from url with a 30-second timeout.
// ctx is forwarded to the request for cancellation support.
// An optional bearer token may be provided for authenticated endpoints.
// Returns an error for non-2xx responses.
func fetchImage(ctx context.Context, url, token string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, url)
	}
	return io.ReadAll(resp.Body)
}
