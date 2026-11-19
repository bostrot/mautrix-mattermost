package mattermost

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

// ErrUnauthorized is returned (wrapped) when Mattermost rejects the token with HTTP 401,
// e.g. because the session expired or was revoked.
var ErrUnauthorized = errors.New("mattermost: unauthorized")

// normalizeURL trims any trailing slashes from a server URL.
func normalizeURL(serverURL string) string {
	return strings.TrimRight(serverURL, "/")
}

// LoginWithPassword authenticates with email + password and returns the session token.
func LoginWithPassword(serverURL, email, password string) (string, error) {
	serverURL = normalizeURL(serverURL)
	body := map[string]string{
		"login_id": email,
		"password": password,
	}
	jsonBody, _ := json.Marshal(body)

	resp, err := newClient().Post(serverURL+"/api/v4/users/login", "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("login failed (HTTP %d) at %s: %s", resp.StatusCode, serverURL+"/api/v4/users/login", string(body))
	}

	token := resp.Header.Get("Token")
	if token == "" {
		return "", fmt.Errorf("server did not return a session token")
	}
	return token, nil
}

// GetSelf returns the authenticated user's ID and username by calling /api/v4/users/me.
func GetSelf(serverURL, token string) (id, username string, err error) {
	var u User
	if err = doGet(serverURL, token, "/api/v4/users/me", &u); err != nil {
		return
	}
	return u.ID, u.Username, nil
}

// GetUser fetches a user profile by ID.
func GetUser(serverURL, token, userID string) (*User, error) {
	var u User
	if err := doGet(serverURL, token, "/api/v4/users/"+userID, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// NameDisplaySettings is how the server names users: its default
// (TeammateNameDisplay) and whether users may override it.
type NameDisplaySettings struct {
	Default string
	Locked  bool
}

// GetNameDisplaySettings reads the server's name display settings from the
// client config, which any logged-in user may read.
func GetNameDisplaySettings(serverURL, token string) (NameDisplaySettings, error) {
	var cfg map[string]string
	if err := doGet(serverURL, token, "/api/v4/config/client?format=old", &cfg); err != nil {
		return NameDisplaySettings{}, err
	}
	return NameDisplaySettings{
		Default: cfg["TeammateNameDisplay"],
		Locked:  cfg["LockTeammateNameDisplay"] == "true",
	}, nil
}

// GetNameFormatPreference returns the logged-in user's own name display
// preference, or "" when they have not set one. Mattermost answers an unset
// preference with app.preference.get.app_error (HTTP 400 on 12.x, 404 on
// older servers); any other failure is returned.
func GetNameFormatPreference(serverURL, token string) (string, error) {
	const path = "/api/v4/users/me/preferences/display_settings/name/name_format"
	req, err := http.NewRequest("GET", normalizeURL(serverURL)+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := newClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var apiErr struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(body, &apiErr)
		if apiErr.ID == "app.preference.get.app_error" || resp.StatusCode == http.StatusNotFound {
			return "", nil
		}
		return "", fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, path, string(body))
	}
	var pref struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &pref); err != nil {
		return "", fmt.Errorf("decode error from %s: %w", path, err)
	}
	return pref.Value, nil
}

// GetUsersByIDs fetches the profiles for a list of user IDs in one request.
// IDs with no matching user are simply absent from the result.
func GetUsersByIDs(serverURL, token string, userIDs []string) ([]*User, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	var out []*User
	if err := doPost(serverURL, token, "/api/v4/users/ids", userIDs, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetUsersByUsernames fetches the profiles for a list of usernames in one
// request. Usernames with no matching user are simply absent from the result.
func GetUsersByUsernames(serverURL, token string, usernames []string) ([]*User, error) {
	if len(usernames) == 0 {
		return nil, nil
	}
	var out []*User
	if err := doPost(serverURL, token, "/api/v4/users/usernames", usernames, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetTeams returns the list of teams the authenticated user belongs to.
func GetTeams(serverURL, token, userID string) ([]*Team, error) {
	var teams []*Team
	if err := doGet(serverURL, token, "/api/v4/users/"+userID+"/teams", &teams); err != nil {
		return nil, err
	}
	return teams, nil
}

// GetChannelsForUser returns all channels the user is a member of in a given team.
// This includes public, private, DM and group channels.
func GetChannelsForUser(serverURL, token, userID, teamID string) ([]*Channel, error) {
	var channels []*Channel
	path := fmt.Sprintf("/api/v4/users/%s/teams/%s/channels", userID, teamID)
	if err := doGet(serverURL, token, path, &channels); err != nil {
		return nil, err
	}
	return channels, nil
}

// GetChannel fetches a single channel by ID.
// GetPost fetches a single post by ID. It is used to build previews for
// permalinks that were pasted into a message.
func GetPost(serverURL, token, postID string) (*Post, error) {
	var p Post
	if err := doGet(serverURL, token, "/api/v4/posts/"+postID, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func GetChannel(serverURL, token, channelID string) (*Channel, error) {
	var ch Channel
	if err := doGet(serverURL, token, "/api/v4/channels/"+channelID, &ch); err != nil {
		return nil, err
	}
	return &ch, nil
}

// GetChannelMembers returns the member list for a channel (up to 200 per page).
func GetChannelMembers(serverURL, token, channelID string) ([]*ChannelMember, error) {
	var members []*ChannelMember
	path := fmt.Sprintf("/api/v4/channels/%s/members?per_page=200", channelID)
	if err := doGet(serverURL, token, path, &members); err != nil {
		return nil, err
	}
	return members, nil
}

// GetPosts fetches posts from a channel. beforePostID paginates backwards.
// Returns posts ordered newest-first.
func GetPosts(serverURL, token, channelID string, perPage int, beforePostID string) (*PostList, error) {
	path := fmt.Sprintf("/api/v4/channels/%s/posts?per_page=%d", channelID, perPage)
	if beforePostID != "" {
		path += "&before=" + beforePostID
	}
	var pl PostList
	if err := doGet(serverURL, token, path, &pl); err != nil {
		return nil, err
	}
	return &pl, nil
}

// GetFileInfo returns metadata for a single file attachment.
func GetFileInfo(serverURL, token, fileID string) (*FileInfo, error) {
	var fi FileInfo
	if err := doGet(serverURL, token, "/api/v4/files/"+fileID+"/info", &fi); err != nil {
		return nil, err
	}
	return &fi, nil
}

// UploadFile uploads raw file bytes to a channel and returns the file ID.
func UploadFile(serverURL, token, channelID string, data []byte, filename, mimeType string) (string, error) {
	serverURL = normalizeURL(serverURL)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// channelId form field
	if err := w.WriteField("channel_id", channelID); err != nil {
		return "", err
	}

	// file part with proper content-type header
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="files"; filename="%s"`, filename))
	h.Set("Content-Type", mimeType)
	part, err := w.CreatePart(h)
	if err != nil {
		return "", err
	}
	if _, err = part.Write(data); err != nil {
		return "", err
	}
	w.Close()

	req, err := http.NewRequest("POST", serverURL+"/api/v4/files", &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", w.FormDataContentType())

	resp, err := newClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		FileInfos []*FileInfo `json:"file_infos"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if len(result.FileInfos) == 0 {
		return "", fmt.Errorf("upload returned no file infos")
	}
	return result.FileInfos[0].ID, nil
}

// ProfileImageURL returns the URL for a user's profile picture.
func ProfileImageURL(serverURL, userID string) string {
	return normalizeURL(serverURL) + "/api/v4/users/" + userID + "/image"
}

// --- internal helpers ---

func newClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func doGet(serverURL, token, path string, out any) error {
	req, err := http.NewRequest("GET", normalizeURL(serverURL)+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := newClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: HTTP 401 from %s: %s", ErrUnauthorized, path, string(body))
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, path, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode error from %s: %w", path, err)
	}
	return nil
}

// ViewChannel marks a channel read for the token's user, as opening it in
// Mattermost does (POST /channels/members/me/view).
func ViewChannel(serverURL, token, channelID string) error {
	body := map[string]any{"channel_id": channelID, "prev_channel_id": "", "collapsed_threads_supported": true}
	return doPost(serverURL, token, "/api/v4/channels/members/me/view", body, &struct{}{})
}

// MarkThreadRead marks a collapsed reply thread read up to ts (milliseconds)
// for the user. Mattermost keys thread state by team; DM and group threads
// accept any team the user belongs to.
func MarkThreadRead(serverURL, token, userID, teamID, threadID string, ts int64) error {
	path := fmt.Sprintf("/api/v4/users/%s/teams/%s/threads/%s/read/%d", userID, teamID, threadID, ts)
	return doPut(serverURL, token, path, nil)
}

func doPost(serverURL, token, path string, body any, out any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", normalizeURL(serverURL)+path, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := newClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, path, string(errBody))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func doPut(serverURL, token, path string, body any) error {
	return doMethod("PUT", serverURL, token, path, body)
}

func doDelete(serverURL, token, path string) error {
	return doMethod("DELETE", serverURL, token, path, nil)
}

func doMethod(method, serverURL, token, path string, body any) error {
	serverURL = normalizeURL(serverURL)
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}
	req, err := http.NewRequest(method, serverURL+path, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := newClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, path, string(errBody))
	}
	return nil
}
