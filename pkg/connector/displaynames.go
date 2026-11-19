package connector

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Values of network.display_names.
const (
	displayNamesServer   = "server"
	displayNamesUsername = "username"
	displayNamesFullName = "full_name"
	displayNamesNickname = "nickname"
)

// parseDisplayNames validates network.display_names. For a fixed choice it
// returns the Mattermost name format; for "server" it returns "", meaning the
// format is read from Mattermost at connect. Empty means "nickname", the
// bridge's naming before the option existed.
func parseDisplayNames(setting string) (string, error) {
	switch setting {
	case displayNamesServer:
		return "", nil
	case displayNamesUsername:
		return mattermost.NameFormatUsername, nil
	case displayNamesFullName:
		return mattermost.NameFormatFullName, nil
	case "", displayNamesNickname:
		return mattermost.NameFormatNicknameFullName, nil
	}
	return "", fmt.Errorf("unknown value %q (want server, username, full_name or nickname)", setting)
}

// knownNameFormat reports whether f is a format Mattermost defines.
func knownNameFormat(f string) bool {
	switch f {
	case mattermost.NameFormatUsername, mattermost.NameFormatFullName, mattermost.NameFormatNicknameFullName:
		return true
	}
	return false
}

// displayNamesSetting returns network.display_names for this bridge.
func (c *MattermostClient) displayNamesSetting() string {
	if c.displayNamesFn != nil {
		return c.displayNamesFn()
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil {
		return ""
	}
	if conn, ok := c.UserLogin.Bridge.Network.(*MattermostConnector); ok && conn.Config != nil {
		return conn.Config.DisplayNames
	}
	return ""
}

// resolveNameFormat decides the name format for this connection and stores
// it. With "server", the logged-in user's own preference wins unless the
// server locks the setting, as in Mattermost's own clients. If the server
// can't be asked, the last known format is kept (username before the first
// success, Mattermost's default) and the failure is logged.
func (c *MattermostClient) resolveNameFormat(log zerolog.Logger) {
	fixed, err := parseDisplayNames(c.displayNamesSetting())
	if err != nil {
		// Start rejects invalid values; this only guards direct callers.
		log.Error().Err(err).Msg("Invalid network.display_names; using usernames")
		fixed = mattermost.NameFormatUsername
	}
	format := fixed
	if format == "" {
		format = c.serverNameFormat(log)
	}
	if format == "" {
		return
	}
	c.mu.Lock()
	changed := c.nameFormat != format
	c.nameFormat = format
	c.mu.Unlock()
	if changed {
		log.Info().Str("setting", c.displayNamesSetting()).Str("name_format", format).Msg("Naming Mattermost users")
	}
}

// serverNameFormat is the format Mattermost shows the logged-in user, or ""
// if it could not be determined.
func (c *MattermostClient) serverNameFormat(log zerolog.Logger) string {
	settings, err := mattermost.GetNameDisplaySettings(c.serverURL(), c.Token)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to read the server's name display setting; keeping the current names")
		return ""
	}
	if !settings.Locked {
		pref, err := mattermost.GetNameFormatPreference(c.serverURL(), c.Token)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to read the name display preference; using the server default")
		} else if knownNameFormat(pref) {
			return pref
		}
	}
	if knownNameFormat(settings.Default) {
		return settings.Default
	}
	return mattermost.NameFormatUsername
}

// displayName is a Mattermost user's name in Matrix under the active format.
// Every ghost name, DM room name, mention pill and quote author uses it.
func (c *MattermostClient) displayName(u *mattermost.User) string {
	c.mu.RLock()
	format := c.nameFormat
	c.mu.RUnlock()
	if format == "" {
		format = mattermost.NameFormatUsername
	}
	return u.NameFor(format)
}
