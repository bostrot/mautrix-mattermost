package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/bostrot/mautrix-mattermost/pkg/connector"
)

// Build metadata injected at link time via -X linker flags.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	m := mxmain.BridgeMain{
		Name:        "mautrix-mattermost",
		Description: "A Matrix-Mattermost bridge",
		URL:         "https://github.com/bostrot/mautrix-mattermost",
		Version:     "0.1.0",
		Connector:   &connector.MattermostConnector{},
	}
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
