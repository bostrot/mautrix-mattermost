package connector

import (
	"maunium.net/go/mautrix/bridgev2/status"
)

// Connection-state reporting follows the official mautrix bridges (Discord,
// Meta, Slack, Signal): a dropped or failed connection that the connector is
// already retrying is TRANSIENT_DISCONNECT, not UNKNOWN_ERROR.
//
// The difference matters to the user. bridgev2 treats UNKNOWN_ERROR as an
// error: clients show it at once, and it schedules a forced reconnect.
// TRANSIENT_DISCONNECT is held back, and only raised if it is still unresolved
// after bridgev2.TransientDisconnectNoticeDelay. A websocket that a proxy resets
// and that reconnects within seconds should not flash an error in Beeper.

// maxTransientConnectFailures is how many consecutive failed connection
// attempts are reported as transient before escalating to UNKNOWN_ERROR. With
// the connect loop's doubling backoff from 5 s, six attempts span about five
// minutes. The Discord bridge uses the same count.
const maxTransientConnectFailures = 6

func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		"mm-websocket-disconnected": "Disconnected from Mattermost, reconnecting",
		"mm-connect-failed":         "Failed to connect to Mattermost",
	})
}

// websocketDisconnectedState is reported when an established websocket closes.
// The connect loop is already reconnecting, so it is transient.
func websocketDisconnectedState() status.BridgeState {
	return status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      "mm-websocket-disconnected",
		Message:    "WebSocket connection closed, reconnecting",
	}
}

// connectFailedState is reported when a connection attempt fails. failures is
// the number of consecutive failed attempts, including this one.
func connectFailedState(failures int, err error) status.BridgeState {
	state := status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      "mm-connect-failed",
		Message:    err.Error(),
	}
	if failures > maxTransientConnectFailures {
		state.StateEvent = status.StateUnknownError
	}
	return state
}
