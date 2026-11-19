package connector

import (
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2/status"
)

func TestWebsocketDisconnectIsTransient(t *testing.T) {
	state := websocketDisconnectedState()
	if state.StateEvent != status.StateTransientDisconnect {
		t.Fatalf("a dropped websocket reports %s; bridgev2 shows that at once, want %s",
			state.StateEvent, status.StateTransientDisconnect)
	}
	if state.Error == "" {
		t.Error("state carries no error code")
	}
}

func TestConnectFailureEscalatesAfterRetries(t *testing.T) {
	err := errors.New("dial tcp: connection refused")
	for failures := 1; failures <= maxTransientConnectFailures; failures++ {
		if got := connectFailedState(failures, err).StateEvent; got != status.StateTransientDisconnect {
			t.Errorf("failure %d reports %s, want %s", failures, got, status.StateTransientDisconnect)
		}
	}
	for _, failures := range []int{maxTransientConnectFailures + 1, maxTransientConnectFailures + 20} {
		if got := connectFailedState(failures, err).StateEvent; got != status.StateUnknownError {
			t.Errorf("failure %d reports %s; a persistent outage must surface as %s", failures, got, status.StateUnknownError)
		}
	}
	if msg := connectFailedState(1, err).Message; msg != err.Error() {
		t.Errorf("message = %q, want the connection error", msg)
	}
}

func TestConnectionErrorsHaveHumanText(t *testing.T) {
	for _, code := range []status.BridgeStateErrorCode{
		websocketDisconnectedState().Error,
		connectFailedState(1, errors.New("x")).Error,
	} {
		if status.BridgeStateHumanErrors[code] == "" {
			t.Errorf("error code %q has no human-readable text", code)
		}
	}
}
