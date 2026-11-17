package connector

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

// Match the existing archive policy: members below moderator level cannot send.
// Higher administrator restrictions are retained.
const archiveLockLevel = 50

const archiveJournalKey = "com.pugetaudio.archive_lock"
const archiveMarkerKey = "com.pugetaudio.archive_lock_token"

// PortalMetadata retains fields owned by other versions/features verbatim.
type PortalMetadata map[string]json.RawMessage

type archiveChange struct {
	Before  json.RawMessage `json:"before,omitempty"` // nil means absent, including events_default
	Applied json.RawMessage `json:"applied"`
}
type archiveJournal struct {
	Version      int                      `json:"version"`
	Phase        string                   `json:"phase"`
	Token        string                   `json:"token"`
	Changes      map[string]archiveChange `json:"changes"`
	MarkerBefore json.RawMessage          `json:"marker_before,omitempty"`
}

type archiveState struct {
	Content   map[string]json.RawMessage
	OwnSender bool
}
type archiveStore interface {
	Read(context.Context) (archiveState, error)
	Write(context.Context, map[string]json.RawMessage) error
	Load() (*archiveJournal, error)
	Save(context.Context, *archiveJournal) error
}

type portalArchiveStore struct{ p *bridgev2.Portal }

func (s *portalArchiveStore) Read(ctx context.Context) (archiveState, error) {
	if s.p.MXID == "" {
		return archiveState{}, fmt.Errorf("portal has no Matrix room")
	}
	mx, ok := s.p.Bridge.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState)
	if !ok {
		return archiveState{}, fmt.Errorf("Matrix connector cannot read raw room state")
	}
	evt, err := mx.GetStateEvent(ctx, s.p.MXID, event.StatePowerLevels, "")
	if err != nil {
		return archiveState{}, err
	}
	if evt == nil {
		return archiveState{}, fmt.Errorf("missing power level event")
	}
	raw := evt.Content.VeryRaw
	if raw == nil {
		return archiveState{}, fmt.Errorf("raw power level state unavailable")
	}
	var content map[string]json.RawMessage
	if err = json.Unmarshal(raw, &content); err != nil {
		return archiveState{}, err
	}
	if content == nil {
		return archiveState{}, fmt.Errorf("invalid power level content")
	}
	return archiveState{content, evt.Sender == s.p.Bridge.Bot.GetMXID()}, nil
}
func (s *portalArchiveStore) Write(ctx context.Context, content map[string]json.RawMessage) error {
	raw, err := json.Marshal(content)
	if err != nil {
		return err
	}
	_, err = s.p.Bridge.Bot.SendState(ctx, s.p.MXID, event.StatePowerLevels, "", &event.Content{VeryRaw: raw}, time.Time{})
	return err
}
func (s *portalArchiveStore) metadata() (*PortalMetadata, error) {
	m, ok := s.p.Metadata.(*PortalMetadata)
	if !ok || m == nil {
		return nil, fmt.Errorf("unexpected portal metadata type %T", s.p.Metadata)
	}
	return m, nil
}
func (s *portalArchiveStore) Load() (*archiveJournal, error) {
	m, err := s.metadata()
	if err != nil {
		return nil, err
	}
	raw, ok := (*m)[archiveJournalKey]
	if !ok {
		return nil, nil
	}
	var j archiveJournal
	if err = json.Unmarshal(raw, &j); err != nil {
		return nil, err
	}
	if j.Version != 1 || (j.Phase != "pending" && j.Phase != "owned" && j.Phase != "restoring") || j.Token == "" {
		return nil, fmt.Errorf("unsupported archive journal")
	}
	for key, change := range j.Changes {
		if key != "events_default" && key != "m.room.message" && key != "m.room.encrypted" {
			return nil, fmt.Errorf("invalid archive journal field")
		}
		var level int
		if json.Unmarshal(change.Applied, &level) != nil || level != archiveLockLevel {
			return nil, fmt.Errorf("invalid applied level")
		}
		if change.Before != nil && parseArchiveLevel(change.Before, &level) != nil {
			return nil, fmt.Errorf("invalid original level")
		}
	}
	return &j, nil
}
func (s *portalArchiveStore) Save(ctx context.Context, j *archiveJournal) error {
	m, err := s.metadata()
	if err != nil {
		return err
	}
	next := make(PortalMetadata, len(*m)+1)
	for k, v := range *m {
		next[k] = v
	}
	if j == nil {
		delete(next, archiveJournalKey)
	} else {
		raw, err := json.Marshal(j)
		if err != nil {
			return err
		}
		next[archiveJournalKey] = raw
	}
	archiveMetaMu.Lock()
	old := *m
	*m = next
	archiveMetaMu.Unlock()
	if err = s.p.Save(ctx); err != nil {
		archiveMetaMu.Lock()
		*m = old
		archiveMetaMu.Unlock()
		return err
	}
	return nil
}

// archiveMetaMu guards swapping a portal's metadata map. The portal lane is the
// only writer, but hasArchiveJournal reads it from the websocket goroutine.
var archiveMetaMu sync.RWMutex

// lenientArchiveFields reads the three fields without validating them. Restore
// must work on a room an administrator has since made odd (a float level, an
// events object of the wrong type): such a value can never equal what the bridge
// applied, so it is simply not ours to touch.
func lenientArchiveFields(content map[string]json.RawMessage) map[string]json.RawMessage {
	fields := map[string]json.RawMessage{"events_default": content["events_default"]}
	var events map[string]json.RawMessage
	if raw, ok := content["events"]; ok {
		_ = json.Unmarshal(raw, &events)
	}
	for _, key := range []string{"m.room.message", "m.room.encrypted"} {
		fields[key] = events[key]
	}
	return fields
}

func archiveFields(content map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{"events_default": content["events_default"]}
	var events map[string]json.RawMessage
	if raw, ok := content["events"]; ok {
		if err := json.Unmarshal(raw, &events); err != nil || events == nil {
			return nil, fmt.Errorf("invalid events power levels")
		}
	}
	for _, key := range []string{"m.room.message", "m.room.encrypted"} {
		fields[key] = events[key]
	}
	for _, raw := range fields {
		if raw != nil {
			var n int
			if err := parseArchiveLevel(raw, &n); err != nil {
				return nil, fmt.Errorf("invalid numeric power level: %w", err)
			}
		}
	}
	return fields, nil
}
func equalRaw(a, b json.RawMessage) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var x, y any
	return decodeArchiveJSON(a, &x) == nil && decodeArchiveJSON(b, &y) == nil && reflect.DeepEqual(x, y)
}

// archiveFields validates the events object before any mutation below.
func setArchiveField(content map[string]json.RawMessage, key string, value json.RawMessage) {
	target := content
	if key != "events_default" {
		target = map[string]json.RawMessage{}
		if content["events"] != nil {
			_ = json.Unmarshal(content["events"], &target)
		}
	}
	if value == nil {
		delete(target, key)
	} else {
		target[key] = value
	}
	if key != "events_default" {
		content["events"], _ = json.Marshal(target)
	}
}

// The portal lane serializes local callers. Matrix has no atomic state CAS:
// an administrator can still race the read/write pair. Verification detects
// observed conflicts, but cannot prevent lost updates in that window.
func reconcileArchiveLock(ctx context.Context, s archiveStore, archived bool) error {
	j, err := s.Load()
	if err != nil {
		return err
	}
	state, err := s.Read(ctx)
	if err != nil {
		return fmt.Errorf("read power levels: %w", err)
	}
	var fields map[string]json.RawMessage
	if j != nil && !archived {
		fields = lenientArchiveFields(state.Content)
	} else if fields, err = archiveFields(state.Content); err != nil {
		return err
	}
	if j != nil && j.Phase == "pending" {
		verr := verifyArchiveOwnership(state, fields, j)
		// A restore never needs the pending write to have been verified: it
		// reverts only fields still equal to what the bridge applied, so it can
		// always make progress instead of wedging on an unverifiable state.
		if verr != nil && archived {
			err = verr
			original := true
			for key, change := range j.Changes {
				original = original && equalRaw(fields[key], change.Before)
			}
			if !original {
				return err
			}
			for key, change := range j.Changes {
				setArchiveField(state.Content, key, change.Applied)
			}
			state.Content[archiveMarkerKey], _ = json.Marshal(j.Token)
			if err = s.Write(ctx, state.Content); err != nil {
				return err
			}
			state, err = s.Read(ctx)
			if err != nil {
				return err
			}
			fields, err = archiveFields(state.Content)
			if err != nil {
				return err
			}
			if err = verifyArchiveOwnership(state, fields, j); err != nil {
				return err
			}
		}
		if archived || verr == nil {
			j.Phase = "owned"
			if err = s.Save(ctx, j); err != nil {
				return err
			}
		}
	}
	if j == nil {
		if !archived {
			return nil
		}
		changes := map[string]archiveChange{}
		for key, before := range fields {
			if before == nil && key != "events_default" {
				continue
			}
			level := 0
			if before != nil {
				_ = json.Unmarshal(before, &level)
			}
			if level < archiveLockLevel {
				changes[key] = archiveChange{before, json.RawMessage(strconv.Itoa(archiveLockLevel))}
			}
		}
		if len(changes) == 0 {
			return nil
		}
		token := make([]byte, 16)
		if _, err = rand.Read(token); err != nil {
			return err
		}
		j = &archiveJournal{Version: 1, Phase: "pending", Token: hex.EncodeToString(token), Changes: changes, MarkerBefore: state.Content[archiveMarkerKey]}
		if err = s.Save(ctx, j); err != nil {
			return err
		}
		for key, change := range changes {
			setArchiveField(state.Content, key, change.Applied)
		}
		state.Content[archiveMarkerKey], _ = json.Marshal(j.Token)
		if err = s.Write(ctx, state.Content); err != nil {
			return fmt.Errorf("archive write (journal retained): %w", err)
		}
		verified, err := s.Read(ctx)
		if err != nil {
			return err
		}
		actual, err := archiveFields(verified.Content)
		if err != nil {
			return err
		}
		if err = verifyArchiveOwnership(verified, actual, j); err != nil {
			return err
		}
		j.Phase = "owned"
		return s.Save(ctx, j)
	}
	if archived && j.Phase == "owned" {
		// An administrator who edits a locked field while the channel is archived
		// takes ownership of it: stop owning it so a restore leaves it alone, and
		// do not fight the edit or re-report it every poll cycle.
		pruned := false
		for key, change := range j.Changes {
			if !equalRaw(fields[key], change.Applied) {
				delete(j.Changes, key)
				pruned = true
			}
		}
		if pruned {
			return s.Save(ctx, j)
		}
		return nil
	}
	// Persist restoration intent before any weakening, including on restart.
	if j.Phase != "restoring" {
		j.Phase = "restoring"
		if err = s.Save(ctx, j); err != nil {
			return err
		}
	}
	// Re-read after the (slow) intent save: the write below resends the whole
	// content, so the snapshot it is built from should be as fresh as possible.
	if state, err = s.Read(ctx); err != nil {
		return fmt.Errorf("read power levels before restore: %w", err)
	}
	fields = lenientArchiveFields(state.Content)
	expected := map[string]json.RawMessage{}
	changed := false
	for key, change := range j.Changes {
		if equalRaw(fields[key], change.Applied) {
			setArchiveField(state.Content, key, change.Before)
			expected[key] = change.Before
			changed = true
		}
	}
	markerApplied, _ := json.Marshal(j.Token)
	restoreMarker := equalRaw(state.Content[archiveMarkerKey], markerApplied)
	if restoreMarker {
		setArchiveFieldRaw(state.Content, archiveMarkerKey, j.MarkerBefore)
		changed = true
	}
	if changed {
		if err = s.Write(ctx, state.Content); err != nil {
			return fmt.Errorf("restore write (journal retained): %w", err)
		}
		verified, err := s.Read(ctx)
		if err != nil {
			return fmt.Errorf("verify restore: %w", err)
		}
		if restoreMarker && !equalRaw(verified.Content[archiveMarkerKey], j.MarkerBefore) {
			return fmt.Errorf("restore marker verification failed")
		}
		actual := lenientArchiveFields(verified.Content)
		for key, want := range expected {
			if !equalRaw(actual[key], want) {
				return fmt.Errorf("restore verification failed")
			}
		}
	}
	if err = s.Save(ctx, nil); err != nil {
		return err
	}
	if archived {
		return reconcileArchiveLock(ctx, s, true)
	}
	return nil
}

func verifyArchiveOwnership(state archiveState, fields map[string]json.RawMessage, j *archiveJournal) error {
	if err := verifyArchiveRestriction(fields); err != nil {
		return err
	}
	var token string
	_ = json.Unmarshal(state.Content[archiveMarkerKey], &token)
	if !state.OwnSender || token != j.Token {
		return fmt.Errorf("unresolved archive write: ownership cannot be verified")
	}
	for key, change := range j.Changes {
		if !equalRaw(fields[key], change.Applied) {
			return fmt.Errorf("archive verification failed: applied fields differ")
		}
	}
	return nil
}

func setArchiveFieldRaw(content map[string]json.RawMessage, key string, value json.RawMessage) {
	if value == nil {
		delete(content, key)
	} else {
		content[key] = value
	}
}

func parseArchiveLevel(raw json.RawMessage, level *int) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("null power level")
	}
	return json.Unmarshal(raw, level)
}
func decodeArchiveJSON(raw json.RawMessage, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(out)
}
func verifyArchiveRestriction(fields map[string]json.RawMessage) error {
	defaultLevel := 0
	if fields["events_default"] != nil {
		if err := parseArchiveLevel(fields["events_default"], &defaultLevel); err != nil {
			return err
		}
	}
	if defaultLevel < archiveLockLevel {
		return fmt.Errorf("archive default is not restricted")
	}
	for _, key := range []string{"m.room.message", "m.room.encrypted"} {
		if raw := fields[key]; raw != nil {
			var level int
			if err := parseArchiveLevel(raw, &level); err != nil {
				return err
			}
			if level < archiveLockLevel {
				return fmt.Errorf("archive %s is not restricted", key)
			}
		}
	}
	return nil
}
