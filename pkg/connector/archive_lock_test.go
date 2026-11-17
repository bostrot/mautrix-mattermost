package connector

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"net/http"
	"reflect"
	"testing"
)

type memoryArchiveStore struct {
	content                       map[string]json.RawMessage
	journal                       []byte
	writes, saves, reads          int
	failWrite, failRead, failSave int
	afterWrite                    func(*memoryArchiveStore)
	own                           bool
}

func newArchiveStore(t *testing.T, raw string) *memoryArchiveStore {
	t.Helper()
	s := &memoryArchiveStore{own: true}
	if err := json.Unmarshal([]byte(raw), &s.content); err != nil {
		t.Fatal(err)
	}
	return s
}
func cloneContent(m map[string]json.RawMessage) map[string]json.RawMessage {
	b, _ := json.Marshal(m)
	var out map[string]json.RawMessage
	_ = json.Unmarshal(b, &out)
	return out
}
func (s *memoryArchiveStore) Read(context.Context) (archiveState, error) {
	s.reads++
	if s.failRead == s.reads {
		return archiveState{}, errors.New("read denied")
	}
	return archiveState{cloneContent(s.content), s.own}, nil
}
func (s *memoryArchiveStore) Write(_ context.Context, m map[string]json.RawMessage) error {
	s.writes++
	if s.failWrite == s.writes {
		return errors.New("M_FORBIDDEN")
	}
	s.content = cloneContent(m)
	s.own = true
	if s.afterWrite != nil {
		s.afterWrite(s)
	}
	return nil
}
func (s *memoryArchiveStore) Load() (*archiveJournal, error) {
	if len(s.journal) == 0 {
		return nil, nil
	}
	var j archiveJournal
	err := json.Unmarshal(s.journal, &j)
	return &j, err
}
func (s *memoryArchiveStore) Save(_ context.Context, j *archiveJournal) error {
	s.saves++
	if s.failSave == s.saves {
		return errors.New("database unavailable")
	}
	if j == nil {
		s.journal = nil
		return nil
	}
	s.journal, _ = json.Marshal(j)
	return nil
}
func reconcile(t *testing.T, s *memoryArchiveStore, archived bool) {
	t.Helper()
	if err := reconcileArchiveLock(t.Context(), s, archived); err != nil {
		t.Fatal(err)
	}
}
func TestArchiveExactPolicyRestoration(t *testing.T) {
	for _, raw := range []string{
		`{"users":{"@admin:x":100},"unknown":{"a":1}}`,
		`{"events_default":0,"events":{"m.room.message":10,"m.room.encrypted":-1,"m.room.name":80}}`,
		`{"events_default":100,"events":{"m.room.message":0,"m.room.encrypted":90}}`,
		`{"events_default":70,"events":{"m.room.message":80,"m.room.encrypted":90}}`,
		`{"events_default":0,"com.pugetaudio.archive_lock_token":{"preexisting":true}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			s := newArchiveStore(t, raw)
			before := cloneContent(s.content)
			reconcile(t, s, true)
			writes := s.writes
			reconcile(t, s, true)
			if writes != s.writes {
				t.Fatal("non-idempotent archive")
			}
			fields, err := archiveFields(s.content)
			if err != nil {
				t.Fatal(err)
			}
			for key, v := range fields {
				if v != nil {
					var n int
					_ = json.Unmarshal(v, &n)
					if n < 50 {
						t.Fatalf("unlocked %s", key)
					}
				}
			}
			reconcile(t, s, false)
			if !equalContent(before, s.content) {
				t.Fatalf("restoration: %s -> %s", before, s.content)
			}
			writes = s.writes
			reconcile(t, s, false)
			if s.writes != writes {
				t.Fatal("non-idempotent restore")
			}
		})
	}
}
func TestArchiveInterveningAdminEdit(t *testing.T) {
	s := newArchiveStore(t, `{"events_default":1,"events":{"m.room.message":2,"m.room.encrypted":3}}`)
	reconcile(t, s, true)
	s.content["events_default"] = json.RawMessage(`90`)
	setArchiveField(s.content, "m.room.message", json.RawMessage(`75`))
	s.content["other"] = json.RawMessage(`{"preserve":true}`)
	reconcile(t, s, false)
	if string(s.content["events_default"]) != "90" {
		t.Fatal("overwrote admin default")
	}
	f, _ := archiveFields(s.content)
	if string(f["m.room.message"]) != "75" || string(f["m.room.encrypted"]) != "3" || s.content["other"] == nil {
		t.Fatal("incorrect field ownership restore")
	}
}
func TestArchiveErrorsAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name              string
		read, write, save int
	}{
		{"initial read", 1, 0, 0}, {"prepare persistence", 0, 0, 1}, {"permission", 0, 1, 0}, {"verification", 2, 0, 0}, {"ownership persistence", 0, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newArchiveStore(t, `{}`)
			s.failRead = tc.read
			s.failWrite = tc.write
			s.failSave = tc.save
			if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
				t.Fatal("claimed success")
			}
			if (tc.read == 1 || tc.save == 1) && s.writes != 0 {
				t.Fatal("write before durable intent")
			}
			// Persisted JSON is the only journal carried across the simulated restart.
			restarted := newArchiveStore(t, `{}`)
			restarted.content = cloneContent(s.content)
			restarted.journal = append([]byte(nil), s.journal...)
			reconcile(t, restarted, true)
			reconcile(t, restarted, false)
			if restarted.content["events_default"] != nil || len(restarted.journal) != 0 {
				t.Fatal("failed crash recovery")
			}
		})
	}
}
func TestArchiveVerificationConflict(t *testing.T) {
	s := newArchiveStore(t, `{}`)
	s.afterWrite = func(s *memoryArchiveStore) { s.content["events_default"] = json.RawMessage(`99`); s.own = false }
	if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
		t.Fatal("claimed success despite intervening edit")
	}
	// The pending write cannot be verified, so the restore must make progress
	// without weakening anything: 99 is not what the bridge applied, so it stays.
	if err := reconcileArchiveLock(t.Context(), s, false); err != nil {
		t.Fatalf("restore wedged on an unverifiable pending journal: %v", err)
	}
	if string(s.content["events_default"]) != "99" {
		t.Fatal("weakened administrator policy")
	}
	if len(s.journal) != 0 {
		t.Fatal("journal retained: the room would be revisited forever")
	}
}
func TestArchiveRestoreCrashWindows(t *testing.T) {
	for _, phase := range []string{"before write", "after write", "verification", "clear journal"} {
		t.Run(phase, func(t *testing.T) {
			s := newArchiveStore(t, `{"events_default":12}`)
			reconcile(t, s, true)
			switch phase {
			case "before write":
				s.failWrite = s.writes + 1
			case "after write":
				s.failRead = s.reads + 2
			case "verification":
				s.afterWrite = func(s *memoryArchiveStore) { s.content["events_default"] = json.RawMessage(`88`) }
			case "clear journal":
				s.failSave = s.saves + 2
			}
			if err := reconcileArchiveLock(t.Context(), s, false); err == nil {
				t.Fatal("expected failure")
			}
			s.failRead = 0
			s.failWrite = 0
			s.failSave = 0
			s.afterWrite = nil
			reconcile(t, s, false)
			want := "12"
			if phase == "verification" {
				want = "88"
			}
			if string(s.content["events_default"]) != want || len(s.journal) != 0 {
				t.Fatal("restore restart lost policy")
			}
		})
	}
}
func TestPortalMetadataUnknownRoundTrip(t *testing.T) {
	creator := (&MattermostConnector{}).GetDBMetaTypes().Portal
	meta := creator()
	raw := []byte(`{"unknown":{"future":[1,2]},"another":null}`)
	if err := json.Unmarshal(raw, meta); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var a, b any
	_ = json.Unmarshal(raw, &a)
	_ = json.Unmarshal(encoded, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("unknown metadata lost")
	}
}

func equalContent(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for key, v := range a {
		if !equalRaw(v, b[key]) {
			return false
		}
	}
	return true
}

func TestArchiveProbeGateAndErrorReporting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		probe probeFunc
		want  bool
	}{
		{"archived", archivedProbe, true}, {"active", activeProbe, true},
		{"missing", authoritativeNotFoundProbe, false}, {"unknown", func(string) mattermost.ChannelProbe { return mattermost.ChannelProbe{} }, false},
		{"forbidden", func(string) mattermost.ChannelProbe { return httpProbe(http.StatusForbidden) }, false},
		{"server error", func(string) mattermost.ChannelProbe { return httpProbe(http.StatusInternalServerError) }, false},
		{"transport", func(string) mattermost.ChannelProbe { return transportErrProbe() }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t, lifecycleOptions{probe: tc.probe})
			s := newArchiveStore(t, `{}`)
			res := h.client.syncArchiveStore(t.Context(), s, "channel")
			if res.Verified != tc.want {
				t.Fatalf("result %+v", res)
			}
			if !tc.want && (s.reads != 0 || s.writes != 0 || s.saves != 0) {
				t.Fatal("ambiguous probe changed room")
			}
			s.failRead = s.reads + 1
			if tc.want && h.client.syncArchiveStore(t.Context(), s, "channel").Verified {
				t.Fatal("claimed success on read failure")
			}
		})
	}
}
func TestArchiveRejectsInvalidPowerLevels(t *testing.T) {
	for _, raw := range []string{`{"events_default":null}`, `{"events_default":"0"}`, `{"events":null}`, `{"events":{"m.room.message":null}}`} {
		s := newArchiveStore(t, raw)
		if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
			t.Fatalf("accepted %s", raw)
		}
		if s.writes != 0 || s.saves != 0 {
			t.Fatal("wrote invalid state")
		}
	}
}
func TestArchiveNoOpWriteCannotClaimSuccess(t *testing.T) {
	s := newArchiveStore(t, `{}`)
	s.afterWrite = func(s *memoryArchiveStore) { s.content = map[string]json.RawMessage{} }
	if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
		t.Fatal("claimed success without applied state")
	}
	if s.writes != 1 {
		t.Fatal("retried indefinitely")
	}
}

func TestArchiveInterveningExplicitOverride(t *testing.T) {
	s := newArchiveStore(t, `{"events":{"m.room.name":80}}`)
	reconcile(t, s, true)
	setArchiveField(s.content, "m.room.encrypted", json.RawMessage(`0`))
	// An administrator lowering a locked field while archived takes ownership of
	// it: no error loop, no re-lock, and the journal stops claiming that field.
	reconcile(t, s, true)
	j, err := s.Load()
	if err != nil || j == nil {
		t.Fatalf("journal = %v, err = %v", j, err)
	}
	if _, claims := j.Changes["m.room.encrypted"]; claims {
		t.Fatal("journal still claims the administrator's m.room.encrypted")
	}
	if _, claims := j.Changes["events_default"]; !claims {
		t.Fatal("journal dropped events_default, which the bridge still owns")
	}
	writes := s.writes
	reconcile(t, s, false)
	if s.writes != writes+1 {
		t.Fatal("restore did not write")
	}
	fields, err := archiveFields(s.content)
	if err != nil {
		t.Fatal(err)
	}
	if fields["events_default"] != nil || string(fields["m.room.encrypted"]) != "0" {
		t.Fatal("restore overwrote an unowned explicit override")
	}
}
func TestArchiveRestoreRequiresDurableIntent(t *testing.T) {
	s := newArchiveStore(t, `{}`)
	reconcile(t, s, true)
	writes := s.writes
	s.failSave = s.saves + 1
	if err := reconcileArchiveLock(t.Context(), s, false); err == nil {
		t.Fatal("ignored restoration persistence failure")
	}
	if s.writes != writes {
		t.Fatal("restored before recording intent")
	}
	s.failSave = 0
	reconcile(t, s, false)
}

// An archive must only ever raise restrictions. A room an administrator already
// locked tighter than the archive level (events_default 100, a message override
// of 90) must stay exactly as it is while archived, and only the field that was
// below the archive level may be journaled and changed.
func TestArchiveNeverLowersStricterAdminValues(t *testing.T) {
	s := newArchiveStore(t, `{"events_default":100,"events":{"m.room.message":90,"m.room.encrypted":0}}`)
	reconcile(t, s, true)

	fields, err := archiveFields(s.content)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(fields["events_default"]); got != "100" {
		t.Errorf("archive changed events_default 100 to %s: a stricter admin value was weakened", got)
	}
	if got := string(fields["m.room.message"]); got != "90" {
		t.Errorf("archive changed m.room.message 90 to %s: a stricter admin value was weakened", got)
	}
	if got := string(fields["m.room.encrypted"]); got != "50" {
		t.Errorf("m.room.encrypted = %s, want it raised to the archive level 50", got)
	}
	j, err := s.Load()
	if err != nil || j == nil {
		t.Fatalf("journal = %v, err = %v, want a journal for the one changed field", j, err)
	}
	if _, owned := j.Changes["events_default"]; owned {
		t.Error("journal claims ownership of events_default, which the bridge never changed")
	}
	if _, owned := j.Changes["m.room.message"]; owned {
		t.Error("journal claims ownership of m.room.message, which the bridge never changed")
	}

	// A room that is already at least as restricted everywhere needs no write
	// and no journal at all.
	already := newArchiveStore(t, `{"events_default":100,"events":{"m.room.message":50,"m.room.encrypted":75}}`)
	reconcile(t, already, true)
	if already.writes != 0 || len(already.journal) != 0 {
		t.Errorf("already-restricted room: writes=%d journal=%q, want neither", already.writes, already.journal)
	}
}

// A write is only ours if the state event's sender is the bridge bot. The token
// alone is not enough: anyone who can read the room's state can copy it.
func TestArchiveRejectsOwnershipFromAnotherSender(t *testing.T) {
	s := newArchiveStore(t, `{}`)
	s.afterWrite = func(s *memoryArchiveStore) { s.own = false } // the state now reads as sent by someone else

	if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
		t.Fatal("archive claimed success although the power-level event was not sent by the bridge")
	}
	j, err := s.Load()
	if err != nil || j == nil {
		t.Fatalf("journal = %v, err = %v, want it retained for recovery", j, err)
	}
	if j.Phase == "owned" {
		t.Fatal("journal advanced to owned without verified ownership")
	}
}

// A pending journal whose marker was dropped by another writer (bridgev2 rewrites
// power levels from a typed struct and loses unknown keys) must not leave the
// room locked forever after a restore.
func TestArchivePendingWithDroppedMarkerStillRestores(t *testing.T) {
	s := newArchiveStore(t, `{"users":{"@a:x":100}}`)
	s.afterWrite = func(s *memoryArchiveStore) { delete(s.content, archiveMarkerKey) }
	if err := reconcileArchiveLock(t.Context(), s, true); err == nil {
		t.Fatal("claimed success although the marker was lost")
	}
	if err := reconcileArchiveLock(t.Context(), s, false); err != nil {
		t.Fatalf("restore wedged: %v", err)
	}
	if s.content["events_default"] != nil {
		t.Fatalf("room still locked after restore: events_default=%s", s.content["events_default"])
	}
	if len(s.journal) != 0 {
		t.Fatal("journal retained")
	}
}

// One value the bridge cannot parse must not block restoring the fields it owns,
// and the restore must not rewrite an events object it does not understand.
func TestArchiveRestoreIgnoresUnparsableUnownedValues(t *testing.T) {
	for _, bad := range []string{`0.5`, `99999999999999999999`, `"50"`} {
		t.Run(bad, func(t *testing.T) {
			s := newArchiveStore(t, `{"events":{"m.room.message":`+bad+`}}`)
			reconcileForce := func() { _ = reconcileArchiveLock(t.Context(), s, true) }
			reconcileForce() // archive refuses an unparsable field and writes nothing
			if s.writes != 0 {
				t.Fatalf("archive wrote over an unparsable value: %d writes", s.writes)
			}
			// Simulate a journal from before the admin made the value odd.
			ok := newArchiveStore(t, `{}`)
			reconcile(t, ok, true)
			ok.content["events"] = json.RawMessage(`[]`)
			if err := reconcileArchiveLock(t.Context(), ok, false); err != nil {
				t.Fatalf("restore blocked by a malformed events object: %v", err)
			}
			if string(ok.content["events"]) != "[]" {
				t.Fatalf("restore rewrote an events object it does not own: %s", ok.content["events"])
			}
			if ok.content["events_default"] != nil {
				t.Fatal("owned events_default was not restored")
			}
		})
	}
}

// Disowned fields must not make the journal unreadable on the next start.
func TestArchiveJournalWithNoOwnedFieldsRoundTrips(t *testing.T) {
	s := newArchiveStore(t, `{}`)
	reconcile(t, s, true)
	s.content["events_default"] = json.RawMessage(`0`) // admin unlocks while archived
	reconcile(t, s, true)
	j, err := s.Load()
	if err != nil || j == nil || len(j.Changes) != 0 {
		t.Fatalf("journal = %+v, err = %v, want an empty-but-valid journal", j, err)
	}
	reconcile(t, s, true) // steady state: no error, no writes
	w := s.writes
	reconcile(t, s, false)
	if string(s.content["events_default"]) != "0" {
		t.Fatalf("restore overwrote the administrator's value: %s", s.content["events_default"])
	}
	if _, has := s.content[archiveMarkerKey]; has {
		t.Fatal("stale marker left behind after restore")
	}
	if s.writes != w+1 {
		t.Fatalf("restore writes = %d, want one (marker removal only)", s.writes-w)
	}
}
