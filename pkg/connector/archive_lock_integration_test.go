package connector

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// These doubles exercise the actual framework interfaces and wire encoding,
// while using only an isolated local SQLite database and no network listeners.
type archiveMatrix struct {
	bridgev2.MatrixConnector
	raw    json.RawMessage
	sender id.UserID
	writes int
}

func (m *archiveMatrix) GetStateEvent(_ context.Context, _ id.RoomID, typ event.Type, key string) (*event.Event, error) {
	if typ != event.StatePowerLevels || key != "" {
		panic("unexpected state read")
	}
	var content event.Content
	if err := json.Unmarshal(m.raw, &content); err != nil {
		return nil, err
	}
	return &event.Event{Content: content, Sender: m.sender}, nil
}

type archiveBot struct {
	bridgev2.MatrixAPI
	matrix *archiveMatrix
}

func (b *archiveBot) GetMXID() id.UserID { return "@bridge:test" }
func (b *archiveBot) SendState(_ context.Context, _ id.RoomID, typ event.Type, key string, content *event.Content, _ time.Time) (*mautrix.RespSendEvent, error) {
	if typ != event.StatePowerLevels || key != "" {
		panic("unexpected state write")
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	b.matrix.raw = raw
	b.matrix.sender = b.GetMXID()
	b.matrix.writes++
	return &mautrix.RespSendEvent{EventID: "$state"}, nil
}
func TestArchivePortalSQLiteRestartAndRawState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "portals.db")
	open := func() *database.Database {
		t.Helper()
		db, err := dbutil.NewWithDialect(path, "sqlite3")
		if err != nil {
			t.Fatal(err)
		}
		out := database.New("test", (&MattermostConnector{}).GetDBMetaTypes(), db)
		if err = out.Upgrade(t.Context()); err != nil {
			t.Fatal(err)
		}
		return out
	}
	db := open()
	key := networkid.PortalKey{ID: "channel"}
	meta := PortalMetadata{"unknown": json.RawMessage(`{"large":9007199254740993}`)}
	p := &database.Portal{PortalKey: key, MXID: "!room:test", Metadata: &meta}
	if err := db.Portal.Insert(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	mx := &archiveMatrix{raw: json.RawMessage(`{"events":{"m.room.message":0,"m.room.encrypted":95},"extension":{"large":9007199254740993}}`), sender: "@admin:test"}
	bot := &archiveBot{matrix: mx}
	br := &bridgev2.Bridge{DB: db, Matrix: mx, Bot: bot}
	store := &portalArchiveStore{&bridgev2.Portal{Portal: p, Bridge: br}}
	// Crash after sending but before recording verified ownership.
	state, err := store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	journal := &archiveJournal{Version: 1, Phase: "pending", Token: "test-token", Changes: map[string]archiveChange{
		"events_default": {Applied: json.RawMessage(`50`)},
		"m.room.message": {Before: json.RawMessage(`0`), Applied: json.RawMessage(`50`)},
	}}
	if err = store.Save(t.Context(), journal); err != nil {
		t.Fatal(err)
	}
	setArchiveField(state.Content, "events_default", json.RawMessage(`50`))
	setArchiveField(state.Content, "m.room.message", json.RawMessage(`50`))
	state.Content[archiveMarkerKey] = json.RawMessage(`"test-token"`)
	if err = store.Write(t.Context(), state.Content); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	br.DB = db
	p, err = db.Portal.GetByKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	store = &portalArchiveStore{&bridgev2.Portal{Portal: p, Bridge: br}}
	if err = reconcileArchiveLock(t.Context(), store, false); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := newArchiveStore(t, `{"events":{"m.room.message":0,"m.room.encrypted":95},"extension":{"large":9007199254740993}}`)
	if !equalContent(restored.Content, want.content) {
		t.Fatalf("incorrect raw restoration: %s", mx.raw)
	}
	p, err = db.Portal.GetByKey(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	loaded := p.Metadata.(*PortalMetadata)
	if !equalRaw((*loaded)["unknown"], meta["unknown"]) || (*loaded)[archiveJournalKey] != nil {
		t.Fatal("metadata did not survive SQL round trip")
	}
	// Failed database persistence must never lead to a Matrix write.
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before := mx.writes
	if err = reconcileArchiveLock(t.Context(), store, true); err == nil {
		t.Fatal("ignored database failure")
	}
	if before != mx.writes {
		t.Fatal("wrote Matrix before durable intent")
	}
}

// The real store must accept a journal whose fields were all taken over by an
// administrator; rejecting it would wedge the portal's restore after a restart.
func TestArchivePortalStoreLoadsJournalWithNoOwnedFields(t *testing.T) {
	db, err := dbutil.NewWithDialect(filepath.Join(t.TempDir(), "portals.db"), "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	bdb := database.New("test", (&MattermostConnector{}).GetDBMetaTypes(), db)
	if err = bdb.Upgrade(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	meta := PortalMetadata{}
	p := &database.Portal{PortalKey: networkid.PortalKey{ID: "channel"}, MXID: "!room:test", Metadata: &meta}
	if err = bdb.Portal.Insert(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	store := &portalArchiveStore{&bridgev2.Portal{Portal: p, Bridge: &bridgev2.Bridge{DB: bdb}}}
	if err = store.Save(t.Context(), &archiveJournal{Version: 1, Phase: "owned", Token: "t", Changes: map[string]archiveChange{}}); err != nil {
		t.Fatal(err)
	}
	j, err := store.Load()
	if err != nil || j == nil {
		t.Fatalf("Load = %v, %v; want the empty journal back", j, err)
	}
	if len(j.Changes) != 0 || j.Phase != "owned" {
		t.Fatalf("journal = %+v", j)
	}
}
