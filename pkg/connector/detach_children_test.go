package connector

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type fakeStore struct {
	children []*database.Portal
	updated  []*database.Portal
	getErr   error
	updErr   error
}

func (f *fakeStore) GetChildren(context.Context, networkid.PortalKey) ([]*database.Portal, error) {
	return f.children, f.getErr
}

func (f *fakeStore) Update(_ context.Context, p *database.Portal) error {
	if f.updErr != nil {
		return f.updErr
	}
	f.updated = append(f.updated, p)
	return nil
}

// Only the space's own children are detached; a portal that belongs to another
// parent is left alone, and the detach clears the parent rather than deleting.
func TestDetachChildrenClearsOnlyThisSpacesLinks(t *testing.T) {
	space := networkid.PortalKey{ID: "team:t1", Receiver: "login"}
	child := &database.Portal{PortalKey: networkid.PortalKey{ID: "c1", Receiver: "login"}, ParentKey: space}
	other := &database.Portal{PortalKey: networkid.PortalKey{ID: "c2", Receiver: "login"},
		ParentKey: networkid.PortalKey{ID: "team:t2", Receiver: "login"}}

	store := &fakeStore{children: []*database.Portal{child, other}}
	if err := detachChildren(context.Background(), store, space); err != nil {
		t.Fatalf("detach: %v", err)
	}

	if len(store.updated) != 1 || store.updated[0] != child {
		t.Fatalf("updated %d portals, want only the space's child", len(store.updated))
	}
	if child.ParentKey != (networkid.PortalKey{}) {
		t.Fatalf("child parent = %+v, want cleared", child.ParentKey)
	}
	if other.ParentKey.ID != "team:t2" {
		t.Fatalf("another parent's child was changed: %+v", other.ParentKey)
	}
}

func TestDetachChildrenReportsStoreErrors(t *testing.T) {
	space := networkid.PortalKey{ID: "team:t1", Receiver: "login"}
	if err := detachChildren(context.Background(), &fakeStore{getErr: errors.New("boom")}, space); err == nil {
		t.Fatal("expected the read error")
	}
	child := &database.Portal{PortalKey: networkid.PortalKey{ID: "c1"}, ParentKey: space}
	err := detachChildren(context.Background(), &fakeStore{children: []*database.Portal{child}, updErr: errors.New("locked")}, space)
	if err == nil {
		t.Fatal("expected the write error")
	}
}
