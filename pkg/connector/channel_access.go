package connector

import (
	"context"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
)

// Archive power levels are managed by the durable journal in archive_lock.go.
type accessSyncResult struct {
	Verified bool
	Archived bool
	Reason   string
}

func (c *MattermostClient) syncChannelAccess(ctx context.Context, portal *bridgev2.Portal, channelID string) accessSyncResult {
	return c.syncArchiveStore(ctx, &portalArchiveStore{portal}, channelID)
}

func (c *MattermostClient) syncArchiveStore(ctx context.Context, store archiveStore, channelID string) accessSyncResult {
	probe := c.probeChannel(channelID)
	if probe.State != mattermost.ChannelStateArchived && probe.State != mattermost.ChannelStateActive {
		return accessSyncResult{Reason: "probe-not-authoritative"}
	}
	err := reconcileArchiveLock(ctx, store, probe.State == mattermost.ChannelStateArchived)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("Archive lock reconciliation failed")
		return accessSyncResult{Reason: err.Error()}
	}
	return accessSyncResult{Verified: true, Archived: probe.State == mattermost.ChannelStateArchived, Reason: "verified"}
}

// mmChannelAccessEvent is the neutral placeholder queued to sync the room's
// power levels with the channel's archive state.
//
// Like mmChannelDeleteCheckEvent its type is RemoteEventUnknown: bridgev2 calls
// PreHandle on the portal lane (so the sync is ordered against the lifecycle
// notices around it) and then ignores the event.
type mmChannelAccessEvent struct {
	client    *MattermostClient
	portalKey networkid.PortalKey
	channelID string
}

func (e *mmChannelAccessEvent) GetType() bridgev2.RemoteEventType {
	return bridgev2.RemoteEventUnknown
}
func (e *mmChannelAccessEvent) GetPortalKey() networkid.PortalKey { return e.portalKey }
func (e *mmChannelAccessEvent) AddLogContext(c zerolog.Context) zerolog.Context {
	return c.Str("channel_id", e.channelID).Str("lifecycle", "access-sync")
}
func (e *mmChannelAccessEvent) GetSender() bridgev2.EventSender { return bridgev2.EventSender{} }

// PreHandle runs on the portal lane when the event is dequeued.
func (e *mmChannelAccessEvent) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	if e == nil || e.client == nil || portal == nil {
		return
	}
	res := e.client.syncChannelAccess(ctx, portal, e.channelID)
	log := zerolog.Ctx(ctx)
	log.Debug().
		Str("channel_id", e.channelID).
		Bool("verified", res.Verified).
		Bool("archived", res.Archived).
		Str("reason", res.Reason).
		Msg("Channel access sync")
}

// queueAccessSync queues a power-level sync for the channel. It performs no
// I/O itself.
func (c *MattermostClient) queueAccessSync(channelID string) {
	if channelID == "" {
		return
	}
	res := c.queueEvent(&mmChannelAccessEvent{
		client:    c,
		portalKey: networkid.PortalKey{ID: networkid.PortalID(channelID), Receiver: c.receiverID()},
		channelID: channelID,
	})
	if !res.Queued {
		// Startup/reconnect reconciliation retries access sync independently
		// of the ordinary chat info power levels.
		log := c.lifecycleLogger()
		log.Debug().
			Str("channel_id", channelID).
			Err(res.Error).
			Msg("Channel access sync was not queued")
	}
}

// unlistedPortalIDs returns the channel IDs of this login's bridged portals that
// are absent from the channel listing. Pure so it can be tested without a DB.
func unlistedPortalIDs(portals []*database.Portal, receiver networkid.UserLoginID, listed map[string]bool) []string {
	var out []string
	for _, p := range portals {
		if p == nil || p.MXID == "" || p.Receiver != receiver {
			continue
		}
		if id := string(p.ID); id != "" && !listed[id] {
			out = append(out, id)
		}
	}
	return out
}

// hasArchiveJournal reports whether a portal carries an unfinished or active
// archive lock journal. Such a portal must be revisited after a restart even when
// its channel is listed: a crash between "restoring" and the final save would
// otherwise leave a restored channel's room locked until the next restore event.
func hasArchiveJournal(p *database.Portal) bool {
	if p == nil || p.MXID == "" {
		return false
	}
	m, ok := p.Metadata.(*PortalMetadata)
	if !ok || m == nil {
		return false
	}
	archiveMetaMu.RLock()
	defer archiveMetaMu.RUnlock()
	_, has := (*m)[archiveJournalKey]
	return has
}

// reconcileUnlistedPortals queues an access sync for every bridged portal the
// channel listing did not contain. The sync decides from a live probe and is
// non-destructive: it never deletes a room.
func (c *MattermostClient) reconcileUnlistedPortals(listed map[string]bool) {
	if c.UserLogin == nil || c.UserLogin.Bridge == nil || c.UserLogin.Bridge.DB == nil {
		return
	}
	log := c.lifecycleLogger()
	portals, err := c.UserLogin.Bridge.DB.Portal.GetAllWithMXID(context.Background())
	if err != nil {
		log.Warn().Err(err).Msg("Could not list portals to reconcile archived channels")
		return
	}
	ids := unlistedPortalIDs(portals, c.receiverID(), listed)
	queued := make(map[string]bool, len(ids))
	for _, id := range ids {
		queued[id] = true
	}
	for _, p := range portals {
		if p != nil && p.Receiver == c.receiverID() && hasArchiveJournal(p) && !queued[string(p.ID)] {
			ids = append(ids, string(p.ID))
			queued[string(p.ID)] = true
		}
	}
	for _, id := range ids {
		c.queueAccessSync(id)
	}
	if len(ids) > 0 {
		log.Debug().Int("count", len(ids)).Msg("Queued access sync for portals missing from the channel listing")
	}
}
