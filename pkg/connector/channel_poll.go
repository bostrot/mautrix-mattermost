package connector

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/bostrot/mautrix-mattermost/pkg/mattermost"
	"maunium.net/go/mautrix/bridgev2"
)

func (cfg *Config) channelPollInterval() (time.Duration, error) {
	if cfg == nil || cfg.ChannelPollInterval == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(cfg.ChannelPollInterval)
	if err != nil || interval <= 0 {
		return 0, fmt.Errorf("network.channel_poll_interval must be empty or a positive Go duration, got %q", cfg.ChannelPollInterval)
	}
	return interval, nil
}

func runChannelPoll(ctx context.Context, interval time.Duration, poll func()) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		poll()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *MattermostClient) pollChannelLifecycle(ctx context.Context, previous map[string]bool, queue func(bridgev2.RemoteEvent)) (map[string]bool, bool) {
	channels, err := mattermost.GetAllChannelsIncludingArchived(ctx, c.serverURL(), c.Token, c.UserID)
	if err != nil {
		if ctx.Err() == nil {
			c.UserLogin.Log.Debug().Err(err).Msg("Could not complete Mattermost channel lifecycle poll")
		}
		return previous, false
	}
	if ctx.Err() != nil {
		return previous, false
	}
	current := make(map[string]bool, max(len(previous), len(channels)))
	maps.Copy(current, previous)
	for channelID, channel := range channels {
		if _, known := previous[channelID]; !known {
			current[channelID] = channel.DeleteAt > 0
		}
	}

	// The first complete snapshot is a baseline. Processing only previously
	// observed channels also leaves room creation to the ordinary sync paths.
	reconnect := false
	for _, channelID := range slices.Sorted(maps.Keys(previous)) {
		if ctx.Err() != nil {
			return current, false
		}
		wasArchived := previous[channelID]
		if channel, present := channels[channelID]; present {
			archived := channel.DeleteAt > 0
			if archived != wasArchived {
				c.resyncLifecycleChannel(channel, queue)
				current[channelID] = archived
				reconnect = reconnect || !archived
			}
			continue
		}
		// A complete listing is membership-scoped. A channel that still
		// exists, or refuses access, was left/removed: stop tracking it without
		// resyncing or deleting its room. Retry only inconclusive failures.
		_, outcome := c.probeChannelLifecycle(ctx, channelID, true)
		switch outcome {
		case lifecycleActive, lifecycleArchived, lifecycleAccessLost:
			delete(current, channelID)
		case lifecycleDeleted:
			c.deleteLifecycleChannel(channelID, queue)
			delete(current, channelID)
		}
	}
	return current, reconnect
}

func (c *MattermostClient) runLifecyclePoll(ctx context.Context, interval time.Duration) {
	var previous map[string]bool
	runChannelPoll(ctx, interval, func() {
		var reconnect bool
		previous, reconnect = c.pollChannelLifecycle(ctx, previous, c.queueLifecycleEvent)
		if reconnect && ctx.Err() == nil {
			c.reconnectWebsocket()
		}
	})
}
