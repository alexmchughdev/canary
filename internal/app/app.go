// Package app composes config + store + detect + slackx + metrics into
// the running Foghorn. Kept out of cmd/foghorn so main.go stays a thin
// entrypoint and the wiring is testable in isolation.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/detect"
	"github.com/alexmchughdev/foghorn/internal/metrics"
	"github.com/alexmchughdev/foghorn/internal/slackx"
	"github.com/alexmchughdev/foghorn/internal/store"
)

const tickInterval = 30 * time.Second

type App struct {
	cfg    *config.Config
	store  store.Store
	slack  *slackx.Client
	mets   *metrics.Metrics
	log    *slog.Logger
	params detect.Params

	mu        sync.Mutex
	baselines map[string]*detect.Baseline // key = sender|channel
}

func New(cfg *config.Config, st store.Store, sc *slackx.Client, m *metrics.Metrics, log *slog.Logger) *App {
	return &App{
		cfg:       cfg,
		store:     st,
		slack:     sc,
		mets:      m,
		log:       log,
		params:    detect.FromConfig(cfg.Detection),
		baselines: make(map[string]*detect.Baseline),
	}
}

// Run blocks until ctx is cancelled. Owns four cooperating goroutines:
// metrics server, slack socket stream, ticker, ingest consumer.
func (a *App) Run(ctx context.Context) error {
	if err := a.rebuildBaselinesOnBoot(ctx); err != nil {
		return fmt.Errorf("resume: %w", err)
	}

	events := make(chan slackx.Event, 256)

	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := a.mets.Serve(ctx, a.cfg.Metrics.Addr); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("metrics: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := a.slack.Run(ctx, events); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("slack: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		a.loop(ctx, events)
	}()

	select {
	case <-ctx.Done():
		wg.Wait()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (a *App) loop(ctx context.Context, events <-chan slackx.Event) {
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-events:
			a.onMessage(ctx, e)
		case now := <-tick.C:
			a.onTick(ctx, now)
		}
	}
}

func (a *App) baselineFor(key string) *detect.Baseline {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.baselines[key]
	if !ok {
		b = detect.NewBaseline()
		a.baselines[key] = b
	}
	return b
}

func (a *App) override(senderID string) detect.Override {
	cfg, ok := a.cfg.Senders[senderID]
	if !ok {
		return detect.Override{}
	}
	if cfg.Auto {
		return detect.Override{Priority: cfg.Priority}
	}
	return detect.Override{HasInterval: true, Interval: cfg.Interval, Priority: cfg.Priority}
}

func senderKey(senderID, channelID string) string {
	return senderID + "|" + channelID
}

func (a *App) onMessage(ctx context.Context, e slackx.Event) {
	a.mets.MessagesIngested.WithLabelValues(e.ChannelID).Inc()

	key := senderKey(e.SenderID, e.ChannelID)
	b := a.baselineFor(key)

	sn, err := a.store.GetSender(ctx, e.SenderID, e.ChannelID)
	if err != nil {
		a.log.Error("get sender", "err", err)
		return
	}
	if sn == nil {
		sn = &store.Sender{
			SenderID:       e.SenderID,
			ChannelID:      e.ChannelID,
			State:          store.StateLearning,
			StateEnteredAt: e.Timestamp,
		}
	}

	d := detect.OnMessage(sn, b, e.Timestamp, a.override(e.SenderID), a.params)
	if err := a.store.UpsertSender(ctx, sn); err != nil {
		a.log.Error("upsert sender", "err", err)
		return
	}
	a.reflectMetrics(sn)
	a.applyDecision(ctx, sn, d, e.Timestamp)
}

func (a *App) onTick(ctx context.Context, now time.Time) {
	senders, err := a.store.ListSenders(ctx)
	if err != nil {
		a.log.Error("list senders", "err", err)
		return
	}
	for _, sn := range senders {
		d := detect.OnTick(sn, now, a.override(sn.SenderID), a.params)
		if !d.Transition {
			continue
		}
		if err := a.store.UpsertSender(ctx, sn); err != nil {
			a.log.Error("upsert on tick", "err", err)
			continue
		}
		a.reflectMetrics(sn)
		a.applyDecision(ctx, sn, d, now)
	}
}

// applyDecision turns a state transition into Slack + DB side-effects.
// The alert-raise/clear split is what gives us dedup across restarts:
// RaiseAlert is skipped when there's already an open alert, and
// ClearOpenAlerts is idempotent.
func (a *App) applyDecision(ctx context.Context, sn *store.Sender, d detect.Decision, now time.Time) {
	if !d.Transition {
		return
	}
	a.mets.Transitions.WithLabelValues(sn.SenderID, string(d.From), string(d.To)).Inc()
	a.log.Info("state transition",
		"sender", sn.SenderID, "channel", sn.ChannelID,
		"from", d.From, "to", d.To)

	switch d.To {
	case store.StateDrifting, store.StateOffline:
		open, _ := a.store.HasOpenAlert(ctx, sn.SenderID, sn.ChannelID)
		if open {
			return
		}
		if _, err := a.store.RaiseAlert(ctx, &store.Alert{
			SenderID: sn.SenderID, ChannelID: sn.ChannelID,
			State: d.To, RaisedAt: now, LastIntervalSeconds: d.SilentSeconds,
		}); err != nil {
			a.log.Error("raise alert", "err", err)
			return
		}
		a.mets.AlertsRaised.WithLabelValues(sn.SenderID, string(d.To)).Inc()
		_ = a.slack.PostAlert(ctx, formatAlert(sn, d))
	case store.StateRecovering:
		_ = a.store.ClearOpenAlerts(ctx, sn.SenderID, sn.ChannelID, now)
		_ = a.slack.PostAlert(ctx, formatRecovery(sn, d))
	}
}

func (a *App) reflectMetrics(sn *store.Sender) {
	a.mets.LastSeenSeconds.WithLabelValues(sn.SenderID, sn.ChannelID).Set(float64(sn.LastSeen.Unix()))
	ready := 0.0
	if sn.BaselineReady {
		ready = 1
	}
	a.mets.BaselineReady.WithLabelValues(sn.SenderID, sn.ChannelID).Set(ready)
}

// rebuildBaselinesOnBoot seeds in-memory baselines from persisted
// (mean, stddev) so we don't drop back to learning after a restart.
// We can't reconstruct the full 100-sample window, but the mean seeds
// it enough that BlendedMean produces sensible thresholds immediately.
func (a *App) rebuildBaselinesOnBoot(ctx context.Context) error {
	senders, err := a.store.ListSenders(ctx)
	if err != nil {
		return err
	}
	for _, sn := range senders {
		if sn.IntervalMean <= 0 {
			continue
		}
		b := detect.NewBaseline()
		b.Add(sn.IntervalMean)
		a.baselines[senderKey(sn.SenderID, sn.ChannelID)] = b
		a.reflectMetrics(sn)
	}
	a.log.Info("resumed state", "senders", len(senders))
	return nil
}

func formatAlert(sn *store.Sender, d detect.Decision) string {
	silence := time.Duration(d.SilentSeconds * float64(time.Second)).Round(time.Second)
	switch d.To {
	case store.StateDrifting:
		return fmt.Sprintf(":warning: <@%s> drifting in <#%s> — silent for %s (baseline %s)",
			sn.SenderID, sn.ChannelID, silence,
			time.Duration(sn.IntervalMean*float64(time.Second)).Round(time.Second))
	case store.StateOffline:
		return fmt.Sprintf(":rotating_light: <@%s> offline in <#%s> — silent for %s",
			sn.SenderID, sn.ChannelID, silence)
	}
	return ""
}

func formatRecovery(sn *store.Sender, _ detect.Decision) string {
	return fmt.Sprintf(":white_check_mark: <@%s> back in <#%s>", sn.SenderID, sn.ChannelID)
}
