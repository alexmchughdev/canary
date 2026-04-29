// Package app wires config, store, detect, connector, and metrics into a
// running worker. main.go stays a thin entrypoint.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/connector"
	"github.com/alexmchughdev/foghorn/internal/detect"
	"github.com/alexmchughdev/foghorn/internal/metrics"
	"github.com/alexmchughdev/foghorn/internal/store"
)

const tickInterval = 30 * time.Second

type App struct {
	cfg    *config.Config
	store  store.Store
	conn   connector.Connector
	mets   *metrics.Metrics
	log    *slog.Logger
	params detect.Params

	mu        sync.Mutex
	baselines map[string]*detect.Baseline // key = sender|channel
}

func New(cfg *config.Config, st store.Store, c connector.Connector, m *metrics.Metrics, log *slog.Logger) *App {
	return &App{
		cfg:       cfg,
		store:     st,
		conn:      c,
		mets:      m,
		log:       log,
		params:    detect.FromConfig(cfg.Detection),
		baselines: make(map[string]*detect.Baseline),
	}
}

// Run blocks until ctx is cancelled. It boots state, backfills history,
// then runs the metrics server, connector stream, and ingest/tick loop
// as cooperating goroutines.
func (a *App) Run(ctx context.Context) error {
	if err := a.rebuildBaselinesOnBoot(ctx); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if err := a.backfillHistory(ctx); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}

	messages := make(chan connector.Message, 256)

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
		if err := a.conn.Stream(ctx, messages); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("connector: %w", err)
		}
	}()
	go func() {
		defer wg.Done()
		a.loop(ctx, messages)
	}()

	select {
	case <-ctx.Done():
		wg.Wait()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (a *App) loop(ctx context.Context, messages <-chan connector.Message) {
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-messages:
			a.onMessage(ctx, m)
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

func (a *App) onMessage(ctx context.Context, m connector.Message) {
	a.mets.MessagesIngested.WithLabelValues(m.ChannelID).Inc()

	key := senderKey(m.SenderID, m.ChannelID)
	b := a.baselineFor(key)

	sn, err := a.store.GetSender(ctx, m.SenderID, m.ChannelID)
	if err != nil {
		a.log.Error("get sender", "err", err)
		return
	}
	if sn == nil {
		sn = &store.Sender{
			SenderID:       m.SenderID,
			ChannelID:      m.ChannelID,
			State:          store.StateLearning,
			StateEnteredAt: m.Timestamp,
		}
	}

	d := detect.OnMessage(sn, b, m.Timestamp, a.override(m.SenderID), a.params)
	if err := a.store.UpsertSender(ctx, sn); err != nil {
		a.log.Error("upsert sender", "err", err)
		return
	}
	a.reflectMetrics(sn)
	a.applyDecision(ctx, sn, d, m.Timestamp)
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

// applyDecision turns a state transition into connector + DB side-effects.
// Raise/clear are split so dedup survives restarts: RaiseAlert is a no-op
// when an open alert already exists, ClearOpenAlerts is idempotent.
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
		_ = a.conn.Post(ctx, a.cfg.Channels.AlertTo, formatAlert(sn, d))
	case store.StateRecovering:
		_ = a.store.ClearOpenAlerts(ctx, sn.SenderID, sn.ChannelID, now)
		_ = a.conn.Post(ctx, a.cfg.Channels.AlertTo, formatRecovery(sn, d))
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

// backfillHistory pulls the lookback window from each connector and
// drains it through onMessage before live streaming starts, so senders
// can leave the learning state on boot. Synchronous drain prevents live
// events from interleaving.
func (a *App) backfillHistory(ctx context.Context) error {
	since := time.Now().Add(-a.cfg.Learning.Lookback)
	for _, c := range []connector.Connector{a.conn} {
		msgs, err := c.History(ctx, since)
		if err != nil {
			return fmt.Errorf("connector %s: %w", c.Name(), err)
		}
		a.log.Info("history backfill",
			"connector", c.Name(), "messages", len(msgs), "since", since)
		for _, m := range msgs {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.onMessage(ctx, m)
		}
	}
	return nil
}

// rebuildBaselinesOnBoot seeds in-memory baselines from each sender's
// persisted mean so a restart doesn't drop everyone back into learning.
// The full sample window can't be reconstructed, but the mean is enough
// for BlendedMean to produce sensible thresholds immediately.
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
