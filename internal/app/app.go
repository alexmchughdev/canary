// Package app wires config, store, detect, connector, and metrics into a
// running worker. main.go stays a thin entrypoint.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/alexmchughdev/foghorn/internal/cluster"
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

	mu sync.Mutex
	// baselines tracks per-(sender, channel) inter-arrival baselines.
	baselines map[string]*detect.Baseline
	// classifiers is per-channel. Nil entry means the channel is still
	// in the learn phase and content alerts are suppressed.
	classifiers map[string]*detect.Classifier
	// clusterStates tracks per-cluster cadence + DB id for missing-
	// pattern detection. Key is channel + "|" + clusterIndex.
	clusterStates map[string]*clusterState
	// cooldownUntil holds the next time a content alert of a given kind
	// may be raised on a given channel. Key is channel + ":" + kind.
	cooldownUntil map[string]time.Time

	// historyByChannel buffers backfilled messages per channel during
	// boot so the learn-phase clustering pass can read them.
	historyByChannel map[string][]connector.Message
}

type clusterState struct {
	dbID         int64
	channelID    string
	clusterIndex int
	baseline     *detect.Baseline
	lastSeen     time.Time
}

func New(cfg *config.Config, st store.Store, c connector.Connector, m *metrics.Metrics, log *slog.Logger) *App {
	return &App{
		cfg:              cfg,
		store:            st,
		conn:             c,
		mets:             m,
		log:              log,
		params:           detect.FromConfig(cfg.Detection),
		baselines:        make(map[string]*detect.Baseline),
		classifiers:      make(map[string]*detect.Classifier),
		clusterStates:    make(map[string]*clusterState),
		cooldownUntil:    make(map[string]time.Time),
		historyByChannel: make(map[string][]connector.Message),
	}
}

// Run blocks until ctx is cancelled. It boots state, backfills history,
// learns content clusters, then runs the metrics server, connector
// stream, and ingest/tick loop as cooperating goroutines.
func (a *App) Run(ctx context.Context) error {
	if err := a.rebuildBaselinesOnBoot(ctx); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if err := a.backfillHistory(ctx); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	if err := a.learnClusters(ctx); err != nil {
		return fmt.Errorf("learn clusters: %w", err)
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

func clusterKey(channelID string, idx int) string {
	return channelID + "|" + strconv.Itoa(idx)
}

func cooldownKey(channelID, kind string) string {
	return channelID + ":" + kind
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

	a.classifyAndUpdate(ctx, m)
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

	a.checkMissingPatterns(ctx, now)
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
			Kind:     store.AlertKindFrequency,
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
// drains it through onMessage before live streaming starts. Messages
// are also buffered per channel so the learn-clusters step can run
// over the same corpus.
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
			a.historyByChannel[m.ChannelID] = append(a.historyByChannel[m.ChannelID], m)
			a.onMessage(ctx, m)
		}
	}
	return nil
}

// learnClusters builds a content-cluster fingerprint set per channel
// from the buffered backfill, persists each cluster, and seeds per-
// cluster cadence baselines. After this returns, the channel's
// classifier is non-nil and live messages will be classified.
func (a *App) learnClusters(ctx context.Context) error {
	opts := cluster.BuildOptions{
		Epsilon: a.cfg.Cluster.Epsilon,
		MinPts:  a.cfg.Cluster.MinPts,
	}
	for channelID, msgs := range a.historyByChannel {
		texts := make([]string, 0, len(msgs))
		kept := make([]connector.Message, 0, len(msgs))
		for _, m := range msgs {
			if m.Text == "" {
				continue
			}
			texts = append(texts, m.Text)
			kept = append(kept, m)
		}
		clusters, vec := cluster.BuildWithVectoriser(texts, opts)

		fps := make([]cluster.Fingerprint, len(clusters))
		for i, c := range clusters {
			fps[i] = c.Fingerprint
		}
		a.classifiers[channelID] = detect.NewClassifier(fps, vec, detect.ClassifierOptions{
			Threshold:   a.cfg.Cluster.MatchThreshold,
			StableRatio: a.cfg.Cluster.StableRatio,
		})
		a.mets.ClustersTotal.WithLabelValues(channelID).Set(float64(len(clusters)))

		for _, c := range clusters {
			st := seedClusterState(c, kept)
			st.channelID = channelID
			st.clusterIndex = c.ID
			rec := &store.Cluster{
				ChannelID:      channelID,
				ClusterIndex:   c.ID,
				Size:           c.Size,
				SampleMessage:  c.SampleMessage,
				Centroid:       c.Centroid,
				StableTokens:   c.StableTokens,
				LastMessageAt:  zeroOrPtr(st.lastSeen),
				IntervalMean:   st.baseline.Mean(),
				IntervalStddev: st.baseline.Stddev(),
			}
			if err := a.store.UpsertCluster(ctx, rec); err != nil {
				return fmt.Errorf("upsert cluster %s/%d: %w", channelID, c.ID, err)
			}
			st.dbID = rec.ID
			a.clusterStates[clusterKey(channelID, c.ID)] = st
		}

		a.log.Info("clusters learned",
			"channel", channelID, "messages", len(texts), "clusters", len(clusters))
	}
	// Free the backfill buffer once learning is done.
	a.historyByChannel = nil
	return nil
}

// seedClusterState walks the cluster's members in chronological order
// and feeds inter-arrival intervals into a fresh baseline.
func seedClusterState(c cluster.Cluster, msgs []connector.Message) *clusterState {
	b := detect.NewBaseline()
	var lastSeen time.Time
	var prev time.Time
	for i, idx := range c.Members {
		ts := msgs[idx].Timestamp
		if i > 0 && ts.After(prev) {
			b.Add(ts.Sub(prev).Seconds())
		}
		prev = ts
		lastSeen = ts
	}
	return &clusterState{baseline: b, lastSeen: lastSeen}
}

func zeroOrPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// classifyAndUpdate runs the per-channel classifier on a live message,
// updates the matching cluster's cadence baseline, and raises content
// alerts subject to the cooldown gate. No-op if the channel hasn't
// finished its learn phase.
func (a *App) classifyAndUpdate(ctx context.Context, m connector.Message) {
	if m.Text == "" {
		return
	}
	a.mu.Lock()
	cls, ok := a.classifiers[m.ChannelID]
	a.mu.Unlock()
	if !ok || cls == nil {
		return
	}

	v := cls.Classify(m.Text)
	clusterIDLabel := strconv.Itoa(v.ClusterID)
	a.mets.MessagesClassified.WithLabelValues(m.ChannelID, clusterIDLabel, string(v.Status)).Inc()

	switch v.Status {
	case detect.StatusKnown:
		a.updateClusterCadence(ctx, m.ChannelID, v.ClusterID, m.Timestamp)
	case detect.StatusUnknownPattern:
		a.raiseContentAlert(ctx, m, v, store.AlertKindUnknownPattern)
	case detect.StatusAbnormalContent:
		// Even though content drifted, the message still belongs to the
		// matched cluster's cadence stream. Update the baseline so a
		// real content shift doesn't also start tripping missing alerts.
		a.updateClusterCadence(ctx, m.ChannelID, v.ClusterID, m.Timestamp)
		a.raiseContentAlert(ctx, m, v, store.AlertKindAbnormalContent)
	}
}

func (a *App) updateClusterCadence(ctx context.Context, channelID string, clusterIndex int, ts time.Time) {
	a.mu.Lock()
	st, ok := a.clusterStates[clusterKey(channelID, clusterIndex)]
	a.mu.Unlock()
	if !ok {
		return
	}
	if !st.lastSeen.IsZero() && ts.After(st.lastSeen) {
		st.baseline.Add(ts.Sub(st.lastSeen).Seconds())
	}
	st.lastSeen = ts
	if err := a.store.UpdateClusterStats(ctx, st.dbID, ts, st.baseline.Mean(), st.baseline.Stddev()); err != nil {
		a.log.Error("update cluster stats", "err", err)
	}
}

func (a *App) raiseContentAlert(ctx context.Context, m connector.Message, v detect.Verdict, kind string) {
	if !a.takeCooldown(m.ChannelID, kind, m.Timestamp) {
		return
	}
	var clusterID *int64
	if v.ClusterID >= 0 {
		st := a.lookupClusterState(m.ChannelID, v.ClusterID)
		if st != nil {
			id := st.dbID
			clusterID = &id
		}
	}
	if clusterID == nil {
		// Unknown patterns don't have a matched cluster row to attach
		// to. We still want a record, but the schema requires either a
		// sender_id or a cluster_id. Use the sender path with the
		// content kind so the row is queryable later.
		if _, err := a.store.RaiseAlert(ctx, &store.Alert{
			Kind:     kind,
			SenderID: m.SenderID,
			ChannelID: m.ChannelID,
			RaisedAt: m.Timestamp,
		}); err != nil {
			a.log.Error("raise content alert", "err", err)
			return
		}
	} else {
		if _, err := a.store.RaiseClusterAlert(ctx, &store.Alert{
			Kind:      kind,
			ChannelID: m.ChannelID,
			ClusterID: clusterID,
			RaisedAt:  m.Timestamp,
		}); err != nil {
			a.log.Error("raise content alert", "err", err)
			return
		}
	}
	_ = a.conn.Post(ctx, a.cfg.Channels.AlertTo, formatContentAlert(m, v, kind))
}

func (a *App) lookupClusterState(channelID string, clusterIndex int) *clusterState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.clusterStates[clusterKey(channelID, clusterIndex)]
}

// takeCooldown returns true if a content alert of (channel, kind) may
// fire now and stamps the next allowed time. Returns false if still
// suppressed.
func (a *App) takeCooldown(channelID, kind string, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := cooldownKey(channelID, kind)
	if until, ok := a.cooldownUntil[key]; ok && now.Before(until) {
		return false
	}
	a.cooldownUntil[key] = now.Add(a.cfg.Alerts.CooldownPerKind)
	return true
}

// checkMissingPatterns scans every cluster on every monitored channel
// and raises a missing_pattern alert when silence has exceeded the
// offline multiplier of the cluster's mean cadence.
func (a *App) checkMissingPatterns(ctx context.Context, now time.Time) {
	a.mu.Lock()
	states := make([]*clusterState, 0, len(a.clusterStates))
	for _, st := range a.clusterStates {
		states = append(states, st)
	}
	a.mu.Unlock()

	for _, st := range states {
		mean := st.baseline.Mean()
		if mean <= 0 || st.baseline.Count() < 1 {
			continue
		}
		offlineAt := time.Duration(mean*a.params.OfflineMultiplier) * time.Second
		if a.params.HardCap > 0 && offlineAt > a.params.HardCap {
			offlineAt = a.params.HardCap
		}
		silence := now.Sub(st.lastSeen)
		if silence < offlineAt {
			continue
		}
		open, _ := a.store.HasOpenClusterAlert(ctx, st.channelID, st.dbID)
		if open {
			continue
		}
		if !a.takeCooldown(st.channelID, store.AlertKindMissingPattern, now) {
			continue
		}
		clusterID := st.dbID
		if _, err := a.store.RaiseClusterAlert(ctx, &store.Alert{
			Kind:                store.AlertKindMissingPattern,
			ChannelID:           st.channelID,
			ClusterID:           &clusterID,
			RaisedAt:            now,
			LastIntervalSeconds: silence.Seconds(),
		}); err != nil {
			a.log.Error("raise missing pattern", "err", err)
			continue
		}
		_ = a.conn.Post(ctx, a.cfg.Channels.AlertTo, formatMissingPattern(st, silence))
	}
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

func formatContentAlert(m connector.Message, v detect.Verdict, kind string) string {
	switch kind {
	case store.AlertKindUnknownPattern:
		return fmt.Sprintf(":grey_question: unknown pattern in <#%s> from <@%s>: %q",
			m.ChannelID, m.SenderID, truncate(m.Text, 200))
	case store.AlertKindAbnormalContent:
		return fmt.Sprintf(":warning: abnormal content in <#%s> (cluster %d, sim %.2f) missing tokens %v: %q",
			m.ChannelID, v.ClusterID, v.Similarity, v.MissingTokens, truncate(m.Text, 200))
	}
	return ""
}

func formatMissingPattern(st *clusterState, silence time.Duration) string {
	return fmt.Sprintf(":hourglass: cluster %d in <#%s> silent for %s (mean cadence %s)",
		st.clusterIndex, st.channelID,
		silence.Round(time.Second),
		time.Duration(st.baseline.Mean()*float64(time.Second)).Round(time.Second))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
