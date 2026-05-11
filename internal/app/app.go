// Package app wires config, store, detect, connector, and metrics into a
// running worker. main.go stays a thin entrypoint.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alexmchughdev/foghorn/internal/alerter"
	emailalerter "github.com/alexmchughdev/foghorn/internal/alerter/email"
	slackalerter "github.com/alexmchughdev/foghorn/internal/alerter/slack"
	"github.com/alexmchughdev/foghorn/internal/api"
	"github.com/alexmchughdev/foghorn/internal/cluster"
	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/connector"
	"github.com/alexmchughdev/foghorn/internal/detect"
	"github.com/alexmchughdev/foghorn/internal/metrics"
	"github.com/alexmchughdev/foghorn/internal/store"
)

// BuildAlerter constructs the multi-sink alerter from config. Each entry
// in cfg.Alerters becomes one sink; the result is a Multi that fans out
// to all sinks concurrently.
func BuildAlerter(cfg *config.Config, log *slog.Logger) (*alerter.Multi, error) {
	sinks := make([]alerter.Alerter, 0, len(cfg.Alerters))
	for _, ac := range cfg.Alerters {
		s, err := buildOne(ac)
		if err != nil {
			return nil, fmt.Errorf("alerter %q: %w", ac.Name, err)
		}
		sinks = append(sinks, s)
	}
	return alerter.NewMulti(log, sinks...), nil
}

func buildOne(ac config.AlerterConfig) (alerter.Alerter, error) {
	switch ac.Type {
	case "slack":
		token := os.Getenv(ac.BotTokenEnv)
		return slackalerter.New(slackalerter.Options{
			Name:     ac.Name,
			Token:    token,
			Channels: ac.Channels,
		})
	case "email":
		return emailalerter.New(emailalerter.Options{
			Name:     ac.Name,
			Host:     ac.SMTPHost,
			Port:     ac.SMTPPort,
			User:     os.Getenv(ac.UserEnv),
			Password: os.Getenv(ac.PasswordEnv),
			From:     ac.From,
			To:       ac.To,
		})
	default:
		return nil, fmt.Errorf("unknown alerter type %q", ac.Type)
	}
}

const tickInterval = 30 * time.Second

type App struct {
	cfg     *config.Config
	store   store.Store
	conns   []connector.Connector
	alerter alerter.Alerter
	mets    *metrics.Metrics
	log     *slog.Logger
	params  detect.Params

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
	// watermark[channelID] is the highest Slack timestamp returned by
	// the boot-time History scan on that channel. Live-stream messages
	// with ts <= watermark are dropped before any state mutation:
	// Socket Mode redelivers buffered events on reconnect, so the same
	// message can arrive via both History and Stream within one boot.
	watermark map[string]time.Time
}

type clusterState struct {
	dbID         int64
	channelID    string
	clusterIndex int
	baseline     *detect.Baseline
	lastSeen     time.Time
}

func New(cfg *config.Config, st store.Store, conns []connector.Connector, a alerter.Alerter, m *metrics.Metrics, log *slog.Logger) *App {
	return &App{
		cfg:              cfg,
		store:            st,
		conns:            conns,
		alerter:          a,
		mets:             m,
		log:              log,
		params:           detect.FromConfig(cfg.Detection),
		baselines:        make(map[string]*detect.Baseline),
		classifiers:      make(map[string]*detect.Classifier),
		clusterStates:    make(map[string]*clusterState),
		cooldownUntil:    make(map[string]time.Time),
		historyByChannel: make(map[string][]connector.Message),
		watermark:        make(map[string]time.Time),
	}
}

// Run blocks until ctx is cancelled. It boots state, backfills history,
// learns content clusters, then runs the metrics server, connector
// stream, ingest/tick loop, and HTTP API as cooperating goroutines.
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

	apiToken, err := a.cfg.APIToken()
	if err != nil {
		return fmt.Errorf("api token: %w", err)
	}
	apiSrv := api.New(a.store, a.RelearnChannel, a.cfg.API.Addr, apiToken, api.BuildSHA())

	messages := make(chan connector.Message, 256)

	// metrics + loop + api + one per connector
	goroutines := 3 + len(a.conns)
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	go func() {
		defer wg.Done()
		if err := a.mets.Serve(ctx, a.cfg.Metrics.Addr); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("metrics: %w", err)
		}
	}()
	for _, c := range a.conns {
		c := c
		go func() {
			defer wg.Done()
			if err := c.Stream(ctx, messages); err != nil && ctx.Err() == nil {
				errCh <- fmt.Errorf("connector %s: %w", c.Name(), err)
			}
		}()
	}
	go func() {
		defer wg.Done()
		a.loop(ctx, messages)
	}()
	go func() {
		defer wg.Done()
		if err := apiSrv.Serve(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("api: %w", err)
		}
	}()

	a.log.Info("api listening", "addr", a.cfg.API.Addr)

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
			if a.shouldSkipLive(m) {
				a.mets.MessagesSkipped.WithLabelValues(m.ChannelID, "backfill_overlap").Inc()
				a.log.Debug("skip pre-watermark live message",
					"channel", m.ChannelID, "ts", m.Timestamp)
				continue
			}
			a.onMessage(ctx, m)
		case now := <-tick.C:
			a.onTick(ctx, now)
		}
	}
}

// shouldSkipLive returns true when the live-stream message has a
// timestamp at or before the channel's backfill watermark. Slack's
// Socket Mode redelivers events buffered while the bot was offline,
// so messages already covered by History can arrive a second time via
// Stream. Re-ingesting them would inflate sender msg_counts, double-
// classify, and (worst) bias cluster cadence baselines toward whatever
// rate Slack happens to replay at. The check is strict <=: timestamps
// equal to the watermark are the exact backfilled messages.
func (a *App) shouldSkipLive(m connector.Message) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, ok := a.watermark[m.ChannelID]
	if !ok {
		return false
	}
	return !m.Timestamp.After(w)
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
		_ = a.alerter.Send(ctx, a.frequencyAlert(sn, d, now))
	case store.StateRecovering:
		_ = a.store.ClearOpenAlerts(ctx, sn.SenderID, sn.ChannelID, now)
		_ = a.alerter.Send(ctx, a.recoveryAlert(sn, now))
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
	for _, c := range a.conns {
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
			if w, ok := a.watermark[m.ChannelID]; !ok || m.Timestamp.After(w) {
				a.watermark[m.ChannelID] = m.Timestamp
			}
		}
	}
	return nil
}

// learnClusters builds a content-cluster fingerprint set per channel
// from the buffered backfill, persists each cluster, and seeds per-
// cluster cadence baselines. After this returns, the channel's
// classifier is non-nil and live messages will be classified.
func (a *App) learnClusters(ctx context.Context) error {
	for channelID, msgs := range a.historyByChannel {
		if err := a.buildAndInstallClusters(ctx, channelID, msgs); err != nil {
			return err
		}
	}
	// Free the backfill buffer once learning is done.
	a.historyByChannel = nil
	return nil
}

// buildAndInstallClusters runs the cluster pipeline for one channel:
// vectorise, DBSCAN, persist to the store, then atomically install the
// classifier and per-cluster state. Used by both the boot-time learn
// pass and the on-demand /relearn API.
func (a *App) buildAndInstallClusters(ctx context.Context, channelID string, msgs []connector.Message) error {
	opts := cluster.BuildOptions{
		Epsilon: a.cfg.Cluster.Epsilon,
		MinPts:  a.cfg.Cluster.MinPts,
	}
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
	classifier := detect.NewClassifier(fps, vec, detect.ClassifierOptions{
		Threshold:   a.cfg.Cluster.MatchThreshold,
		StableRatio: a.cfg.Cluster.StableRatio,
	})

	// Persist new cluster rows first so we have assigned DB IDs to attach
	// to the in-memory state.
	newStates := make(map[string]*clusterState, len(clusters))
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
		newStates[clusterKey(channelID, c.ID)] = st
	}

	a.mu.Lock()
	a.classifiers[channelID] = classifier
	for k, st := range a.clusterStates {
		if st.channelID == channelID {
			delete(a.clusterStates, k)
		}
	}
	for k, st := range newStates {
		a.clusterStates[k] = st
	}
	a.mu.Unlock()

	a.mets.ClustersTotal.WithLabelValues(channelID).Set(float64(len(clusters)))
	a.log.Info("clusters learned",
		"channel", channelID, "messages", len(texts), "clusters", len(clusters))
	return nil
}

// RelearnChannel drops the persisted clusters for one channel, pulls
// the current lookback of history from the connector, and rebuilds the
// classifier in place. Open cluster alerts on the channel are cleared
// since their cluster_id rows are about to vanish. Senders and
// frequency alerts are untouched.
func (a *App) RelearnChannel(ctx context.Context, channelID string) error {
	conn := a.connectorForChannel(channelID)
	if conn == nil {
		return fmt.Errorf("channel %s is not monitored", channelID)
	}
	since := time.Now().Add(-a.cfg.Learning.Lookback)
	all, err := conn.History(ctx, since)
	if err != nil {
		return fmt.Errorf("history: %w", err)
	}
	msgs := make([]connector.Message, 0, len(all))
	for _, m := range all {
		if m.ChannelID == channelID {
			msgs = append(msgs, m)
		}
	}

	now := time.Now()
	if err := a.store.ClearOpenClusterAlertsByChannel(ctx, channelID, now); err != nil {
		return fmt.Errorf("clear cluster alerts: %w", err)
	}
	if err := a.store.DeleteClustersByChannel(ctx, channelID); err != nil {
		return fmt.Errorf("delete clusters: %w", err)
	}

	a.mu.Lock()
	for k := range a.cooldownUntil {
		if strings.HasPrefix(k, channelID+":") {
			delete(a.cooldownUntil, k)
		}
	}
	a.mu.Unlock()

	a.log.Info("relearn requested", "channel", channelID, "messages", len(msgs))
	return a.buildAndInstallClusters(ctx, channelID, msgs)
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
	_ = a.alerter.Send(ctx, a.contentAlert(m, v, kind))
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
		_ = a.alerter.Send(ctx, a.missingPatternAlert(st, silence, now))
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

// connectorForChannel returns the connector that monitors channelID,
// or nil if no live connector claims it. Asks each connector for its
// resolved channel set rather than reading config, because config may
// store names that haven't yet been resolved to IDs. Multiple
// connectors monitoring the same channel isn't a supported topology;
// the first match wins.
func (a *App) connectorForChannel(channelID string) connector.Connector {
	for _, c := range a.conns {
		for _, id := range c.Monitored() {
			if id == channelID {
				return c
			}
		}
	}
	return nil
}

// connectorNameForChannel returns the live connector name that owns
// channelID, or "" if no match. Used to stamp outbound alerts.
func (a *App) connectorNameForChannel(channelID string) string {
	if c := a.connectorForChannel(channelID); c != nil {
		return c.Name()
	}
	return ""
}

// frequencyAlert builds the typed alert for a drift/offline transition.
func (a *App) frequencyAlert(sn *store.Sender, d detect.Decision, now time.Time) alerter.Alert {
	silence := time.Duration(d.SilentSeconds * float64(time.Second)).Round(time.Second)
	severity := alerter.SeverityWarning
	title := fmt.Sprintf("%s drifting in %s", sn.SenderID, sn.ChannelID)
	body := fmt.Sprintf("Silent for %s. Baseline cadence: %s.",
		silence,
		time.Duration(sn.IntervalMean*float64(time.Second)).Round(time.Second))
	if d.To == store.StateOffline {
		severity = alerter.SeverityCritical
		title = fmt.Sprintf("%s offline in %s", sn.SenderID, sn.ChannelID)
		body = fmt.Sprintf("Silent for %s.", silence)
	}
	return alerter.Alert{
		Severity:  severity,
		Title:     title,
		Body:      body,
		SenderID:  sn.SenderID,
		ChannelID: sn.ChannelID,
		Connector: a.connectorNameForChannel(sn.ChannelID),
		Kind:      store.AlertKindFrequency,
		RaisedAt:  now,
	}
}

func (a *App) recoveryAlert(sn *store.Sender, now time.Time) alerter.Alert {
	return alerter.Alert{
		Severity:  alerter.SeverityInfo,
		Title:     fmt.Sprintf("%s recovered in %s", sn.SenderID, sn.ChannelID),
		SenderID:  sn.SenderID,
		ChannelID: sn.ChannelID,
		Connector: a.connectorNameForChannel(sn.ChannelID),
		Kind:      store.AlertKindFrequency,
		RaisedAt:  now,
	}
}

func (a *App) contentAlert(m connector.Message, v detect.Verdict, kind string) alerter.Alert {
	al := alerter.Alert{
		Severity:  alerter.SeverityWarning,
		SenderID:  m.SenderID,
		ChannelID: m.ChannelID,
		Connector: m.Connector,
		Kind:      kind,
		RaisedAt:  m.Timestamp,
	}
	switch kind {
	case store.AlertKindUnknownPattern:
		al.Title = fmt.Sprintf("Unknown pattern in %s", m.ChannelID)
		al.Body = fmt.Sprintf("From %s: %q", m.SenderID, truncate(m.Text, 200))
	case store.AlertKindAbnormalContent:
		al.Title = fmt.Sprintf("Abnormal content in %s (cluster %d)", m.ChannelID, v.ClusterID)
		al.Body = fmt.Sprintf("Similarity %.2f, missing stable tokens %v.\nMessage: %q",
			v.Similarity, v.MissingTokens, truncate(m.Text, 200))
	}
	return al
}

func (a *App) missingPatternAlert(st *clusterState, silence time.Duration, now time.Time) alerter.Alert {
	return alerter.Alert{
		Severity: alerter.SeverityCritical,
		Title:    fmt.Sprintf("Cluster %d in %s missing", st.clusterIndex, st.channelID),
		Body: fmt.Sprintf("Silent for %s. Mean cadence: %s.",
			silence.Round(time.Second),
			time.Duration(st.baseline.Mean()*float64(time.Second)).Round(time.Second)),
		ChannelID: st.channelID,
		Connector: a.connectorNameForChannel(st.channelID),
		Kind:      store.AlertKindMissingPattern,
		RaisedAt:  now,
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
