package app

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/alerter"
	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/connector"
	"github.com/alexmchughdev/foghorn/internal/metrics"
	"github.com/alexmchughdev/foghorn/internal/store"
)

type fakeConnector struct {
	history []connector.Message
}

func (f *fakeConnector) Name() string     { return "fake" }
func (f *fakeConnector) Platform() string { return "fake" }
func (f *fakeConnector) History(_ context.Context, _ time.Time) ([]connector.Message, error) {
	return f.history, nil
}
func (f *fakeConnector) Stream(ctx context.Context, _ chan<- connector.Message) error {
	<-ctx.Done()
	return ctx.Err()
}
func (f *fakeConnector) Post(_ context.Context, _, _ string) error { return nil }
func (f *fakeConnector) Close() error                              { return nil }

type fakeAlerter struct{}

func (fakeAlerter) Name() string                              { return "fake" }
func (fakeAlerter) Send(_ context.Context, _ alerter.Alert) error { return nil }

func newAppForTest(t *testing.T, conn connector.Connector) *App {
	t.Helper()
	cfg := &config.Config{
		Detection: config.DetectionConfig{
			LearningMessages:  5,
			DriftSigma:        3,
			OfflineMultiplier: 5,
			HardCap:           30 * time.Minute,
		},
		Learning: config.LearningConfig{Lookback: time.Hour},
		Cluster: config.ClusterConfig{
			Epsilon: 0.4, MinPts: 3,
			MatchThreshold: 0.5, StableRatio: 0.8,
		},
		Alerts: config.AlertsConfig{CooldownPerKind: 30 * time.Second},
	}
	p := filepath.Join(t.TempDir(), "app.db")
	st, err := store.OpenSQLite(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	m := metrics.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(cfg, st, []connector.Connector{conn}, fakeAlerter{}, m, log)
}

// TestWatermark_skipsBackfillOverlap covers the central failure mode
// from the dev-workspace validation run: backfill returns ts={A,B,C},
// then Socket Mode redelivers {B,C,D} from its catch-up buffer, and
// only D should reach onMessage.
func TestWatermark_skipsBackfillOverlap(t *testing.T) {
	tsA := time.Unix(1000, 0)
	tsB := time.Unix(2000, 500_000_000) // .5s past tsB to assert sub-second precision
	tsC := time.Unix(3000, 0)
	tsD := time.Unix(4000, 0)

	conn := &fakeConnector{
		history: []connector.Message{
			{Platform: "fake", ChannelID: "C1", SenderID: "U1", Timestamp: tsA, Text: "a"},
			{Platform: "fake", ChannelID: "C1", SenderID: "U1", Timestamp: tsB, Text: "b"},
			{Platform: "fake", ChannelID: "C1", SenderID: "U1", Timestamp: tsC, Text: "c"},
		},
	}
	a := newAppForTest(t, conn)

	if err := a.backfillHistory(context.Background()); err != nil {
		t.Fatalf("backfillHistory: %v", err)
	}

	got, ok := a.watermark["C1"]
	if !ok {
		t.Fatal("watermark for C1 not set")
	}
	if !got.Equal(tsC) {
		t.Errorf("watermark: got %v want %v", got, tsC)
	}

	// Replay path: B and C should be skipped (B is before, C is at
	// the watermark — strict <=). D is past it and must pass.
	cases := []struct {
		ts      time.Time
		wantSkip bool
		label    string
	}{
		{tsB, true, "before watermark"},
		{tsC, true, "equal to watermark"},
		{tsD, false, "after watermark"},
	}
	for _, tc := range cases {
		got := a.shouldSkipLive(connector.Message{ChannelID: "C1", Timestamp: tc.ts})
		if got != tc.wantSkip {
			t.Errorf("%s: shouldSkipLive(ts=%v) = %v, want %v",
				tc.label, tc.ts, got, tc.wantSkip)
		}
	}
}

// TestWatermark_unseenChannelPasses guards against the "no backfill
// data on this channel" case — a brand-new monitored channel must not
// be silently filtered just because it has no watermark entry yet.
func TestWatermark_unseenChannelPasses(t *testing.T) {
	a := newAppForTest(t, &fakeConnector{})
	if err := a.backfillHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.shouldSkipLive(connector.Message{ChannelID: "C-new", Timestamp: time.Unix(5000, 0)}) {
		t.Error("expected pass on channel with no watermark")
	}
}

// TestWatermark_perChannelIsolation makes sure C1's watermark doesn't
// shadow live messages on C2 and vice versa.
func TestWatermark_perChannelIsolation(t *testing.T) {
	tsLow := time.Unix(1000, 0)
	tsHigh := time.Unix(5000, 0)
	conn := &fakeConnector{
		history: []connector.Message{
			{ChannelID: "C1", SenderID: "U1", Timestamp: tsLow, Text: "x"},
			{ChannelID: "C2", SenderID: "U1", Timestamp: tsHigh, Text: "y"},
		},
	}
	a := newAppForTest(t, conn)
	if err := a.backfillHistory(context.Background()); err != nil {
		t.Fatal(err)
	}

	// A live message on C1 at tsHigh is well past C1's watermark and
	// must pass, even though it lands at C2's watermark.
	if a.shouldSkipLive(connector.Message{ChannelID: "C1", Timestamp: tsHigh}) {
		t.Error("C1 live message past its own watermark should pass")
	}
	// A live message on C2 at tsLow is well before C2's watermark and
	// must be skipped, even though it's above C1's watermark.
	if !a.shouldSkipLive(connector.Message{ChannelID: "C2", Timestamp: tsLow}) {
		t.Error("C2 live message before its own watermark should skip")
	}
}
