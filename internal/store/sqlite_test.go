package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func newStore(t *testing.T) Store {
	t.Helper()
	p := filepath.Join(t.TempDir(), "test.db")
	s, err := OpenSQLite(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestUpsertAndGetSender(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)
	sn := &Sender{
		SenderID:       "U1",
		ChannelID:      "C1",
		FirstSeen:      now,
		LastSeen:       now,
		IntervalMean:   60,
		IntervalStddev: 5,
		MsgCount:       1,
		State:          StateLearning,
		StateEnteredAt: now,
	}
	if err := s.UpsertSender(ctx, sn); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetSender(ctx, "U1", "C1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("got nil")
	}
	if got.State != StateLearning || got.IntervalMean != 60 {
		t.Errorf("unexpected: %+v", got)
	}

	sn.State = StateHealthy
	sn.MsgCount = 25
	sn.BaselineReady = true
	if err := s.UpsertSender(ctx, sn); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSender(ctx, "U1", "C1")
	if got.State != StateHealthy || got.MsgCount != 25 || !got.BaselineReady {
		t.Errorf("after update: %+v", got)
	}
}

func TestGetSender_missing(t *testing.T) {
	s := newStore(t)
	got, err := s.GetSender(context.Background(), "none", "none")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestAlertLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)

	open, err := s.HasOpenAlert(ctx, "U1", "C1")
	if err != nil {
		t.Fatal(err)
	}
	if open {
		t.Fatal("expected no open alert")
	}

	id, err := s.RaiseAlert(ctx, &Alert{
		Kind:     AlertKindFrequency,
		SenderID: "U1", ChannelID: "C1",
		State: StateOffline, RaisedAt: now,
		LastIntervalSeconds: 600,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected id")
	}

	open, _ = s.HasOpenAlert(ctx, "U1", "C1")
	if !open {
		t.Fatal("expected open alert")
	}

	if err := s.ClearOpenAlerts(ctx, "U1", "C1", now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	open, _ = s.HasOpenAlert(ctx, "U1", "C1")
	if open {
		t.Fatal("expected cleared")
	}
}

func TestListSenders(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)
	for _, id := range []string{"U1", "U2", "U3"} {
		if err := s.UpsertSender(ctx, &Sender{
			SenderID: id, ChannelID: "C1",
			FirstSeen: now, LastSeen: now,
			State: StateHealthy, StateEnteredAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	ss, err := s.ListSenders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ss) != 3 {
		t.Errorf("len=%d", len(ss))
	}
}

func TestUpsertAndListClusters(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)

	c := &Cluster{
		ChannelID:      "C1",
		ClusterIndex:   0,
		Size:           4,
		SampleMessage:  "deploy succeeded",
		Centroid:       map[string]float64{"deploy": 0.7, "succeeded": 0.6},
		StableTokens:   []string{"deploy", "succeeded"},
		LastMessageAt:  &now,
		IntervalMean:   120,
		IntervalStddev: 8,
	}
	if err := s.UpsertCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if c.ID == 0 {
		t.Fatal("expected id assigned by upsert")
	}

	got, err := s.GetClusterByIndex(ctx, "C1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("got nil")
	}
	if got.Size != 4 || got.SampleMessage != "deploy succeeded" {
		t.Errorf("unexpected: %+v", got)
	}
	if got.Centroid["deploy"] != 0.7 {
		t.Errorf("centroid round-trip: %+v", got.Centroid)
	}
	if len(got.StableTokens) != 2 {
		t.Errorf("stable tokens: %+v", got.StableTokens)
	}

	// Insert another cluster, then list.
	c2 := &Cluster{
		ChannelID:     "C1",
		ClusterIndex:  1,
		Size:          3,
		SampleMessage: "health check ok",
		Centroid:      map[string]float64{"health": 0.5},
		StableTokens:  []string{"health", "check"},
	}
	if err := s.UpsertCluster(ctx, c2); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListClusters(ctx, "C1")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("len=%d", len(list))
	}

	// Upsert overwrites.
	c.Size = 7
	c.SampleMessage = "deploy succeeded again"
	if err := s.UpsertCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetClusterByIndex(ctx, "C1", 0)
	if got.Size != 7 {
		t.Errorf("size after upsert: %d", got.Size)
	}
}

func TestUpdateClusterStats(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	c := &Cluster{
		ChannelID:     "C1",
		ClusterIndex:  0,
		Size:          3,
		SampleMessage: "x",
		Centroid:      map[string]float64{"a": 1},
		StableTokens:  []string{"a"},
	}
	if err := s.UpsertCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	if err := s.UpdateClusterStats(ctx, c.ID, now, 99, 11); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetClusterByIndex(ctx, "C1", 0)
	if got.IntervalMean != 99 || got.IntervalStddev != 11 {
		t.Errorf("stats not persisted: %+v", got)
	}
	if got.LastMessageAt == nil || !got.LastMessageAt.Equal(now) {
		t.Errorf("last_message_at not persisted: %v", got.LastMessageAt)
	}
}

func TestClusterAlertLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)

	cid := int64(42)
	open, _ := s.HasOpenClusterAlert(ctx, "C1", cid)
	if open {
		t.Fatal("expected no open cluster alert")
	}

	id, err := s.RaiseClusterAlert(ctx, &Alert{
		Kind:      AlertKindMissingPattern,
		ChannelID: "C1",
		ClusterID: &cid,
		RaisedAt:  now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected id")
	}

	open, _ = s.HasOpenClusterAlert(ctx, "C1", cid)
	if !open {
		t.Fatal("expected open cluster alert")
	}

	if err := s.ClearOpenClusterAlerts(ctx, "C1", cid, now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	open, _ = s.HasOpenClusterAlert(ctx, "C1", cid)
	if open {
		t.Fatal("expected cleared")
	}
}

func TestClusterAlert_requiresClusterID(t *testing.T) {
	s := newStore(t)
	if _, err := s.RaiseClusterAlert(context.Background(), &Alert{
		Kind:      AlertKindMissingPattern,
		ChannelID: "C1",
	}); err == nil {
		t.Fatal("expected error when ClusterID nil")
	}
}

func TestSenderAndClusterAlertsCoexist(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	now := time.Now().Truncate(time.Second)

	if _, err := s.RaiseAlert(ctx, &Alert{
		Kind:     AlertKindFrequency,
		SenderID: "U1", ChannelID: "C1",
		State: StateOffline, RaisedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	cid := int64(7)
	if _, err := s.RaiseClusterAlert(ctx, &Alert{
		Kind:      AlertKindUnknownPattern,
		ChannelID: "C1",
		ClusterID: &cid,
		RaisedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}
	openS, _ := s.HasOpenAlert(ctx, "U1", "C1")
	openC, _ := s.HasOpenClusterAlert(ctx, "C1", cid)
	if !openS || !openC {
		t.Errorf("both should be open: sender=%v cluster=%v", openS, openC)
	}
}

// TestMigrate_oldAlertsTable creates a database with the pre-Phase-4
// alerts schema by hand, opens it through OpenSQLite, and verifies that
// existing rows survive and that the new columns are usable.
func TestMigrate_oldAlertsTable(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "old.db")

	raw, err := sql.Open("sqlite", p+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := `
CREATE TABLE alerts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id                TEXT    NOT NULL,
    channel_id               TEXT    NOT NULL,
    state                    TEXT    NOT NULL,
    raised_at                INTEGER NOT NULL,
    cleared_at               INTEGER,
    last_interval_seconds    REAL    NOT NULL DEFAULT 0
);
INSERT INTO alerts (sender_id, channel_id, state, raised_at, last_interval_seconds)
VALUES ('Uold', 'Cold', 'offline', 1700000000, 600);
`
	if _, err := raw.ExecContext(ctx, oldSchema); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	s, err := OpenSQLite(ctx, p)
	if err != nil {
		t.Fatalf("open after migrate: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Pre-existing row preserved and tagged frequency.
	open, err := s.HasOpenAlert(ctx, "Uold", "Cold")
	if err != nil {
		t.Fatal(err)
	}
	if !open {
		t.Errorf("expected migrated row to remain open")
	}

	// New cluster alerts work after migration.
	cid := int64(99)
	if _, err := s.RaiseClusterAlert(ctx, &Alert{
		Kind:      AlertKindMissingPattern,
		ChannelID: "Cnew",
		ClusterID: &cid,
		RaisedAt:  time.Unix(1700000100, 0),
	}); err != nil {
		t.Fatalf("cluster alert after migrate: %v", err)
	}
}
