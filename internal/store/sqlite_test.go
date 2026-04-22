package store

import (
	"context"
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
