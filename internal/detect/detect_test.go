package detect

import (
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/store"
)

func defaultParams() Params {
	return Params{
		LearningMessages:  5,
		DriftSigma:        3,
		OfflineMultiplier: 5,
		HardCap:           30 * time.Minute,
	}
}

func TestBaseline_meanStddev(t *testing.T) {
	b := NewBaseline()
	for _, x := range []float64{10, 12, 11, 9, 10, 11} {
		b.Add(x)
	}
	if got := b.Mean(); got < 10 || got > 11 {
		t.Errorf("mean out of range: %v", got)
	}
	if b.Stddev() <= 0 {
		t.Errorf("stddev should be > 0")
	}
}

func TestBaseline_wraps(t *testing.T) {
	b := NewBaseline()
	for i := 0; i < 250; i++ {
		b.Add(float64(100 + i%2))
	}
	// After wrap the window mean should hover around 100.5.
	if m := b.Mean(); m < 100 || m > 101 {
		t.Errorf("mean after wrap: %v", m)
	}
	if b.Count() != 250 {
		t.Errorf("total seen: %d", b.Count())
	}
}

func TestOnMessage_learningThenHealthy(t *testing.T) {
	p := defaultParams()
	b := NewBaseline()
	s := &store.Sender{SenderID: "U1", ChannelID: "C1", State: store.StateLearning}
	start := time.Unix(1_700_000_000, 0)

	for i := 0; i < p.LearningMessages-1; i++ {
		OnMessage(s, b, start.Add(time.Duration(i)*time.Minute), Override{}, p)
	}
	if s.State != store.StateLearning {
		t.Errorf("should still be learning: %v", s.State)
	}

	d := OnMessage(s, b, start.Add(time.Duration(p.LearningMessages)*time.Minute), Override{}, p)
	if !d.Transition || d.To != store.StateHealthy {
		t.Errorf("expected learning to healthy, got %+v", d)
	}
	if !s.BaselineReady {
		t.Errorf("baseline should be ready")
	}
}

func TestOnMessage_overrideSkipsLearning(t *testing.T) {
	p := defaultParams()
	b := NewBaseline()
	s := &store.Sender{SenderID: "U1", ChannelID: "C1", State: store.StateLearning}
	ov := Override{HasInterval: true, Interval: 5 * time.Minute}

	d := OnMessage(s, b, time.Unix(1_700_000_000, 0), ov, p)
	if d.To != store.StateHealthy || !s.BaselineReady {
		t.Errorf("override should jump straight to healthy: %+v", d)
	}
	if s.IntervalMean != 300 {
		t.Errorf("override should pin mean to 300s, got %v", s.IntervalMean)
	}
}

func TestOnTick_drift(t *testing.T) {
	p := defaultParams()
	s := &store.Sender{
		SenderID: "U1", ChannelID: "C1",
		State:          store.StateHealthy,
		BaselineReady:  true,
		IntervalMean:   60,
		IntervalStddev: 5,
		LastSeen:       time.Unix(1_700_000_000, 0),
	}
	now := s.LastSeen.Add(80 * time.Second) // past mean+3σ=75
	d := OnTick(s, now, Override{}, p)
	if !d.Transition || d.To != store.StateDrifting {
		t.Errorf("expected drift: %+v", d)
	}
}

func TestOnTick_offlineHardCap(t *testing.T) {
	p := defaultParams()
	s := &store.Sender{
		SenderID: "U1", ChannelID: "C1",
		State:         store.StateHealthy,
		BaselineReady: true,
		IntervalMean:  60 * 60, // 1h
		LastSeen:      time.Unix(1_700_000_000, 0),
	}
	// 5× mean = 5h, but hard cap clamps at 30m.
	now := s.LastSeen.Add(31 * time.Minute)
	d := OnTick(s, now, Override{}, p)
	if d.To != store.StateOffline {
		t.Errorf("hard cap should force offline: %+v", d)
	}
}

func TestOnMessage_recovery(t *testing.T) {
	p := defaultParams()
	b := NewBaseline()
	s := &store.Sender{
		SenderID: "U1", ChannelID: "C1",
		State:          store.StateOffline,
		BaselineReady:  true,
		IntervalMean:   60,
		IntervalStddev: 5,
		LastSeen:       time.Unix(1_700_000_000, 0),
		MsgCount:       50,
	}
	// First message after offline transitions to recovering.
	d := OnMessage(s, b, s.LastSeen.Add(10*time.Minute), Override{}, p)
	if d.To != store.StateRecovering || !d.Transition {
		t.Fatalf("expected recovering: %+v", d)
	}
	// Next message transitions to healthy.
	d = OnMessage(s, b, s.LastSeen.Add(time.Minute), Override{}, p)
	if d.To != store.StateHealthy || !d.Transition {
		t.Fatalf("expected healthy: %+v", d)
	}
}
