package alerter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
)

type fakeSink struct {
	name  string
	calls int32
	err   error
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Send(_ context.Context, _ Alert) error {
	atomic.AddInt32(&f.calls, 1)
	return f.err
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMulti_callsAllSinks(t *testing.T) {
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b"}
	m := NewMulti(quietLogger(), a, b)
	if err := m.Send(context.Background(), Alert{}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if atomic.LoadInt32(&a.calls) != 1 || atomic.LoadInt32(&b.calls) != 1 {
		t.Errorf("expected both sinks called, got a=%d b=%d", a.calls, b.calls)
	}
}

func TestMulti_oneFailsOthersStillRun(t *testing.T) {
	a := &fakeSink{name: "a", err: errors.New("boom")}
	b := &fakeSink{name: "b"}
	m := NewMulti(quietLogger(), a, b)
	err := m.Send(context.Background(), Alert{})
	if err == nil {
		t.Fatal("expected joined error")
	}
	if !strings.Contains(err.Error(), "a:") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected sink-name prefix in err, got %v", err)
	}
	if atomic.LoadInt32(&b.calls) != 1 {
		t.Errorf("second sink should still run, got %d calls", b.calls)
	}
}

func TestMulti_emptySinks(t *testing.T) {
	m := NewMulti(quietLogger())
	if err := m.Send(context.Background(), Alert{}); err != nil {
		t.Errorf("empty multi should be a no-op, got %v", err)
	}
}
