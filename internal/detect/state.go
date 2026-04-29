package detect

import (
	"time"

	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/store"
)

// Params holds the numeric knobs used during detection, decoupled from
// config.DetectionConfig so this package has no YAML dependency.
type Params struct {
	LearningMessages  int
	DriftSigma        float64
	OfflineMultiplier float64
	HardCap           time.Duration
}

func FromConfig(c config.DetectionConfig) Params {
	return Params{
		LearningMessages:  c.LearningMessages,
		DriftSigma:        c.DriftSigma,
		OfflineMultiplier: c.OfflineMultiplier,
		HardCap:           c.HardCap,
	}
}

// Decision is the outcome of evaluating one sender. Transition is true
// when the state changed and the caller should raise or clear an alert.
type Decision struct {
	From       store.SenderState
	To         store.SenderState
	Transition bool
	// Silence at evaluation time in seconds, used for alert text only.
	SilentSeconds float64
}

// Override carries per-sender config overrides. Interval pins the
// baseline to a known cadence and skips learning. Priority is opaque
// here and surfaced to callers for alert routing.
type Override struct {
	HasInterval bool
	Interval    time.Duration
	Priority    string
}

// OnMessage evaluates state when a new message arrives. The sender
// record is mutated in place. Callers persist the result and act on
// the returned Decision.
func OnMessage(s *store.Sender, b *Baseline, at time.Time, ov Override, p Params) Decision {
	d := Decision{From: s.State}

	if !s.FirstSeen.IsZero() && !s.LastSeen.IsZero() && at.After(s.LastSeen) {
		b.Add(at.Sub(s.LastSeen).Seconds())
	}
	if s.FirstSeen.IsZero() {
		s.FirstSeen = at
	}
	s.LastSeen = at
	s.MsgCount++
	s.IntervalMean = b.BlendedMean()
	s.IntervalStddev = b.Stddev()

	// A message arriving while drifting/offline is a recovery.
	switch s.State {
	case store.StateDrifting, store.StateOffline:
		d.To = store.StateRecovering
		d.Transition = true
		s.State = store.StateRecovering
		s.StateEnteredAt = at
		return d
	case store.StateRecovering:
		d.To = store.StateHealthy
		d.Transition = true
		s.State = store.StateHealthy
		s.StateEnteredAt = at
		return d
	}

	switch {
	case ov.HasInterval:
		// Pin baseline to override so OnTick uses a stable mean.
		s.BaselineReady = true
		s.IntervalMean = ov.Interval.Seconds()
		s.IntervalStddev = 0
	case s.MsgCount >= p.LearningMessages:
		s.BaselineReady = true
	}

	target := store.StateLearning
	if s.BaselineReady {
		target = store.StateHealthy
	}
	if s.State != target {
		d.To = target
		d.Transition = true
		s.State = target
		s.StateEnteredAt = at
	} else {
		d.To = s.State
	}
	return d
}

// OnTick evaluates state from the clock alone. The periodic timer
// runs this for every known sender so drifts and offlines fire even
// when the next message never arrives.
func OnTick(s *store.Sender, at time.Time, ov Override, p Params) Decision {
	d := Decision{From: s.State}
	if !s.BaselineReady || s.LastSeen.IsZero() {
		d.To = s.State
		return d
	}

	silence := at.Sub(s.LastSeen)
	d.SilentSeconds = silence.Seconds()
	mean := time.Duration(s.IntervalMean * float64(time.Second))
	stddev := time.Duration(s.IntervalStddev * float64(time.Second))
	if ov.HasInterval {
		mean = ov.Interval
		stddev = 0
	}

	driftAt := mean + time.Duration(p.DriftSigma*float64(stddev))
	offlineAt := time.Duration(float64(mean) * p.OfflineMultiplier)
	if p.HardCap > 0 && offlineAt > p.HardCap {
		offlineAt = p.HardCap
	}

	var target store.SenderState
	switch {
	case silence >= offlineAt:
		target = store.StateOffline
	case silence >= driftAt:
		target = store.StateDrifting
	default:
		target = store.StateHealthy
	}

	// Don't downgrade offline to drifting on a tick. Wait for a message.
	if s.State == store.StateOffline && target == store.StateDrifting {
		target = store.StateOffline
	}

	if s.State != target {
		d.Transition = true
		d.To = target
		s.State = target
		s.StateEnteredAt = at
	} else {
		d.To = s.State
	}
	return d
}
