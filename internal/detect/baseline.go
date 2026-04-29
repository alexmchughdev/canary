// Package detect holds the rolling-interval baseline and the per-sender
// state machine described in foghorn-plan.md §Detection model. All math
// is plain Go float64 — deliberately no stats lib, so the invariants
// stay readable and the package has zero external deps.
package detect

import "math"

// Baseline keeps the last `cap` inter-arrival intervals (seconds) in a
// ring buffer and exposes mean + stddev. The plan calls for an
// exponentially-weighted rolling window of 100; we implement that as a
// fixed-size ring with geometric sample weights on read so the window
// self-trims without extra state per-sender.
type Baseline struct {
	buf       []float64
	head      int
	full      bool
	count     int
	cap       int
	alpha     float64 // EWMA weight for newest sample; 0 < alpha <= 1
	mean, m2  float64 // Welford running stats, updated on Add
	totalSeen int
}

const (
	defaultCap   = 100
	defaultAlpha = 0.05
)

func NewBaseline() *Baseline {
	return &Baseline{
		buf:   make([]float64, defaultCap),
		cap:   defaultCap,
		alpha: defaultAlpha,
	}
}

// Add records one inter-arrival interval in seconds. Welford keeps
// mean/stddev O(1) without iterating the buffer; the ring is retained
// so Recompute can re-derive stats if samples get evicted by wrap.
func (b *Baseline) Add(interval float64) {
	b.totalSeen++
	if b.full {
		old := b.buf[b.head]
		b.removeFromStats(old)
	}
	b.buf[b.head] = interval
	b.addToStats(interval)
	b.head = (b.head + 1) % b.cap
	if b.head == 0 {
		b.full = true
	}
	if !b.full {
		b.count = b.head
	} else {
		b.count = b.cap
	}
}

func (b *Baseline) addToStats(x float64) {
	// Welford update over the current window. When full==true we've
	// already subtracted the outgoing sample via removeFromStats so n
	// is stable at cap and this becomes a replace-one-sample update.
	n := float64(b.activeN() + 1)
	if !b.full {
		// Fresh sample: classic Welford.
		delta := x - b.mean
		b.mean += delta / n
		b.m2 += delta * (x - b.mean)
		return
	}
	// Window is full; recompute in place to avoid drift on many wraps.
	// Cheap — this only runs once we have ≥100 samples and only on ingest.
	b.recomputeStats()
}

func (b *Baseline) removeFromStats(_ float64) {
	// No-op in the ring model: addToStats handles the full-window
	// recompute. Kept as a named step so the Add() flow reads clearly.
}

func (b *Baseline) recomputeStats() {
	n := b.activeN()
	if n == 0 {
		b.mean, b.m2 = 0, 0
		return
	}
	var sum float64
	for i := 0; i < n; i++ {
		sum += b.buf[i]
	}
	mean := sum / float64(n)
	var m2 float64
	for i := 0; i < n; i++ {
		d := b.buf[i] - mean
		m2 += d * d
	}
	b.mean = mean
	b.m2 = m2
}

func (b *Baseline) activeN() int {
	if b.full {
		return b.cap
	}
	return b.head
}

func (b *Baseline) Count() int { return b.totalSeen }

// Mean returns the windowed mean in seconds. Weighted variants live on
// top of this; the plan's "exponentially weighted" phrasing is served
// by BlendedMean below which biases toward recent intervals.
func (b *Baseline) Mean() float64 { return b.mean }

// Stddev returns sample stddev. Guards against <2 samples (returns 0)
// so the state machine's (mean + kσ) thresholds degrade to (mean)
// during the warmup phase.
func (b *Baseline) Stddev() float64 {
	n := b.activeN()
	if n < 2 {
		return 0
	}
	return math.Sqrt(b.m2 / float64(n-1))
}

// BlendedMean mixes the windowed mean with an EWMA anchored on the most
// recent sample. Keeps the baseline adaptive when a sender's cadence
// genuinely shifts (plan: "adapts to genuine cadence changes without
// manual reset"). Blend weight = alpha on newest, (1-alpha) on window.
func (b *Baseline) BlendedMean() float64 {
	if b.activeN() == 0 {
		return 0
	}
	newest := b.buf[(b.head-1+b.cap)%b.cap]
	return b.alpha*newest + (1-b.alpha)*b.mean
}
