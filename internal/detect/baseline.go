// Package detect holds the rolling-interval baseline and per-sender
// state machine. All math is plain float64 with no external deps.
package detect

import "math"

// Baseline keeps the last `cap` inter-arrival intervals (seconds) in a
// ring buffer and exposes mean + stddev. The fixed-size ring self-trims
// on wrap so per-sender state stays bounded.
type Baseline struct {
	buf       []float64
	head      int
	full      bool
	count     int
	cap       int
	alpha     float64 // EWMA weight for newest sample, in (0, 1]
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
// mean/stddev O(1) until the ring wraps. Once full we recompute in
// place to avoid drift across many evictions.
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
	if !b.full {
		n := float64(b.activeN() + 1)
		delta := x - b.mean
		b.mean += delta / n
		b.m2 += delta * (x - b.mean)
		return
	}
	b.recomputeStats()
}

// removeFromStats is a no-op. Once the ring is full, addToStats triggers
// a full recompute. Kept as a named step so Add() reads clearly.
func (b *Baseline) removeFromStats(_ float64) {}

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

// Mean is the windowed mean in seconds. Use BlendedMean for a
// recency-biased view.
func (b *Baseline) Mean() float64 { return b.mean }

// Stddev returns the sample stddev, or 0 with fewer than 2 samples so
// (mean + kσ) thresholds degrade gracefully during warmup.
func (b *Baseline) Stddev() float64 {
	n := b.activeN()
	if n < 2 {
		return 0
	}
	return math.Sqrt(b.m2 / float64(n-1))
}

// BlendedMean mixes the windowed mean with the newest sample, weighted
// alpha on newest and (1-alpha) on the window. Keeps the baseline
// adaptive when a sender's real cadence shifts.
func (b *Baseline) BlendedMean() float64 {
	if b.activeN() == 0 {
		return 0
	}
	newest := b.buf[(b.head-1+b.cap)%b.cap]
	return b.alpha*newest + (1-b.alpha)*b.mean
}
