// Package store persists sender baselines and in-flight alerts so a
// restart doesn't lose learned cadence or double-fire alerts on recovery.
package store

import (
	"context"
	"time"
)

type SenderState string

const (
	StateLearning   SenderState = "learning"
	StateHealthy    SenderState = "healthy"
	StateDrifting   SenderState = "drifting"
	StateOffline    SenderState = "offline"
	StateRecovering SenderState = "recovering"
)

// Sender is the persisted per-(sender, channel) record. Baseline is
// stored as mean+stddev so detection math stays O(1) per ingest. The
// full interval window lives only in memory.
type Sender struct {
	SenderID       string
	ChannelID      string
	FirstSeen      time.Time
	LastSeen       time.Time
	IntervalMean   float64 // seconds
	IntervalStddev float64 // seconds
	MsgCount       int
	State          SenderState
	StateEnteredAt time.Time
	BaselineReady  bool
	MutedUntil     *time.Time
}

// Alert is a single state-transition record. ClearedAt is NULL while
// the alert is in-flight, which boot-time resume uses to avoid
// re-alerting known-offline senders.
type Alert struct {
	ID                  int64
	SenderID            string
	ChannelID           string
	State               SenderState
	RaisedAt            time.Time
	ClearedAt           *time.Time
	LastIntervalSeconds float64
}

type Store interface {
	UpsertSender(ctx context.Context, s *Sender) error
	GetSender(ctx context.Context, senderID, channelID string) (*Sender, error)
	ListSenders(ctx context.Context) ([]*Sender, error)

	RaiseAlert(ctx context.Context, a *Alert) (int64, error)
	ClearOpenAlerts(ctx context.Context, senderID, channelID string, at time.Time) error
	HasOpenAlert(ctx context.Context, senderID, channelID string) (bool, error)

	Close() error
}
