// Package store persists sender baselines, in-flight alerts, and learned
// content clusters so a restart doesn't lose state or double-fire alerts.
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

// Alert kinds. The frequency kind is the existing per-sender state-
// transition alert. The other three are content-cluster alerts keyed
// on (channel, cluster).
const (
	AlertKindFrequency       = "frequency"
	AlertKindUnknownPattern  = "unknown_pattern"
	AlertKindAbnormalContent = "abnormal_content"
	AlertKindMissingPattern  = "missing_pattern"
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

// Alert records one raised alert. Exactly one of SenderID or ClusterID
// is set: SenderID for frequency alerts, ClusterID for content-cluster
// alerts. The DB enforces this with a CHECK constraint.
type Alert struct {
	ID                  int64
	Kind                string // AlertKind* values
	SenderID            string // empty when ClusterID is set
	ClusterID           *int64 // nil when SenderID is set
	ChannelID           string
	State               SenderState
	RaisedAt            time.Time
	ClearedAt           *time.Time
	LastIntervalSeconds float64
}

// Cluster is the persisted summary of one learned content cluster on a
// channel. Centroid and StableTokens are stored as JSON blobs.
type Cluster struct {
	ID             int64
	ChannelID      string
	ClusterIndex   int // DBSCAN id local to the channel
	Size           int
	SampleMessage  string
	Centroid       map[string]float64
	StableTokens   []string
	LastMessageAt  *time.Time
	IntervalMean   float64
	IntervalStddev float64
}

type Store interface {
	UpsertSender(ctx context.Context, s *Sender) error
	GetSender(ctx context.Context, senderID, channelID string) (*Sender, error)
	ListSenders(ctx context.Context) ([]*Sender, error)

	RaiseAlert(ctx context.Context, a *Alert) (int64, error)
	ClearOpenAlerts(ctx context.Context, senderID, channelID string, at time.Time) error
	HasOpenAlert(ctx context.Context, senderID, channelID string) (bool, error)

	RaiseClusterAlert(ctx context.Context, a *Alert) (int64, error)
	ClearOpenClusterAlerts(ctx context.Context, channelID string, clusterID int64, at time.Time) error
	ClearOpenClusterAlertsByChannel(ctx context.Context, channelID string, at time.Time) error
	HasOpenClusterAlert(ctx context.Context, channelID string, clusterID int64) (bool, error)
	ListAlerts(ctx context.Context, openOnly bool) ([]*Alert, error)

	UpsertCluster(ctx context.Context, c *Cluster) error
	GetClusterByIndex(ctx context.Context, channelID string, clusterIndex int) (*Cluster, error)
	ListClusters(ctx context.Context, channelID string) ([]*Cluster, error)
	ListAllClusters(ctx context.Context) ([]*Cluster, error)
	UpdateClusterStats(ctx context.Context, id int64, lastMessageAt time.Time, mean, stddev float64) error
	DeleteClustersByChannel(ctx context.Context, channelID string) error

	Close() error
}
