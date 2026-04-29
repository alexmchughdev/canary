// Package connector defines the contract every chat-platform integration
// implements. Platform-specific code lives under subpackages and the worker
// only ever sees Connector.
package connector

import (
	"context"
	"time"
)

// Connector is one configured connection to one chat platform.
type Connector interface {
	// Name is a human-readable identifier from config (e.g. "prod-slack").
	// Used in metrics labels and log fields. Must be unique within a Foghorn
	// instance.
	Name() string

	// Platform is the platform family ("slack"). Stable across instances,
	// used for routing.
	Platform() string

	// History returns messages posted in monitored channels since `since`,
	// in chronological order (oldest first). Used at boot to seed baselines
	// and clusters before live monitoring begins.
	History(ctx context.Context, since time.Time) ([]Message, error)

	// Stream blocks until ctx is cancelled, feeding live messages from
	// monitored channels into out. Implementations are responsible for
	// their own reconnection logic.
	Stream(ctx context.Context, out chan<- Message) error

	// Post sends `text` to the given channel on this platform. Plain text
	// only; formatting is the caller's concern. The Alerter abstraction
	// (Phase 5) wraps this for typed alert delivery.
	Post(ctx context.Context, channel, text string) error

	// Close releases platform-specific resources. Safe to call multiple
	// times.
	Close() error
}

// Message is the platform-neutral view of one chat message ingested by
// Foghorn. Text may be empty during Phase 1 — the Slack connector will not
// populate it until Phase 4 widens ingest to support clustering.
type Message struct {
	Platform  string    // "slack"
	Connector string    // logical connector name from config
	SenderID  string    // platform-specific user/bot id
	ChannelID string    // platform-specific channel id
	Timestamp time.Time // UTC
	Text      string    // empty until Phase 4
	SubType   string    // platform-specific message subtype, if any
}
