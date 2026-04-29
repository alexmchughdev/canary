// Package connector is the contract every chat-platform integration
// implements. The worker only ever sees Connector.
package connector

import (
	"context"
	"time"
)

// Connector is one configured connection to one chat platform.
type Connector interface {
	// Name is the logical identifier from config, used in metrics labels
	// and log fields.
	Name() string

	// Platform is the platform family ("slack").
	Platform() string

	// History returns messages from monitored channels since `since`,
	// oldest first.
	History(ctx context.Context, since time.Time) ([]Message, error)

	// Stream blocks until ctx is cancelled, feeding live messages into out.
	Stream(ctx context.Context, out chan<- Message) error

	// Post sends plain text to a channel.
	Post(ctx context.Context, channel, text string) error

	// Close releases platform-specific resources.
	Close() error
}

// Message is the platform-neutral view of one ingested chat message.
type Message struct {
	Platform  string
	Connector string
	SenderID  string
	ChannelID string
	Timestamp time.Time
	Text      string
	SubType   string
}
