// Package alerter delivers detection alerts to one or more sinks. Each
// concrete sink (Slack, email, future PagerDuty or generic webhooks)
// implements Alerter; Multi composes several sinks and fans out
// concurrently.
package alerter

import (
	"context"
	"time"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Alert is the typed payload an Alerter sends. Each sink formats it
// differently: a Slack post, an email body, a PagerDuty incident.
type Alert struct {
	Severity  Severity
	Title     string
	Body      string
	SenderID  string
	ChannelID string
	Connector string
	Kind      string
	RaisedAt  time.Time
}

// Alerter is one sink. Future sinks (PagerDuty, generic webhooks)
// implement this same interface.
type Alerter interface {
	Name() string
	Send(ctx context.Context, a Alert) error
}
