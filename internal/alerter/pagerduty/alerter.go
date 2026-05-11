// Package pagerduty is the PagerDuty Events API v2 implementation of
// alerter.Alerter. Each Foghorn alert becomes one HTTP POST to the
// public enqueue endpoint with a stable dedup_key so raise and resolve
// events pair on the PagerDuty side.
package pagerduty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/alexmchughdev/foghorn/internal/alerter"
)

const defaultEndpoint = "https://events.pagerduty.com/v2/enqueue"

type Alerter struct {
	name       string
	routingKey string
	endpoint   string
	client     *http.Client
	severities map[alerter.Severity]struct{}
	retryDelay time.Duration
}

type Options struct {
	Name       string
	RoutingKey string
	// Endpoint overrides the default PagerDuty enqueue URL. Tests
	// point this at httptest.NewServer; production leaves it empty.
	Endpoint string
	// Client overrides the HTTP client. Defaults to one with a
	// 10-second timeout.
	Client *http.Client
	// Severities is the trigger-event allow-list. Empty defaults to
	// ["critical"]; a non-empty list replaces the default entirely.
	// Resolve events bypass this filter (see Send).
	Severities []string
}

func New(opts Options) (*Alerter, error) {
	if opts.RoutingKey == "" {
		return nil, errors.New("pagerduty alerter: routing key required")
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	sevs := make(map[alerter.Severity]struct{})
	if len(opts.Severities) == 0 {
		sevs[alerter.SeverityCritical] = struct{}{}
	} else {
		for _, s := range opts.Severities {
			sevs[alerter.Severity(strings.ToLower(strings.TrimSpace(s)))] = struct{}{}
		}
	}
	return &Alerter{
		name:       opts.Name,
		routingKey: opts.RoutingKey,
		endpoint:   endpoint,
		client:     client,
		severities: sevs,
		retryDelay: time.Second,
	}, nil
}

func (a *Alerter) Name() string { return a.name }

// Send maps one Foghorn Alert to one PagerDuty event. Recovery alerts
// (severity=info) become resolve events and always fire so the paired
// trigger's incident closes regardless of the allow-list. Trigger
// events are gated by the severities allow-list; alerts outside the
// list are dropped silently.
func (a *Alerter) Send(ctx context.Context, alert alerter.Alert) error {
	body := a.buildBody(alert)
	if body == nil {
		return nil
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("pagerduty: marshal: %w", err)
	}
	return a.postWithRetry(ctx, buf)
}

func (a *Alerter) buildBody(alert alerter.Alert) map[string]any {
	dedup := dedupKey(alert)
	if alert.Severity == alerter.SeverityInfo {
		return map[string]any{
			"routing_key":  a.routingKey,
			"event_action": "resolve",
			"dedup_key":    dedup,
		}
	}
	if _, ok := a.severities[alert.Severity]; !ok {
		return nil
	}
	payload := map[string]any{
		"summary":   summary(alert),
		"severity":  string(alert.Severity),
		"source":    "foghorn",
		"component": alert.ChannelID,
		"group":     alert.Connector,
		"class":     alert.Kind,
		"custom_details": map[string]any{
			"sender_id":  alert.SenderID,
			"channel_id": alert.ChannelID,
			"connector":  alert.Connector,
			"kind":       alert.Kind,
			"title":      alert.Title,
			"body":       alert.Body,
			"raised_at":  alert.RaisedAt.Format(time.RFC3339),
		},
	}
	return map[string]any{
		"routing_key":  a.routingKey,
		"event_action": "trigger",
		"dedup_key":    dedup,
		"payload":      payload,
	}
}

func summary(a alerter.Alert) string {
	if a.Title != "" {
		return a.Title
	}
	return fmt.Sprintf("%s alert in %s", a.Kind, a.ChannelID)
}

// dedupKey returns a stable key that pairs trigger and resolve events
// for one Foghorn-side alert identity. Sender-scoped frequency and
// content alerts carry a SenderID; cluster-scoped missing_pattern
// alerts arrive with SenderID empty, and Alert doesn't carry a cluster
// index today, so they fall back to a per-(connector, channel, kind)
// key. That matches the existing per-(channel, kind) cooldown gate in
// app.go which already collapses multiple cluster identities within
// the same kind.
func dedupKey(a alerter.Alert) string {
	scope := a.SenderID
	if scope == "" {
		scope = "scope"
	}
	return fmt.Sprintf("foghorn:%s:%s:%s:%s",
		a.Connector, a.ChannelID, a.Kind, scope)
}

func (a *Alerter) postWithRetry(ctx context.Context, body []byte) error {
	err := a.post(ctx, body)
	if err == nil || !shouldRetry(err) {
		return err
	}
	t := time.NewTimer(a.retryDelay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	return a.post(ctx, body)
}

type httpStatusError struct {
	status int
	body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("pagerduty: status %d: %s", e.status, e.body)
}

func (a *Alerter) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("pagerduty: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("pagerduty: post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	buf, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return &httpStatusError{status: resp.StatusCode, body: strings.TrimSpace(string(buf))}
}

func shouldRetry(err error) bool {
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.status >= 500
	}
	return true
}
