package pagerduty

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/alerter"
)

func newServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(s.Close)
	return s
}

func testAlerter(t *testing.T, endpoint string, sevs []string) *Alerter {
	t.Helper()
	a, err := New(Options{
		Name:       "pd",
		RoutingKey: "r0",
		Endpoint:   endpoint,
		Severities: sevs,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Compress the retry sleep so the retry tests don't spend a real
	// second waiting between attempts.
	a.retryDelay = time.Millisecond
	return a
}

func TestNew_requiresRoutingKey(t *testing.T) {
	if _, err := New(Options{Name: "pd"}); err == nil {
		t.Fatal("expected error when routing key empty")
	}
}

func TestSend_triggerBody(t *testing.T) {
	var captured []byte
	s := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		captured = buf
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type: %q", got)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity:  alerter.SeverityCritical,
		Title:     "U1 offline in C1",
		SenderID:  "U1",
		ChannelID: "C1",
		Connector: "prod-slack",
		Kind:      "frequency",
		RaisedAt:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(captured, &body); err != nil {
		t.Fatal(err)
	}
	if body["event_action"] != "trigger" {
		t.Errorf("event_action: %v", body["event_action"])
	}
	if body["routing_key"] != "r0" {
		t.Errorf("routing_key: %v", body["routing_key"])
	}
	if got, want := body["dedup_key"], "foghorn:prod-slack:C1:frequency:U1"; got != want {
		t.Errorf("dedup_key: got %v want %s", got, want)
	}
	payload, _ := body["payload"].(map[string]any)
	if payload == nil {
		t.Fatal("payload missing on trigger")
	}
	if payload["severity"] != "critical" {
		t.Errorf("payload severity: %v", payload["severity"])
	}
	if payload["source"] != "foghorn" {
		t.Errorf("payload source: %v", payload["source"])
	}
	if payload["component"] != "C1" {
		t.Errorf("payload component: %v", payload["component"])
	}
	if payload["group"] != "prod-slack" {
		t.Errorf("payload group: %v", payload["group"])
	}
	if payload["class"] != "frequency" {
		t.Errorf("payload class: %v", payload["class"])
	}
	details, _ := payload["custom_details"].(map[string]any)
	if details == nil || details["sender_id"] != "U1" {
		t.Errorf("custom_details: %v", payload["custom_details"])
	}
}

func TestSend_resolveDropsPayloadKeepsDedup(t *testing.T) {
	var captured []byte
	s := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		captured = buf
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity:  alerter.SeverityInfo,
		Title:     "U1 recovered in C1",
		SenderID:  "U1",
		ChannelID: "C1",
		Connector: "prod-slack",
		Kind:      "frequency",
	})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(captured, &body); err != nil {
		t.Fatal(err)
	}
	if body["event_action"] != "resolve" {
		t.Errorf("event_action: %v", body["event_action"])
	}
	if _, ok := body["payload"]; ok {
		t.Errorf("resolve must omit payload, got %v", body)
	}
	if body["dedup_key"] != "foghorn:prod-slack:C1:frequency:U1" {
		t.Errorf("dedup_key: %v", body["dedup_key"])
	}
	if body["routing_key"] != "r0" {
		t.Errorf("routing_key: %v", body["routing_key"])
	}
}

func TestSend_severityFilter_defaultDropsWarning(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityWarning,
		Title:    "drifting",
		Kind:     "frequency",
	})
	if err != nil {
		t.Fatalf("filtered drop must not error: %v", err)
	}
	if got := atomic.LoadInt32(&count); got != 0 {
		t.Errorf("default allow-list should drop warning, got %d posts", got)
	}
}

func TestSend_severityFilter_widenedAcceptsWarning(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, []string{"critical", "warning"})
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityWarning,
		Title:    "drifting",
		Kind:     "frequency",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Errorf("widened allow-list should accept warning, got %d posts", got)
	}
}

func TestSend_severityFilter_doesNotBlockResolves(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, []string{"critical"})
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityInfo,
		SenderID: "U1",
		Kind:     "frequency",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Errorf("resolve must bypass allow-list, got %d posts", got)
	}
}

func TestSend_dedupKey_clusterScopedFallsBack(t *testing.T) {
	var captured []byte
	s := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		captured = buf
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity:  alerter.SeverityCritical,
		ChannelID: "C1",
		Connector: "prod-slack",
		Kind:      "missing_pattern",
	})
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	_ = json.Unmarshal(captured, &body)
	if got, want := body["dedup_key"], "foghorn:prod-slack:C1:missing_pattern:scope"; got != want {
		t.Errorf("dedup_key: got %v want %s", got, want)
	}
}

func TestSend_retryOn5xxThenSuccess(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityCritical,
		Kind:     "frequency",
	})
	if err != nil {
		t.Fatalf("expected success after retry: %v", err)
	}
	if got := atomic.LoadInt32(&count); got != 2 {
		t.Errorf("expected 2 requests (1 fail + 1 retry), got %d", got)
	}
}

func TestSend_noRetryOn4xx(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid routing key"}`))
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityCritical,
		Kind:     "frequency",
	})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention status: %v", err)
	}
	if got := atomic.LoadInt32(&count); got != 1 {
		t.Errorf("expected 1 request (no retry on 4xx), got %d", got)
	}
}

func TestSend_retryOn5xxExhausted(t *testing.T) {
	var count int32
	s := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	a := testAlerter(t, s.URL, nil)
	err := a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityCritical,
		Kind:     "frequency",
	})
	if err == nil {
		t.Fatal("expected error after retry exhausted")
	}
	if got := atomic.LoadInt32(&count); got != 2 {
		t.Errorf("expected 2 requests (1 + 1 retry), got %d", got)
	}
}
