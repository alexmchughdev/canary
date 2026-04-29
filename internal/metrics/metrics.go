// Package metrics exposes the Prometheus surface listed in
// foghorn-plan.md §Metrics exposed. Cardinality stays bounded because
// sender and channel IDs come from the finite configured set.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	SendersTotal      *prometheus.GaugeVec   // {channel, state}
	LastSeenSeconds   *prometheus.GaugeVec   // {sender, channel}
	Transitions       *prometheus.CounterVec // {sender, from_state, to_state}
	AlertsRaised      *prometheus.CounterVec // {sender, state}
	MessagesIngested  *prometheus.CounterVec // {channel}
	SlackDisconnects  prometheus.Counter
	BaselineReady     *prometheus.GaugeVec // {sender, channel}

	reg *prometheus.Registry
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		SendersTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "foghorn_senders_total",
			Help: "Number of tracked senders in each state, per channel.",
		}, []string{"channel", "state"}),
		LastSeenSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "foghorn_sender_last_seen_seconds",
			Help: "Unix timestamp of the most recent message from each sender.",
		}, []string{"sender", "channel"}),
		Transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "foghorn_state_transitions_total",
			Help: "State transitions per sender.",
		}, []string{"sender", "from_state", "to_state"}),
		AlertsRaised: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "foghorn_alerts_raised_total",
			Help: "Alerts raised per sender and state.",
		}, []string{"sender", "state"}),
		MessagesIngested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "foghorn_messages_ingested_total",
			Help: "Heartbeat messages ingested per channel.",
		}, []string{"channel"}),
		SlackDisconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "foghorn_slack_disconnects_total",
			Help: "Socket Mode disconnect events observed.",
		}),
		BaselineReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "foghorn_baseline_ready",
			Help: "1 if the baseline for a sender has crossed the learning threshold, else 0.",
		}, []string{"sender", "channel"}),
	}
	reg.MustRegister(
		m.SendersTotal, m.LastSeenSeconds, m.Transitions,
		m.AlertsRaised, m.MessagesIngested, m.SlackDisconnects, m.BaselineReady,
	)
	return m
}

// Serve runs an HTTP server exposing /metrics. Blocks until ctx done
// or listener error; caller runs it in a goroutine and shares ctx
// with the rest of the process.
func (m *Metrics) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
