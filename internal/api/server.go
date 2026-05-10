// Package api serves the read-only HTTP surface (clusters, senders,
// alerts) plus a single mutating endpoint (/relearn). Runs on its own
// port, separate from metrics. Bearer-token auth on every endpoint
// except /healthz and /version.
package api

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/alexmchughdev/foghorn/internal/store"
)

// RelearnFunc rebuilds clusters for one channel from history. Injected
// by the worker at startup to avoid a circular import between api and
// app.
type RelearnFunc func(ctx context.Context, channelID string) error

type Server struct {
	store   store.Store
	relearn RelearnFunc
	token   string
	addr    string
	sha     string
	started time.Time
}

func New(st store.Store, relearn RelearnFunc, addr, token, sha string) *Server {
	return &Server{
		store:   st,
		relearn: relearn,
		addr:    addr,
		token:   token,
		sha:     sha,
		started: time.Now().UTC(),
	}
}

// Addr returns the configured listen address. Useful in tests and for
// log messages at boot.
func (s *Server) Addr() string { return s.addr }

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/version", s.handleVersion)
	mux.Handle("/clusters", s.authed(s.handleClusters))
	mux.Handle("/senders", s.authed(s.handleSenders))
	mux.Handle("/alerts", s.authed(s.handleAlerts))
	mux.Handle("/relearn", s.authed(s.handleRelearn))
	return mux
}

// Serve runs the API until ctx is cancelled or the listener fails.
// Shutdown grace is bounded so the worker process can exit promptly.
func (s *Server) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.routes(),
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

// BuildSHA returns the VCS revision baked into the binary by the Go
// build system, or "unknown" if not available (e.g. `go run`).
func BuildSHA() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return "unknown"
}
