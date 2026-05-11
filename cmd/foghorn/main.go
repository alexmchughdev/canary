package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alexmchughdev/foghorn/internal/app"
	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/metrics"
	"github.com/alexmchughdev/foghorn/internal/store"
)

func main() {
	cfgPath := flag.String("config", "foghorn.yaml", "path to YAML config file")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(*cfgPath, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("foghorn exited with error", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath string, log *slog.Logger) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	cfg.MigrateLegacySlack(log)
	cfg.MigrateAlertTo(log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	st, err := store.OpenSQLite(ctx, cfg.Store.Path)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	conns, err := app.BuildConnectors(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("connectors: %w", err)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	multi, err := app.BuildAlerter(cfg, log)
	if err != nil {
		return fmt.Errorf("alerter: %w", err)
	}

	m := metrics.New()
	a := app.New(cfg, st, conns, multi, m, log)

	log.Info("foghorn starting",
		"connectors", len(cfg.Connectors),
		"alerters", len(cfg.Alerters),
		"metrics", cfg.Metrics.Addr)

	return a.Run(ctx)
}
