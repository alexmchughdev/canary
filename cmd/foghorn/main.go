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
	slackconn "github.com/alexmchughdev/foghorn/internal/connector/slack"
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
	cfg.MigrateAlertTo(log)
	appToken, botToken, err := cfg.Tokens()
	if err != nil {
		return fmt.Errorf("tokens: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	st, err := store.OpenSQLite(ctx, cfg.Store.Path)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	sc, err := slackconn.New(slackconn.Options{
		Name:     "slack",
		AppToken: appToken,
		BotToken: botToken,
		Monitor:  cfg.MonitoredChannels(),
		Logger:   log,
	})
	if err != nil {
		return err
	}
	defer func() { _ = sc.Close() }()

	multi, err := app.BuildAlerter(cfg, log)
	if err != nil {
		return fmt.Errorf("alerter: %w", err)
	}

	m := metrics.New()
	a := app.New(cfg, st, sc, multi, m, log)

	log.Info("foghorn starting",
		"channels", len(cfg.Channels.Monitor),
		"alerters", len(cfg.Alerters),
		"metrics", cfg.Metrics.Addr)

	return a.Run(ctx)
}
