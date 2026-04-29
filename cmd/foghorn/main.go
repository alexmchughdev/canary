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
	"github.com/alexmchughdev/foghorn/internal/slackx"
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

	sc, err := slackx.New(slackx.Options{
		AppToken: appToken,
		BotToken: botToken,
		Monitor:  cfg.MonitoredChannels(),
		AlertTo:  cfg.Channels.AlertTo,
		Logger:   log,
	})
	if err != nil {
		return err
	}

	m := metrics.New()
	a := app.New(cfg, st, sc, m, log)

	log.Info("foghorn starting",
		"channels", len(cfg.Channels.Monitor),
		"alert_to", cfg.Channels.AlertTo,
		"metrics", cfg.Metrics.Addr)

	return a.Run(ctx)
}
