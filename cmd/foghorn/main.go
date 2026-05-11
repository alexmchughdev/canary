// Command foghorn is the Foghorn worker + HTTP API binary.
//
// Subcommands:
//
//	foghorn run   [-config foghorn.yaml]   (default if no subcommand)
//	foghorn check [-config foghorn.yaml]   pre-flight validation only
//
// When -config is omitted the binary boots from environment variables
// alone (SLACK_APP_TOKEN, SLACK_BOT_TOKEN, FOGHORN_API_TOKEN, and the
// optional FOGHORN_ALERT_CHANNEL); see config.FromEnv.
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
	args := os.Args[1:]
	sub := "run"
	if len(args) > 0 {
		switch args[0] {
		case "run", "check":
			sub = args[0]
			args = args[1:]
		}
	}

	fs := flag.NewFlagSet(sub, flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to YAML config file; when unset, config is built from environment variables")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var err error
	switch sub {
	case "run":
		err = run(*cfgPath, log)
	case "check":
		err = check(*cfgPath, log)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("foghorn exited with error", "subcommand", sub, "err", err)
		os.Exit(1)
	}
}

func run(cfgPath string, log *slog.Logger) error {
	cfg, err := loadConfig(cfgPath, log)
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

// check loads config and runs the Slack-auth, scope, and channel-access
// validators that boot would run, then exits. Read-only: no Store
// open, no Socket Mode connection, no goroutines started.
//
// Useful for CI, pre-deployment checks, and debugging misconfiguration.
func check(cfgPath string, log *slog.Logger) error {
	cfg, err := loadConfig(cfgPath, log)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	cfg.MigrateLegacySlack(log)
	cfg.MigrateAlertTo(log)

	if _, err := cfg.APIToken(); err != nil {
		return fmt.Errorf("api token: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conns, err := app.BuildConnectors(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("connectors: %w", err)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	if _, err := app.BuildAlerter(cfg, log); err != nil {
		return fmt.Errorf("alerter: %w", err)
	}

	totalChannels := 0
	for _, c := range conns {
		totalChannels += len(c.Monitored())
	}
	log.Info("foghorn check ok",
		"config", configSource(cfgPath),
		"connectors", len(conns),
		"alerters", len(cfg.Alerters),
		"channels", totalChannels)
	return nil
}

// loadConfig picks between the YAML file path (when -config was set)
// and the environment-only default builder. A missing -config means
// "build from env"; an explicit -config to a missing file is an error
// (treated as a typo or a misconfigured deployment, not an invitation
// to silently fall back).
func loadConfig(cfgPath string, log *slog.Logger) (*config.Config, error) {
	if cfgPath == "" {
		cfg, err := config.FromEnv()
		if err != nil {
			return nil, err
		}
		log.Info("config: loaded from environment")
		return cfg, nil
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	log.Info("config: loaded from file", "path", cfgPath)
	return cfg, nil
}

func configSource(cfgPath string) string {
	if cfgPath == "" {
		return "env"
	}
	return cfgPath
}
