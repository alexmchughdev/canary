package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/connector"
	slackconn "github.com/alexmchughdev/foghorn/internal/connector/slack"
)

// BuildConnectors instantiates one connector per entry in
// cfg.Connectors, resolves each connector's monitor list against the
// platform's live channel set, and returns the result in cfg order.
// Token env vars are resolved here; a missing token fails fast rather
// than at first handshake. The dispatch from Type to a concrete
// constructor is the one place a new platform plugs in.
func BuildConnectors(ctx context.Context, cfg *config.Config, log *slog.Logger) ([]connector.Connector, error) {
	excluded := slackAlertDestinations(cfg)
	out := make([]connector.Connector, 0, len(cfg.Connectors))
	for _, cc := range cfg.Connectors {
		c, err := buildOneConnector(ctx, cc, excluded, log)
		if err != nil {
			return nil, fmt.Errorf("connector %q: %w", cc.Name, err)
		}
		out = append(out, c)
	}
	return out, nil
}

func buildOneConnector(ctx context.Context, cc config.ConnectorConfig, excluded []slackconn.ExcludedChannel, log *slog.Logger) (connector.Connector, error) {
	switch cc.Type {
	case "slack":
		appToken, botToken, err := cc.Tokens()
		if err != nil {
			return nil, err
		}
		c, err := slackconn.New(slackconn.Options{
			Name:     cc.Name,
			AppToken: appToken,
			BotToken: botToken,
			Logger:   log,
		})
		if err != nil {
			return nil, err
		}
		if err := c.Bootstrap(ctx, cc.Monitor, excluded); err != nil {
			return nil, err
		}
		res, err := c.ValidateAccess(ctx)
		if err != nil {
			return nil, err
		}
		log.Info("slack auth ok",
			"connector", cc.Name,
			"team", res.Team,
			"user", res.User,
			"channels", len(res.Channels),
			"scopes", "ok")
		return c, nil
	default:
		return nil, fmt.Errorf("unknown type %q", cc.Type)
	}
}

// slackAlertDestinations returns every channel listed in a Slack
// alerter's channels:, paired with the alerter's name. Consumed by
// each Slack connector's Bootstrap to keep alert channels out of the
// monitor set. Multi-workspace setups are tolerated: a destination
// that doesn't exist in a given connector's workspace fails to resolve
// during Bootstrap and is silently dropped, so passing the union to
// every connector is safe.
func slackAlertDestinations(cfg *config.Config) []slackconn.ExcludedChannel {
	var out []slackconn.ExcludedChannel
	for _, a := range cfg.Alerters {
		if a.Type != "slack" {
			continue
		}
		for _, ch := range a.Channels {
			out = append(out, slackconn.ExcludedChannel{Channel: ch, Owner: a.Name})
		}
	}
	return out
}
