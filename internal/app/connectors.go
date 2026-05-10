package app

import (
	"fmt"
	"log/slog"

	"github.com/alexmchughdev/foghorn/internal/config"
	"github.com/alexmchughdev/foghorn/internal/connector"
	slackconn "github.com/alexmchughdev/foghorn/internal/connector/slack"
)

// BuildConnectors instantiates one connector per entry in
// cfg.Connectors. Token env vars are resolved here; a missing token
// fails fast rather than at first handshake. Returns the constructed
// connectors in cfg.Connectors order. The dispatch from Type to a
// concrete constructor is the one place a new platform plugs in.
func BuildConnectors(cfg *config.Config, log *slog.Logger) ([]connector.Connector, error) {
	out := make([]connector.Connector, 0, len(cfg.Connectors))
	for _, cc := range cfg.Connectors {
		c, err := buildOneConnector(cc, log)
		if err != nil {
			return nil, fmt.Errorf("connector %q: %w", cc.Name, err)
		}
		out = append(out, c)
	}
	return out, nil
}

func buildOneConnector(cc config.ConnectorConfig, log *slog.Logger) (connector.Connector, error) {
	switch cc.Type {
	case "slack":
		appToken, botToken, err := cc.Tokens()
		if err != nil {
			return nil, err
		}
		return slackconn.New(slackconn.Options{
			Name:     cc.Name,
			AppToken: appToken,
			BotToken: botToken,
			Monitor:  cc.MonitorSet(),
			Logger:   log,
		})
	default:
		return nil, fmt.Errorf("unknown type %q", cc.Type)
	}
}
