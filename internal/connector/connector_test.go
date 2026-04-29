package connector_test

import (
	"github.com/alexmchughdev/foghorn/internal/connector"
	"github.com/alexmchughdev/foghorn/internal/connector/slack"
)

// Compile-time assertion that the Slack client satisfies Connector.
var _ connector.Connector = (*slack.Client)(nil)
