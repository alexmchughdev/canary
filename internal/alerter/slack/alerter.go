// Package slack is the Slack-channel implementation of alerter.Alerter.
// It builds its own slack-go client rather than reusing the ingest
// connector so alert delivery and ingest are independent concerns.
package slack

import (
	"context"
	"errors"

	slackgo "github.com/slack-go/slack"

	"github.com/alexmchughdev/foghorn/internal/alerter"
)

type Alerter struct {
	name     string
	client   *slackgo.Client
	channels []string
}

type Options struct {
	Name     string
	Token    string
	Channels []string
}

func New(opts Options) (*Alerter, error) {
	if opts.Token == "" {
		return nil, errors.New("slack alerter: bot token required")
	}
	if len(opts.Channels) == 0 {
		return nil, errors.New("slack alerter: at least one channel required")
	}
	return &Alerter{
		name:     opts.Name,
		client:   slackgo.New(opts.Token),
		channels: opts.Channels,
	}, nil
}

func (a *Alerter) Name() string { return a.name }

func (a *Alerter) Send(ctx context.Context, alert alerter.Alert) error {
	text := alerter.FormatSlack(alert)
	var firstErr error
	for _, ch := range a.channels {
		_, _, err := a.client.PostMessageContext(ctx, ch,
			slackgo.MsgOptionText(text, false),
			slackgo.MsgOptionDisableLinkUnfurl(),
		)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
