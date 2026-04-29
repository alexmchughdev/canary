// Package slack is the Slack implementation of connector.Connector.
// It wraps slack-go's Socket Mode client with the narrow surface Foghorn
// needs: stream channel messages as connector.Message values, post text
// to arbitrary channels, auto-reconnect under the hood.
package slack

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/alexmchughdev/foghorn/internal/connector"
)

const platform = "slack"

type Client struct {
	name    string
	api     *slack.Client
	sock    *socketmode.Client
	monitor map[string]struct{}
	log     *slog.Logger
}

type Options struct {
	Name     string              // logical connector name from config
	AppToken string              // xapp-… for Socket Mode
	BotToken string              // xoxb-… for web API
	Monitor  map[string]struct{} // channel IDs to ingest
	Logger   *slog.Logger
}

func New(opts Options) (*Client, error) {
	if opts.AppToken == "" || opts.BotToken == "" {
		return nil, fmt.Errorf("slack: both app and bot tokens required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	api := slack.New(opts.BotToken, slack.OptionAppLevelToken(opts.AppToken))
	sock := socketmode.New(api)
	return &Client{
		name:    opts.Name,
		api:     api,
		sock:    sock,
		monitor: opts.Monitor,
		log:     log,
	}, nil
}

func (c *Client) Name() string     { return c.name }
func (c *Client) Platform() string { return platform }

// History returns messages posted in monitored channels since `since`.
// TODO(phase-2): implement via conversations.history.
func (c *Client) History(ctx context.Context, since time.Time) ([]connector.Message, error) {
	return nil, nil
}

// Stream blocks until ctx is done, feeding monitored channel messages to
// out. The underlying slack-go Socket Mode client handles reconnects;
// this method just acks events and filters.
func (c *Client) Stream(ctx context.Context, out chan<- connector.Message) error {
	go c.consume(ctx, out)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.sock.RunContext(ctx)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (c *Client) consume(ctx context.Context, out chan<- connector.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-c.sock.Events:
			if !ok {
				return
			}
			c.handle(ctx, evt, out)
		}
	}
}

func (c *Client) handle(ctx context.Context, evt socketmode.Event, out chan<- connector.Message) {
	switch evt.Type {
	case socketmode.EventTypeConnecting, socketmode.EventTypeConnectionError:
		c.log.Info("slack socket connecting", "type", evt.Type)
	case socketmode.EventTypeConnected:
		c.log.Info("slack socket connected")
	case socketmode.EventTypeDisconnect:
		c.log.Warn("slack socket disconnect")
	case socketmode.EventTypeHello:
		// no-op
	case socketmode.EventTypeEventsAPI:
		outer, ok := evt.Data.(slackevents.EventsAPIEvent)
		if !ok {
			return
		}
		c.sock.Ack(*evt.Request)
		if inner, ok := outer.InnerEvent.Data.(*slackevents.MessageEvent); ok {
			c.forward(ctx, inner, out)
		}
	}
}

func (c *Client) forward(ctx context.Context, m *slackevents.MessageEvent, out chan<- connector.Message) {
	// Ignore bot messages we posted ourselves and message edits/deletions —
	// only real new heartbeats count as a tick.
	if m.SubType != "" && m.SubType != "bot_message" {
		return
	}
	if _, want := c.monitor[m.Channel]; !want {
		return
	}
	sender := m.User
	if sender == "" {
		sender = m.BotID
	}
	if sender == "" {
		return
	}
	ts, err := parseSlackTS(m.TimeStamp)
	if err != nil {
		c.log.Warn("bad slack ts", "ts", m.TimeStamp, "err", err)
		return
	}
	select {
	case <-ctx.Done():
	case out <- connector.Message{
		Platform:  platform,
		Connector: c.name,
		SenderID:  sender,
		ChannelID: m.Channel,
		Timestamp: ts,
		SubType:   m.SubType,
	}:
	}
}

// Post sends a plain-text message to the given channel. Caller formats
// the body; this package stays transport-only so callers (the alerter
// in Phase 5) can own message shape.
func (c *Client) Post(ctx context.Context, channel, text string) error {
	_, _, err := c.api.PostMessageContext(ctx, channel,
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	return err
}

// Close is a no-op; slack-go's Socket Mode client doesn't require explicit
// shutdown beyond ctx cancellation. Provided for Connector symmetry.
func (c *Client) Close() error { return nil }

// parseSlackTS converts "1713790000.000123" → time.Time. Slack uses
// seconds.microseconds as a string; we only need second precision for
// baseline math so we truncate.
func parseSlackTS(s string) (time.Time, error) {
	var secs, usecs int64
	_, err := fmt.Sscanf(s, "%d.%d", &secs, &usecs)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, usecs*1000).UTC(), nil
}
