// Package slackx wraps slack-go's Socket Mode client with the narrow
// surface Canary needs: stream channel messages as Event values, post
// alerts to the configured channel, auto-reconnect under the hood.
package slackx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// Event is the metadata-only view of a Slack message that Canary
// ingests. No text/blocks/attachments — the security posture in the
// plan forbids persisting content, so we deliberately don't plumb it.
type Event struct {
	SenderID  string
	ChannelID string
	Timestamp time.Time
}

type Client struct {
	api     *slack.Client
	sock    *socketmode.Client
	monitor map[string]struct{}
	alertTo string
	log     *slog.Logger
}

type Options struct {
	AppToken string              // xapp-… for Socket Mode
	BotToken string              // xoxb-… for web API
	Monitor  map[string]struct{} // channel IDs to ingest
	AlertTo  string              // channel ID to post alerts to
	Logger   *slog.Logger
}

func New(opts Options) (*Client, error) {
	if opts.AppToken == "" || opts.BotToken == "" {
		return nil, fmt.Errorf("slackx: both app and bot tokens required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	api := slack.New(opts.BotToken, slack.OptionAppLevelToken(opts.AppToken))
	sock := socketmode.New(api)
	return &Client{
		api:     api,
		sock:    sock,
		monitor: opts.Monitor,
		alertTo: opts.AlertTo,
		log:     log,
	}, nil
}

// Run blocks until ctx is done, feeding monitored channel messages to
// out. The underlying slack-go Socket Mode client handles reconnects;
// this method just acks events and filters.
func (c *Client) Run(ctx context.Context, out chan<- Event) error {
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

func (c *Client) consume(ctx context.Context, out chan<- Event) {
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

func (c *Client) handle(ctx context.Context, evt socketmode.Event, out chan<- Event) {
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

func (c *Client) forward(ctx context.Context, m *slackevents.MessageEvent, out chan<- Event) {
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
	case out <- Event{SenderID: sender, ChannelID: m.Channel, Timestamp: ts}:
	}
}

// PostAlert posts a plain-text message to the configured alert channel.
// Caller formats the body; slackx stays ignorant of alert phrasing so
// the detect package can own message shape and the Slack code stays
// transport-only.
func (c *Client) PostAlert(ctx context.Context, text string) error {
	_, _, err := c.api.PostMessageContext(ctx, c.alertTo,
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	return err
}

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
