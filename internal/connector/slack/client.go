// Package slack is the Slack implementation of connector.Connector,
// wrapping slack-go's Socket Mode client.
package slack

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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
	Name     string
	AppToken string
	BotToken string
	Monitor  map[string]struct{}
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

// History fans conversations.history across each monitored channel and
// returns the merged result sorted oldest-first.
func (c *Client) History(ctx context.Context, since time.Time) ([]connector.Message, error) {
	var out []connector.Message
	for channelID := range c.monitor {
		msgs, err := c.historyForChannel(ctx, channelID, since)
		if err != nil {
			return nil, fmt.Errorf("history %s: %w", channelID, err)
		}
		out = append(out, msgs...)
	}
	sortChronological(out)
	return out, nil
}

func sortChronological(msgs []connector.Message) {
	sort.Slice(msgs, func(i, j int) bool {
		return msgs[i].Timestamp.Before(msgs[j].Timestamp)
	})
}

// historyForChannel pages conversations.history until the cursor is empty.
func (c *Client) historyForChannel(ctx context.Context, channelID string, since time.Time) ([]connector.Message, error) {
	var out []connector.Message
	cursor := ""
	for {
		resp, err := c.api.GetConversationHistoryContext(ctx, &slack.GetConversationHistoryParameters{
			ChannelID: channelID,
			Oldest:    fmt.Sprintf("%d.000000", since.Unix()),
			Cursor:    cursor,
			Limit:     200,
		})
		if err != nil {
			return nil, err
		}
		for _, m := range resp.Messages {
			if m.SubType != "" && m.SubType != "bot_message" {
				continue
			}
			sender := m.User
			if sender == "" {
				sender = m.BotID
			}
			if sender == "" {
				continue
			}
			ts, err := parseSlackTS(m.Timestamp)
			if err != nil {
				continue
			}
			out = append(out, connector.Message{
				Platform:  platform,
				Connector: c.name,
				SenderID:  sender,
				ChannelID: channelID,
				Timestamp: ts,
				SubType:   m.SubType,
			})
		}
		if !resp.HasMore {
			break
		}
		cursor = resp.ResponseMetaData.NextCursor
	}
	return out, nil
}

// Stream blocks until ctx is done, feeding monitored channel messages to out.
// slack-go's Socket Mode client handles reconnects internally.
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

// forward filters edits/deletions and our own bot posts, then emits a
// connector.Message. Only real new posts count as heartbeats.
func (c *Client) forward(ctx context.Context, m *slackevents.MessageEvent, out chan<- connector.Message) {
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

func (c *Client) Post(ctx context.Context, channel, text string) error {
	_, _, err := c.api.PostMessageContext(ctx, channel,
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	return err
}

// Close is a no-op. ctx cancellation is enough to shut down Socket Mode.
func (c *Client) Close() error { return nil }

// parseSlackTS converts a Slack timestamp ("1713790000.000123") to
// time.Time, keeping microsecond precision.
func parseSlackTS(s string) (time.Time, error) {
	var secs, usecs int64
	_, err := fmt.Sscanf(s, "%d.%d", &secs, &usecs)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, usecs*1000).UTC(), nil
}
