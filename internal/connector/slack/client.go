// Package slack is the Slack implementation of connector.Connector,
// wrapping slack-go's Socket Mode client.
package slack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/alexmchughdev/foghorn/internal/connector"
)

// RequiredScopes lists the OAuth scopes Foghorn needs at runtime. Keep
// in lockstep with slack/manifest.yaml so the manifest, validator, and
// docs can't drift apart.
var RequiredScopes = []string{
	"channels:history",
	"channels:read",
	"chat:write",
	"groups:history",
	"groups:read",
}

const platform = "slack"

type Client struct {
	name     string
	api      *slack.Client
	sock     *socketmode.Client
	botToken string
	monitor  map[string]struct{}
	// botUserID is the authenticated bot's user_id (U...). botID is the
	// bot's bot_id (B...). Both are captured by ValidateAccess from
	// auth.test and used by forward() to drop the bot's own posts
	// before they reach the worker, preventing alert-channel recursion.
	//
	// SECURITY: these contain the identity of the authenticated bot.
	// Not a credential, but identifying — don't log raw. Log the
	// team/user names returned via ValidationResult instead.
	botUserID string
	botID     string
	log       *slog.Logger
}

// ExcludedChannel is one channel a connector must not monitor, along
// with the alerter that owns it (used for actionable error messages).
type ExcludedChannel struct {
	// Channel is the channel ID or name as written in alerter config.
	Channel string
	// Owner is the alerter's configured name.
	Owner string
}

type Options struct {
	Name     string
	AppToken string
	BotToken string
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
		name:     opts.Name,
		api:      api,
		sock:     sock,
		botToken: opts.BotToken,
		monitor:  map[string]struct{}{},
		log:      log,
	}, nil
}

func (c *Client) Name() string     { return c.name }
func (c *Client) Platform() string { return platform }

// Monitored returns the resolved channel IDs this client streams from,
// populated by Bootstrap. Sorted for stable iteration in tests and logs.
func (c *Client) Monitored() []string {
	out := make([]string, 0, len(c.monitor))
	for id := range c.monitor {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// channelMeta is the slim view of a Slack channel we keep around for
// name-to-ID resolution and membership checks.
type channelMeta struct {
	ID   string
	Name string
}

// Bootstrap resolves the desired monitor list against the bot's
// accessible channels via conversations.list. Each entry may be a
// channel ID (passes through), a name with leading '#', or a bare name.
// An empty desired list means "monitor every channel the bot is in".
//
// excluded is the union of channels configured as alert destinations
// across all Slack alerters; entries here are subtracted from the
// auto-monitor set, and explicit-monitor entries that overlap return
// a startup error naming the channel and the alerter responsible.
// This prevents the worker from ingesting its own alerter posts.
//
// All unresolved-name and alert-destination errors are reported
// together so the user can fix everything in one pass instead of
// reboot-by-reboot.
func (c *Client) Bootstrap(ctx context.Context, desired []string, excluded []ExcludedChannel) error {
	botChans, err := c.listBotChannels(ctx)
	if err != nil {
		return fmt.Errorf("list bot channels: %w", err)
	}

	excludedByID := resolveExclusions(excluded, botChans)
	nameByID := make(map[string]string, len(botChans))
	for _, ch := range botChans {
		nameByID[ch.ID] = ch.Name
	}

	if len(desired) == 0 {
		ids := make([]string, 0, len(botChans))
		for _, ch := range botChans {
			if _, isAlert := excludedByID[ch.ID]; isAlert {
				continue
			}
			ids = append(ids, ch.ID)
		}
		c.monitor = idSet(ids)
		c.log.Info("slack auto-monitor",
			"connector", c.name, "channels", len(ids), "excluded", len(excludedByID))
		return nil
	}

	resolved, missing := resolveMonitor(desired, botChans)

	var conflicts []string
	keep := make([]string, 0, len(resolved))
	for _, id := range resolved {
		if owner, isAlert := excludedByID[id]; isAlert {
			conflicts = append(conflicts,
				fmt.Sprintf("#%s (%s) is the destination for alerter %q", nameByID[id], id, owner))
			continue
		}
		keep = append(keep, id)
	}

	var problems []string
	if len(missing) > 0 {
		problems = append(problems,
			"channel(s) not found or bot not invited: "+strings.Join(missing, ", "))
	}
	if len(conflicts) > 0 {
		problems = append(problems,
			"cannot monitor alert destination(s): "+strings.Join(conflicts, "; "))
	}
	if len(problems) > 0 {
		return fmt.Errorf("connector %q monitor: %s", c.name, strings.Join(problems, "; "))
	}

	c.monitor = idSet(keep)
	c.log.Info("slack monitor resolved",
		"connector", c.name, "channels", len(keep))
	return nil
}

// resolveExclusions builds a map from channel ID to owning alerter
// name, given the alerter-destination list and the bot's known
// channels. Entries that don't resolve (malformed IDs, or names not
// in the bot's channel set) are dropped silently: they can't be in
// c.monitor anyway, so there's nothing to exclude.
func resolveExclusions(excluded []ExcludedChannel, known []channelMeta) map[string]string {
	byName := make(map[string]string, len(known))
	knownIDs := make(map[string]struct{}, len(known))
	for _, ch := range known {
		byName[ch.Name] = ch.ID
		knownIDs[ch.ID] = struct{}{}
	}
	out := make(map[string]string, len(excluded))
	for _, e := range excluded {
		if looksLikeChannelID(e.Channel) {
			if _, ok := knownIDs[e.Channel]; ok {
				out[e.Channel] = e.Owner
			}
			continue
		}
		name := strings.TrimPrefix(e.Channel, "#")
		if id, ok := byName[name]; ok {
			out[id] = e.Owner
		}
	}
	return out
}

func idSet(ids []string) map[string]struct{} {
	s := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return s
}

// listBotChannels pages users.conversations and returns the channels
// the authenticated bot is a member of. Public and private channels
// both qualify; DMs and MPIMs are intentionally excluded since they
// aren't valid heartbeat channels.
//
// Membership is enforced by the API (users.conversations returns only
// channels the caller is in), so no client-side IsMember filter is
// needed. This replaced an earlier conversations.list + IsMember-
// filter approach to remove the implicit trust in Slack's is_member
// field being correctly populated on every paginated response.
func (c *Client) listBotChannels(ctx context.Context) ([]channelMeta, error) {
	var out []channelMeta
	cursor := ""
	for {
		chans, next, err := c.api.GetConversationsForUserContext(ctx, &slack.GetConversationsForUserParameters{
			Types:           []string{"public_channel", "private_channel"},
			ExcludeArchived: true,
			Limit:           200,
			Cursor:          cursor,
		})
		if err != nil {
			return nil, err
		}
		for _, ch := range chans {
			out = append(out, channelMeta{ID: ch.ID, Name: ch.Name})
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return out, nil
}

// resolveMonitor classifies each desired entry as either a Slack channel
// ID (passes through unchanged) or a channel name (stripped of an
// optional leading '#' and looked up in known). Returns the resolved IDs
// in input order plus the friendly-formatted names of any entries that
// didn't resolve — the caller renders one aggregate error.
func resolveMonitor(desired []string, known []channelMeta) (resolved []string, missing []string) {
	byName := make(map[string]string, len(known))
	for _, ch := range known {
		byName[ch.Name] = ch.ID
	}
	for _, d := range desired {
		if looksLikeChannelID(d) {
			resolved = append(resolved, d)
			continue
		}
		name := strings.TrimPrefix(d, "#")
		if id, ok := byName[name]; ok {
			resolved = append(resolved, id)
			continue
		}
		missing = append(missing, "#"+name)
	}
	return resolved, missing
}

// ValidationResult is what ValidateAccess returns on success. The
// caller logs a one-line summary based on these fields.
type ValidationResult struct {
	Team     string
	User     string
	Scopes   []string
	Channels []channelMeta
}

// ValidateAccess confirms the bot's token works, that the granted
// scopes cover RequiredScopes, and that the bot is a member of every
// channel in c.monitor. Intended to run once at boot, after Bootstrap,
// before any goroutines start.
//
// Returns actionable errors:
//
//   - missing scopes name the gap and point at slack/manifest.yaml
//   - not-a-member errors name the channel and tell the operator to
//     /invite the bot
func (c *Client) ValidateAccess(ctx context.Context) (*ValidationResult, error) {
	auth, err := c.api.AuthTestContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth.test: %w", err)
	}
	// Capture the bot's identity so forward() can drop its own posts.
	// Runs before any goroutine starts the stream, so the filter is
	// always populated by the time forward() is called.
	c.botUserID = auth.UserID
	c.botID = auth.BotID

	granted, err := c.grantedScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("granted scopes: %w", err)
	}
	if missing := scopeDiff(RequiredScopes, granted); len(missing) > 0 {
		return nil, fmt.Errorf(
			"missing Slack scope(s): %s. Update the app's OAuth scopes (see slack/manifest.yaml) and reinstall the bot",
			strings.Join(missing, ", "))
	}

	var (
		channels  []channelMeta
		notMember []string
	)
	for _, id := range c.Monitored() {
		info, err := c.api.GetConversationInfoContext(ctx, &slack.GetConversationInfoInput{ChannelID: id})
		if err != nil {
			return nil, fmt.Errorf("conversations.info %s: %w", id, err)
		}
		if !info.IsMember {
			notMember = append(notMember, fmt.Sprintf("#%s (%s)", info.Name, id))
			continue
		}
		channels = append(channels, channelMeta{ID: id, Name: info.Name})
	}
	if len(notMember) > 0 {
		return nil, fmt.Errorf(
			"bot is not a member of: %s. Invite the bot with /invite in each channel and retry",
			strings.Join(notMember, ", "))
	}

	return &ValidationResult{
		Team:     auth.Team,
		User:     auth.User,
		Scopes:   granted,
		Channels: channels,
	}, nil
}

// scopeDiff returns the elements of required that aren't in granted.
// Whitespace around granted entries is tolerated; Slack returns the
// X-OAuth-Scopes header as a comma-separated list which can include
// stray spaces.
func scopeDiff(required, granted []string) []string {
	have := make(map[string]struct{}, len(granted))
	for _, s := range granted {
		s = strings.TrimSpace(s)
		if s != "" {
			have[s] = struct{}{}
		}
	}
	var missing []string
	for _, s := range required {
		if _, ok := have[s]; !ok {
			missing = append(missing, s)
		}
	}
	return missing
}

// grantedScopes hits Slack's auth.test endpoint directly so we can read
// the X-OAuth-Scopes response header. slack-go's high-level client
// discards response headers, and Slack doesn't expose granted scopes
// in any JSON response body.
func (c *Client) grantedScopes(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/auth.test", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.botToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	hdr := resp.Header.Get("X-OAuth-Scopes")
	if hdr == "" {
		return nil, errors.New("no X-OAuth-Scopes header in response")
	}
	var out []string
	for _, s := range strings.Split(hdr, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out, nil
}

// looksLikeChannelID matches Slack's public/private channel ID shape:
// starts with C (public) or G (private), followed by uppercase
// alphanumeric, total length 9 or more. Lowercase letters can't appear
// in real IDs, so "deploys" and "Cdeploys" both fall through to name
// resolution.
func looksLikeChannelID(s string) bool {
	if len(s) < 9 {
		return false
	}
	if s[0] != 'C' && s[0] != 'G' {
		return false
	}
	for i := 1; i < len(s); i++ {
		b := s[i]
		if !(b >= 'A' && b <= 'Z') && !(b >= '0' && b <= '9') {
			return false
		}
	}
	return true
}

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
				Text:      m.Text,
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
//
// Message text is populated for content-cluster anomaly detection. The
// original Slack-only design excluded text on the assumption that
// frequency was the only signal; the brief was updated to require
// structural content analysis (TF-IDF + DBSCAN clustering, schema
// fingerprinting). Content lives in memory by default and is only
// persisted as cluster sample messages and centroid vectors, not as a
// full message archive.
func (c *Client) forward(ctx context.Context, m *slackevents.MessageEvent, out chan<- connector.Message) {
	if m.SubType != "" && m.SubType != "bot_message" {
		return
	}
	// Drop the bot's own posts. The alert-channel exclusion in
	// Bootstrap closes the dominant feedback loop; this filter closes
	// every other path (misconfigured manifests, future features
	// posting to monitored channels, third-party tools sharing the bot
	// token). Either ID matching is enough — m.User is set on normal
	// bot_message events; m.BotID covers webhook-style posts where
	// m.User may be empty.
	if m.User != "" && m.User == c.botUserID {
		return
	}
	if m.BotID != "" && m.BotID == c.botID {
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
		Text:      m.Text,
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
