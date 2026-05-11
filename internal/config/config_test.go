package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const fullYAML = `
slack:
  app_token_env: X_APP
  bot_token_env: X_BOT
channels:
  monitor: [C1, C2]
  alert_to: CALERT
detection:
  learning_messages: 10
  drift_sigma: 2.5
  offline_multiplier: 4
  hard_cap: 15m
store:
  path: /tmp/x.db
metrics:
  addr: :1234
senders:
  U1: 5m
  U2: auto
  U3:
    interval: 1h
    priority: high
  U4:
    interval: auto
`

func writeTmp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "foghorn.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad_full(t *testing.T) {
	c, err := Load(writeTmp(t, fullYAML))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Detection.HardCap != 15*time.Minute {
		t.Errorf("hard cap: %v", c.Detection.HardCap)
	}
	if c.Detection.LearningMessages != 10 {
		t.Errorf("learning: %d", c.Detection.LearningMessages)
	}
	if got := c.Senders["U1"].Interval; got != 5*time.Minute {
		t.Errorf("U1 interval: %v", got)
	}
	if !c.Senders["U2"].Auto {
		t.Errorf("U2 should be auto")
	}
	if c.Senders["U3"].Interval != time.Hour || c.Senders["U3"].Priority != "high" {
		t.Errorf("U3: %+v", c.Senders["U3"])
	}
	if !c.Senders["U4"].Auto {
		t.Errorf("U4 should be auto via mapping")
	}
}

func TestLoad_defaults(t *testing.T) {
	min := `
channels:
  monitor: [C1]
  alert_to: CALERT
`
	c, err := Load(writeTmp(t, min))
	if err != nil {
		t.Fatal(err)
	}
	if c.Detection.LearningMessages != 20 {
		t.Errorf("default learning: %d", c.Detection.LearningMessages)
	}
	if c.Detection.HardCap != 30*time.Minute {
		t.Errorf("default hard cap: %v", c.Detection.HardCap)
	}
	if c.Metrics.Addr != ":9090" {
		t.Errorf("default metrics addr: %q", c.Metrics.Addr)
	}
	if c.Learning.Lookback != 7*24*time.Hour {
		t.Errorf("default lookback: %v", c.Learning.Lookback)
	}
	if c.Cluster.Epsilon != 0.4 || c.Cluster.MinPts != 3 {
		t.Errorf("default cluster: %+v", c.Cluster)
	}
	if c.Cluster.MatchThreshold != 0.5 || c.Cluster.StableRatio != 0.8 {
		t.Errorf("default cluster thresholds: %+v", c.Cluster)
	}
	if c.Alerts.CooldownPerKind != 15*time.Minute {
		t.Errorf("default cooldown: %v", c.Alerts.CooldownPerKind)
	}
}

func TestLoad_learningLookback(t *testing.T) {
	cases := map[string]time.Duration{
		"7d":   7 * 24 * time.Hour,
		"3d":   3 * 24 * time.Hour,
		"12h":  12 * time.Hour,
		"30m":  30 * time.Minute,
		"168h": 168 * time.Hour,
	}
	for in, want := range cases {
		body := "channels: {monitor: [C1], alert_to: CALERT}\nlearning: {lookback: " + in + "}\n"
		c, err := Load(writeTmp(t, body))
		if err != nil {
			t.Fatalf("load %q: %v", in, err)
		}
		if c.Learning.Lookback != want {
			t.Errorf("lookback %q = %v, want %v", in, c.Learning.Lookback, want)
		}
	}
}

func TestValidate_errors(t *testing.T) {
	cases := map[string]string{
		"no monitor":    `channels: {alert_to: X}`,
		"no alert sink": `channels: {monitor: [C1]}`,
		"alerter missing name": `
channels: {monitor: [C1]}
alerters:
  - type: slack
    channels: [C1]
`,
		"alerter unknown type": `
channels: {monitor: [C1]}
alerters:
  - name: x
    type: pagerduty
`,
		"slack alerter missing channels": `
channels: {monitor: [C1]}
alerters:
  - name: ops
    type: slack
`,
		"email alerter missing host": `
channels: {monitor: [C1]}
alerters:
  - name: ops
    type: email
    from: a@b
    to: [c@d]
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTmp(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestLoad_alertersBlock(t *testing.T) {
	body := `
channels: {monitor: [C1]}
alerters:
  - name: ops-slack
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [C1]
  - name: ops-email
    type: email
    smtp_host: smtp.example
    smtp_port: 587
    from: foghorn@example.com
    to: [ops@example.com, oncall@example.com]
`
	c, err := Load(writeTmp(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Alerters) != 2 {
		t.Fatalf("alerters len=%d", len(c.Alerters))
	}
	if c.Alerters[0].Type != "slack" || c.Alerters[1].Type != "email" {
		t.Errorf("types: %+v", c.Alerters)
	}
	if c.Alerters[1].SMTPPort != 587 {
		t.Errorf("smtp port: %d", c.Alerters[1].SMTPPort)
	}
}

func TestMigrateAlertTo_synthesizesDefaultSlackAlerter(t *testing.T) {
	body := `
channels: {monitor: [C1], alert_to: CALERT}
slack: {bot_token_env: X_BOT}
`
	c, err := Load(writeTmp(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Alerters) != 0 {
		t.Fatalf("alerters should be empty before migrate, got %+v", c.Alerters)
	}
	c.MigrateAlertTo(nil)
	if len(c.Alerters) != 1 {
		t.Fatalf("after migrate, alerters len=%d", len(c.Alerters))
	}
	a := c.Alerters[0]
	if a.Type != "slack" || a.Channels[0] != "CALERT" || a.BotTokenEnv != "X_BOT" {
		t.Errorf("synthesized alerter: %+v", a)
	}
}

func TestMigrateAlertTo_noopWhenAlertersConfigured(t *testing.T) {
	c := &Config{
		Channels: ChannelsConfig{AlertTo: "CALERT"},
		Alerters: []AlerterConfig{{Name: "x", Type: "slack", Channels: []string{"C1"}}},
	}
	c.MigrateAlertTo(nil)
	if len(c.Alerters) != 1 {
		t.Errorf("should not duplicate, got %d", len(c.Alerters))
	}
}

func TestTokens(t *testing.T) {
	t.Setenv("X_APP", "xapp-1")
	t.Setenv("X_BOT", "xoxb-1")
	c, err := Load(writeTmp(t, fullYAML))
	if err != nil {
		t.Fatal(err)
	}
	app, bot, err := c.Tokens()
	if err != nil {
		t.Fatal(err)
	}
	if app != "xapp-1" || bot != "xoxb-1" {
		t.Errorf("tokens: %q %q", app, bot)
	}

	t.Setenv("X_APP", "")
	if _, _, err := c.Tokens(); err == nil {
		t.Fatal("expected error on empty token")
	}
}

func TestLoad_connectorsBlock(t *testing.T) {
	body := `
connectors:
  - name: prod-slack
    type: slack
    app_token_env: SLACK_APP_TOKEN
    bot_token_env: SLACK_BOT_TOKEN
    monitor: [C1, C2]
alerters:
  - name: ops-slack
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [CALERT]
`
	c, err := Load(writeTmp(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Connectors) != 1 {
		t.Fatalf("connectors len=%d", len(c.Connectors))
	}
	cc := c.Connectors[0]
	if cc.Name != "prod-slack" || cc.Type != "slack" {
		t.Errorf("connector: %+v", cc)
	}
	if len(cc.Monitor) != 2 || cc.Monitor[0] != "C1" {
		t.Errorf("monitor: %+v", cc.Monitor)
	}
	got := c.MonitoredChannels()
	for _, id := range []string{"C1", "C2"} {
		if _, ok := got[id]; !ok {
			t.Errorf("MonitoredChannels missing %q", id)
		}
	}
}

func TestValidate_connectorErrors(t *testing.T) {
	cases := map[string]string{
		"missing name": `
connectors:
  - type: slack
    monitor: [C1]
alerters:
  - name: x
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [C1]
`,
		"duplicate name": `
connectors:
  - name: a
    type: slack
    monitor: [C1]
  - name: a
    type: slack
    monitor: [C2]
alerters:
  - name: x
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [C1]
`,
		"unknown type": `
connectors:
  - name: a
    type: discord
    monitor: [C1]
alerters:
  - name: x
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [C1]
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTmp(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestLoad_emptyMonitorAllowed pins the auto-discover contract: a
// connector with no monitor list parses cleanly and is left for the
// connector itself to fill in at boot.
func TestLoad_emptyMonitorAllowed(t *testing.T) {
	body := `
connectors:
  - name: a
    type: slack
alerters:
  - name: x
    type: slack
    bot_token_env: SLACK_BOT_TOKEN
    channels: [C1]
`
	c, err := Load(writeTmp(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Connectors) != 1 {
		t.Fatalf("connectors len=%d", len(c.Connectors))
	}
	if len(c.Connectors[0].Monitor) != 0 {
		t.Errorf("monitor should be empty, got %+v", c.Connectors[0].Monitor)
	}
}

func TestMigrateLegacySlack_synthesises(t *testing.T) {
	body := `
slack:
  app_token_env: X_APP
  bot_token_env: X_BOT
channels:
  monitor: [C1, C2]
  alert_to: CALERT
`
	c, err := Load(writeTmp(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Connectors) != 0 {
		t.Fatalf("connectors should be empty pre-migrate, got %d", len(c.Connectors))
	}
	c.MigrateLegacySlack(nil)
	if len(c.Connectors) != 1 {
		t.Fatalf("after migrate, connectors len=%d", len(c.Connectors))
	}
	cc := c.Connectors[0]
	if cc.Name != "default-slack" || cc.Type != "slack" {
		t.Errorf("synthesised connector: %+v", cc)
	}
	if cc.AppTokenEnv != "X_APP" || cc.BotTokenEnv != "X_BOT" {
		t.Errorf("token envs not carried over: %+v", cc)
	}
	if len(cc.Monitor) != 2 || cc.Monitor[0] != "C1" || cc.Monitor[1] != "C2" {
		t.Errorf("monitor not carried over: %+v", cc.Monitor)
	}
}

func TestMigrateLegacySlack_noopWhenConnectorsConfigured(t *testing.T) {
	c := &Config{
		Connectors: []ConnectorConfig{{Name: "x", Type: "slack", Monitor: []string{"C1"}}},
		Channels:   ChannelsConfig{Monitor: []string{"Clegacy"}},
	}
	c.MigrateLegacySlack(nil)
	if len(c.Connectors) != 1 || c.Connectors[0].Name != "x" {
		t.Errorf("connectors mutated: %+v", c.Connectors)
	}
}

func TestMigrateLegacySlack_idempotent(t *testing.T) {
	c := &Config{
		Slack:    SlackConfig{AppTokenEnv: "X_APP", BotTokenEnv: "X_BOT"},
		Channels: ChannelsConfig{Monitor: []string{"C1"}},
	}
	c.MigrateLegacySlack(nil)
	first := c.Connectors
	c.MigrateLegacySlack(nil)
	if len(c.Connectors) != len(first) {
		t.Errorf("not idempotent: %d → %d", len(first), len(c.Connectors))
	}
}

func TestConnectorTokens(t *testing.T) {
	t.Setenv("APP_X", "xapp-1")
	t.Setenv("BOT_X", "xoxb-1")
	cc := ConnectorConfig{AppTokenEnv: "APP_X", BotTokenEnv: "BOT_X"}
	app, bot, err := cc.Tokens()
	if err != nil {
		t.Fatal(err)
	}
	if app != "xapp-1" || bot != "xoxb-1" {
		t.Errorf("tokens: %q %q", app, bot)
	}

	t.Setenv("APP_X", "")
	if _, _, err := cc.Tokens(); err == nil {
		t.Fatal("expected error on empty app token")
	}
}

func TestFromEnv_defaults(t *testing.T) {
	t.Setenv("FOGHORN_ALERT_CHANNEL", "")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if len(c.Connectors) != 1 {
		t.Fatalf("connectors len=%d", len(c.Connectors))
	}
	cc := c.Connectors[0]
	if cc.Name != "slack-main" || cc.Type != "slack" {
		t.Errorf("connector: %+v", cc)
	}
	if cc.AppTokenEnv != "SLACK_APP_TOKEN" || cc.BotTokenEnv != "SLACK_BOT_TOKEN" {
		t.Errorf("token envs: %+v", cc)
	}
	if len(cc.Monitor) != 0 {
		t.Errorf("monitor should be empty for auto-discovery, got %+v", cc.Monitor)
	}
	if len(c.Alerters) != 1 {
		t.Fatalf("alerters len=%d", len(c.Alerters))
	}
	a := c.Alerters[0]
	if a.Name != "ops-slack" || a.Type != "slack" {
		t.Errorf("alerter: %+v", a)
	}
	if a.BotTokenEnv != "SLACK_BOT_TOKEN" {
		t.Errorf("alerter bot token env: %q", a.BotTokenEnv)
	}
	if len(a.Channels) != 1 || a.Channels[0] != "#alerts" {
		t.Errorf("alerter channels: %+v", a.Channels)
	}
	if c.Metrics.Addr != ":9090" {
		t.Errorf("metrics addr: %q", c.Metrics.Addr)
	}
	if c.API.Addr != ":8080" || c.API.TokenEnv != "FOGHORN_API_TOKEN" {
		t.Errorf("api: %+v", c.API)
	}
	if c.Store.Path == "" {
		t.Error("store path empty")
	}
}

func TestFromEnv_alertChannelOverride(t *testing.T) {
	t.Setenv("FOGHORN_ALERT_CHANNEL", "#ops-foghorn")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if got := c.Alerters[0].Channels[0]; got != "#ops-foghorn" {
		t.Errorf("alert channel: got %q want %q", got, "#ops-foghorn")
	}
}

func clearEmailEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM", "SMTP_TO"} {
		t.Setenv(k, "")
	}
}

func setEmailEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_PORT", "587")
	t.Setenv("SMTP_USERNAME", "foghorn@example.com")
	t.Setenv("SMTP_PASSWORD", "secret")
	t.Setenv("SMTP_FROM", "foghorn@example.com")
	t.Setenv("SMTP_TO", "oncall@example.com,backup@example.com")
}

func TestFromEnv_emailAlerter_disabledWhenHostUnset(t *testing.T) {
	clearEmailEnv(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range c.Alerters {
		if a.Type == "email" {
			t.Errorf("email alerter should not be built without SMTP_HOST: %+v", a)
		}
	}
}

func TestFromEnv_emailAlerter_enabledWhenHostSet(t *testing.T) {
	setEmailEnv(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	var got *AlerterConfig
	for i, a := range c.Alerters {
		if a.Type == "email" {
			got = &c.Alerters[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("expected an email alerter; got %+v", c.Alerters)
	}
	if got.SMTPHost != "smtp.example.com" || got.SMTPPort != 587 {
		t.Errorf("host/port: %+v", got)
	}
	if got.UserEnv != "SMTP_USERNAME" || got.PasswordEnv != "SMTP_PASSWORD" {
		t.Errorf("env names: %+v", got)
	}
	if got.From != "foghorn@example.com" {
		t.Errorf("from: %q", got.From)
	}
	if len(got.To) != 2 || got.To[0] != "oncall@example.com" || got.To[1] != "backup@example.com" {
		t.Errorf("to: %+v", got.To)
	}
}

func TestFromEnv_emailAlerter_partialEnvErrors(t *testing.T) {
	cases := []struct {
		name  string
		unset string
	}{
		{"missing SMTP_PORT", "SMTP_PORT"},
		{"missing SMTP_USERNAME", "SMTP_USERNAME"},
		{"missing SMTP_PASSWORD", "SMTP_PASSWORD"},
		{"missing SMTP_FROM", "SMTP_FROM"},
		{"missing SMTP_TO", "SMTP_TO"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setEmailEnv(t)
			t.Setenv(tc.unset, "")
			_, err := FromEnv()
			if err == nil {
				t.Fatalf("expected error when %s is unset", tc.unset)
			}
			if !strings.Contains(err.Error(), tc.unset) {
				t.Errorf("error should mention %s: %v", tc.unset, err)
			}
		})
	}
}

func TestFromEnv_emailAlerter_invalidPort(t *testing.T) {
	setEmailEnv(t)
	t.Setenv("SMTP_PORT", "not-a-number")
	if _, err := FromEnv(); err == nil {
		t.Fatal("expected error on non-numeric SMTP_PORT")
	}
}

func TestFromEnv_pagerduty_disabledWhenKeyUnset(t *testing.T) {
	t.Setenv("PAGERDUTY_ROUTING_KEY", "")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range c.Alerters {
		if a.Type == "pagerduty" {
			t.Errorf("pagerduty alerter should not be built without PAGERDUTY_ROUTING_KEY: %+v", a)
		}
	}
}

func TestFromEnv_pagerduty_enabledWhenKeySet(t *testing.T) {
	t.Setenv("PAGERDUTY_ROUTING_KEY", "r0")
	t.Setenv("PAGERDUTY_SEVERITIES", "")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var got *AlerterConfig
	for i, a := range c.Alerters {
		if a.Type == "pagerduty" {
			got = &c.Alerters[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("expected a pagerduty alerter; got %+v", c.Alerters)
	}
	if got.RoutingKeyEnv != "PAGERDUTY_ROUTING_KEY" {
		t.Errorf("routing_key_env: %q", got.RoutingKeyEnv)
	}
	// Empty PAGERDUTY_SEVERITIES leaves Severities nil; the alerter
	// package then defaults to ["critical"].
	if len(got.Severities) != 0 {
		t.Errorf("severities should be empty for default: %+v", got.Severities)
	}
}

func TestFromEnv_pagerduty_severitiesParsed(t *testing.T) {
	t.Setenv("PAGERDUTY_ROUTING_KEY", "r0")
	t.Setenv("PAGERDUTY_SEVERITIES", "critical, warning")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var got *AlerterConfig
	for i, a := range c.Alerters {
		if a.Type == "pagerduty" {
			got = &c.Alerters[i]
			break
		}
	}
	if got == nil {
		t.Fatal("missing pagerduty alerter")
	}
	if len(got.Severities) != 2 || got.Severities[0] != "critical" || got.Severities[1] != "warning" {
		t.Errorf("severities: %+v", got.Severities)
	}
}

func TestFromEnv_allSinks(t *testing.T) {
	setEmailEnv(t)
	t.Setenv("PAGERDUTY_ROUTING_KEY", "r0")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Alerters) != 3 {
		t.Fatalf("expected 3 alerters (slack+email+pagerduty), got %d: %+v",
			len(c.Alerters), c.Alerters)
	}
	types := map[string]bool{}
	for _, a := range c.Alerters {
		types[a.Type] = true
	}
	for _, want := range []string{"slack", "email", "pagerduty"} {
		if !types[want] {
			t.Errorf("missing %s sink", want)
		}
	}
}

func TestConnectorForChannel(t *testing.T) {
	c := &Config{
		Connectors: []ConnectorConfig{
			{Name: "a", Type: "slack", Monitor: []string{"C1", "C2"}},
			{Name: "b", Type: "slack", Monitor: []string{"C3"}},
		},
	}
	if got := c.ConnectorForChannel("C2"); got != "a" {
		t.Errorf("C2: got %q want %q", got, "a")
	}
	if got := c.ConnectorForChannel("C3"); got != "b" {
		t.Errorf("C3: got %q want %q", got, "b")
	}
	if got := c.ConnectorForChannel("C-missing"); got != "" {
		t.Errorf("missing: got %q want empty", got)
	}
}
