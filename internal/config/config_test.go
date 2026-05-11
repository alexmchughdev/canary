package config

import (
	"os"
	"path/filepath"
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
