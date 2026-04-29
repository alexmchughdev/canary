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
}

func TestValidate_errors(t *testing.T) {
	cases := map[string]string{
		"no monitor": `channels: {alert_to: X}`,
		"no alert":   `channels: {monitor: [C1]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTmp(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
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
