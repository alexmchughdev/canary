package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Slack     SlackConfig          `yaml:"slack"`
	Channels  ChannelsConfig       `yaml:"channels"`
	Detection DetectionConfig      `yaml:"detection"`
	Learning  LearningConfig       `yaml:"learning"`
	Cluster   ClusterConfig        `yaml:"cluster"`
	Alerts    AlertsConfig         `yaml:"alerts"`
	Alerters  []AlerterConfig      `yaml:"alerters"`
	Store     StoreConfig          `yaml:"store"`
	Metrics   MetricsConfig        `yaml:"metrics"`
	API       APIConfig            `yaml:"api"`
	Senders   map[string]SenderCfg `yaml:"senders"`
}

// APIConfig configures the HTTP API server. Addr is the listen address
// on a separate port from metrics. TokenEnv names the env var that
// holds the bearer token; if unset at startup the API will not start.
type APIConfig struct {
	Addr     string `yaml:"addr"`
	TokenEnv string `yaml:"token_env"`
}

// AlerterConfig describes one alert sink. Type discriminates which
// fields below apply.
type AlerterConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // "slack" or "email"

	// slack
	BotTokenEnv string   `yaml:"bot_token_env"`
	Channels    []string `yaml:"channels"`

	// email
	SMTPHost    string   `yaml:"smtp_host"`
	SMTPPort    int      `yaml:"smtp_port"`
	UserEnv     string   `yaml:"user_env"`
	PasswordEnv string   `yaml:"password_env"`
	From        string   `yaml:"from"`
	To          []string `yaml:"to"`
}

type SlackConfig struct {
	AppTokenEnv string `yaml:"app_token_env"`
	BotTokenEnv string `yaml:"bot_token_env"`
}

type ChannelsConfig struct {
	Monitor []string `yaml:"monitor"`
	AlertTo string   `yaml:"alert_to"`
}

type DetectionConfig struct {
	LearningMessages  int           `yaml:"learning_messages"`
	DriftSigma        float64       `yaml:"drift_sigma"`
	OfflineMultiplier float64       `yaml:"offline_multiplier"`
	HardCap           time.Duration `yaml:"hard_cap"`
}

type LearningConfig struct {
	Lookback time.Duration `yaml:"lookback"`
}

type ClusterConfig struct {
	Epsilon          float64 `yaml:"epsilon"`
	MinPts           int     `yaml:"min_pts"`
	MatchThreshold   float64 `yaml:"match_threshold"`
	StableRatio      float64 `yaml:"stable_ratio"`
}

// AlertsConfig holds alert-routing knobs. CooldownPerKind suppresses
// repeated alerts of the same kind on the same channel. It applies to
// content-cluster alerts (unknown_pattern, abnormal_content,
// missing_pattern). Frequency alerts use their existing per-(channel,
// sender) raise/clear dedup and are unaffected.
type AlertsConfig struct {
	CooldownPerKind time.Duration `yaml:"cooldown_per_kind"`
}

// UnmarshalYAML extends time.Duration parsing with a "d" (day) suffix.
// yaml.v3's default decoder rejects "d" since time.ParseDuration does.
func (l *LearningConfig) UnmarshalYAML(node *yaml.Node) error {
	raw := struct {
		Lookback string `yaml:"lookback"`
	}{}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	if raw.Lookback == "" {
		return nil
	}
	d, err := parseLookbackDuration(raw.Lookback)
	if err != nil {
		return fmt.Errorf("learning.lookback %q: %w", raw.Lookback, err)
	}
	l.Lookback = d
	return nil
}

func parseLookbackDuration(s string) (time.Duration, error) {
	if n := len(s); n > 1 && s[n-1] == 'd' {
		days, err := time.ParseDuration(s[:n-1] + "h")
		if err != nil {
			return 0, err
		}
		return days * 24, nil
	}
	return time.ParseDuration(s)
}

type StoreConfig struct {
	Path string `yaml:"path"`
}

type MetricsConfig struct {
	Addr string `yaml:"addr"`
}

// SenderCfg is a per-sender override. The YAML shape accepts a bare
// duration ("5m"), the literal "auto", or a mapping with {interval,
// priority}. UnmarshalYAML normalises all three.
type SenderCfg struct {
	Auto     bool
	Interval time.Duration
	Priority string
}

func (s *SenderCfg) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Value == "auto" {
			s.Auto = true
			return nil
		}
		d, err := time.ParseDuration(node.Value)
		if err != nil {
			return fmt.Errorf("sender override %q: %w", node.Value, err)
		}
		s.Interval = d
		return nil
	case yaml.MappingNode:
		raw := struct {
			Interval string `yaml:"interval"`
			Priority string `yaml:"priority"`
		}{}
		if err := node.Decode(&raw); err != nil {
			return err
		}
		if raw.Interval == "auto" || raw.Interval == "" {
			s.Auto = true
		} else {
			d, err := time.ParseDuration(raw.Interval)
			if err != nil {
				return fmt.Errorf("sender override interval %q: %w", raw.Interval, err)
			}
			s.Interval = d
		}
		s.Priority = raw.Priority
		return nil
	default:
		return fmt.Errorf("sender override: unsupported yaml kind %d", node.Kind)
	}
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Detection.LearningMessages == 0 {
		c.Detection.LearningMessages = 20
	}
	if c.Detection.DriftSigma == 0 {
		c.Detection.DriftSigma = 3
	}
	if c.Detection.OfflineMultiplier == 0 {
		c.Detection.OfflineMultiplier = 5
	}
	if c.Detection.HardCap == 0 {
		c.Detection.HardCap = 30 * time.Minute
	}
	if c.Learning.Lookback == 0 {
		c.Learning.Lookback = 7 * 24 * time.Hour
	}
	if c.Cluster.Epsilon == 0 {
		c.Cluster.Epsilon = 0.4
	}
	if c.Cluster.MinPts == 0 {
		c.Cluster.MinPts = 3
	}
	if c.Cluster.MatchThreshold == 0 {
		c.Cluster.MatchThreshold = 0.5
	}
	if c.Cluster.StableRatio == 0 {
		c.Cluster.StableRatio = 0.8
	}
	if c.Alerts.CooldownPerKind == 0 {
		c.Alerts.CooldownPerKind = 15 * time.Minute
	}
	if c.Store.Path == "" {
		c.Store.Path = "foghorn.db"
	}
	if c.Metrics.Addr == "" {
		c.Metrics.Addr = ":9090"
	}
	if c.API.Addr == "" {
		c.API.Addr = ":8080"
	}
	if c.API.TokenEnv == "" {
		c.API.TokenEnv = "FOGHORN_API_TOKEN"
	}
	if c.Slack.AppTokenEnv == "" {
		c.Slack.AppTokenEnv = "SLACK_APP_TOKEN"
	}
	if c.Slack.BotTokenEnv == "" {
		c.Slack.BotTokenEnv = "SLACK_BOT_TOKEN"
	}
}

func (c *Config) Validate() error {
	if len(c.Channels.Monitor) == 0 {
		return errors.New("channels.monitor must list at least one channel id")
	}
	if c.Channels.AlertTo == "" && len(c.Alerters) == 0 {
		return errors.New("either channels.alert_to or alerters must be configured")
	}
	if c.Detection.DriftSigma <= 0 {
		return errors.New("detection.drift_sigma must be > 0")
	}
	if c.Detection.OfflineMultiplier <= 1 {
		return errors.New("detection.offline_multiplier must be > 1")
	}
	if c.Detection.LearningMessages < 2 {
		return errors.New("detection.learning_messages must be >= 2")
	}
	for i, ac := range c.Alerters {
		if ac.Name == "" {
			return fmt.Errorf("alerters[%d]: name required", i)
		}
		switch ac.Type {
		case "slack":
			if len(ac.Channels) == 0 {
				return fmt.Errorf("alerters[%d] %q: channels required", i, ac.Name)
			}
		case "email":
			if ac.SMTPHost == "" || ac.SMTPPort == 0 {
				return fmt.Errorf("alerters[%d] %q: smtp_host and smtp_port required", i, ac.Name)
			}
			if ac.From == "" || len(ac.To) == 0 {
				return fmt.Errorf("alerters[%d] %q: from and to required", i, ac.Name)
			}
		default:
			return fmt.Errorf("alerters[%d] %q: unknown type %q", i, ac.Name, ac.Type)
		}
	}
	return nil
}

// MigrateAlertTo synthesizes a default Slack alerter from the legacy
// channels.alert_to field when no alerters: block was provided. Logs a
// deprecation warning. Idempotent and safe to call when alerters is
// already populated.
func (c *Config) MigrateAlertTo(log *slog.Logger) {
	if len(c.Alerters) > 0 || c.Channels.AlertTo == "" {
		return
	}
	if log != nil {
		log.Warn("config: channels.alert_to is deprecated; use alerters: block")
	}
	c.Alerters = []AlerterConfig{{
		Name:        "default-slack",
		Type:        "slack",
		BotTokenEnv: c.Slack.BotTokenEnv,
		Channels:    []string{c.Channels.AlertTo},
	}}
}

// Tokens resolves Slack tokens from the configured env vars. Errors if
// either is unset so startup fails fast rather than at first handshake.
func (c *Config) Tokens() (app, bot string, err error) {
	app = os.Getenv(c.Slack.AppTokenEnv)
	bot = os.Getenv(c.Slack.BotTokenEnv)
	if app == "" {
		return "", "", fmt.Errorf("env %s is empty", c.Slack.AppTokenEnv)
	}
	if bot == "" {
		return "", "", fmt.Errorf("env %s is empty", c.Slack.BotTokenEnv)
	}
	return app, bot, nil
}

// APIToken resolves the API bearer token from its configured env var.
// Errors if the var is unset so the API fails closed at startup rather
// than serving authenticated endpoints with an empty token.
func (c *Config) APIToken() (string, error) {
	tok := os.Getenv(c.API.TokenEnv)
	if tok == "" {
		return "", fmt.Errorf("env %s is empty", c.API.TokenEnv)
	}
	return tok, nil
}

// MonitoredChannels returns the configured channel IDs as a set for
// O(1) membership checks.
func (c *Config) MonitoredChannels() map[string]struct{} {
	s := make(map[string]struct{}, len(c.Channels.Monitor))
	for _, id := range c.Channels.Monitor {
		s[id] = struct{}{}
	}
	return s
}
