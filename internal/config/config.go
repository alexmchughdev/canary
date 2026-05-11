package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Connectors is the new top-level connector list. Each entry is one
	// connection to one chat platform (currently only "slack" is
	// implemented). When this list is non-empty it is the source of
	// truth; the legacy Slack and Channels fields below are accepted
	// only as a deprecated shim and migrated by MigrateLegacySlack.
	Connectors []ConnectorConfig `yaml:"connectors"`

	// Slack is the deprecated top-level slack block. Use Connectors.
	Slack SlackConfig `yaml:"slack"`
	// Channels is the deprecated top-level channels block. Use the
	// per-connector Monitor list under Connectors.
	Channels ChannelsConfig `yaml:"channels"`

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

// ConnectorConfig is one entry in the connectors: array. Type
// discriminates which platform-specific fields apply. Only "slack" is
// implemented today; the schema accepts other types so they can land as
// a purely additive change later.
type ConnectorConfig struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`

	// slack
	AppTokenEnv string   `yaml:"app_token_env"`
	BotTokenEnv string   `yaml:"bot_token_env"`
	Monitor     []string `yaml:"monitor"`
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
	Type string `yaml:"type"` // "slack", "email", or "pagerduty"

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

	// pagerduty
	RoutingKeyEnv string   `yaml:"routing_key_env"`
	Severities    []string `yaml:"severities"`
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

// FromEnv builds a Config from environment variables alone, for the
// no-mounted-config container case. It synthesises a single Slack
// connector with an empty monitor list (auto-discovers every channel
// the bot is in at startup) and a default Slack alerter that posts to
// FOGHORN_ALERT_CHANNEL (default "#alerts"). When their gate env vars
// are set, an email alerter (SMTP_HOST) and a PagerDuty alerter
// (PAGERDUTY_ROUTING_KEY) are added to the Multi fan-out. All other
// knobs take the same defaults Load applies. Token-bearing env vars are
// referenced by name, not captured by value, so log redaction and
// rotation remain orthogonal.
func FromEnv() (*Config, error) {
	alertChan := os.Getenv("FOGHORN_ALERT_CHANNEL")
	if alertChan == "" {
		alertChan = "#alerts"
	}
	alerters := []AlerterConfig{{
		Name:        "ops-slack",
		Type:        "slack",
		BotTokenEnv: "SLACK_BOT_TOKEN",
		Channels:    []string{alertChan},
	}}
	if email, err := emailFromEnv(); err != nil {
		return nil, err
	} else if email != nil {
		alerters = append(alerters, *email)
	}
	if pd, err := pagerdutyFromEnv(); err != nil {
		return nil, err
	} else if pd != nil {
		alerters = append(alerters, *pd)
	}
	c := &Config{
		Connectors: []ConnectorConfig{{
			Name:        "slack-main",
			Type:        "slack",
			AppTokenEnv: "SLACK_APP_TOKEN",
			BotTokenEnv: "SLACK_BOT_TOKEN",
		}},
		Alerters: alerters,
		Store:    StoreConfig{Path: defaultStorePath()},
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("env config: %w", err)
	}
	return c, nil
}

// emailFromEnv returns a fully-populated email AlerterConfig when
// SMTP_HOST is set, nil when unset. Partially-set env (host present
// but a required companion missing) is an error so operators get a
// specific startup failure rather than a silent skip.
func emailFromEnv() (*AlerterConfig, error) {
	host := os.Getenv("SMTP_HOST")
	if host == "" {
		return nil, nil
	}
	portStr := os.Getenv("SMTP_PORT")
	if portStr == "" {
		return nil, errors.New("SMTP_HOST is set but SMTP_PORT is empty — required for email alerter")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return nil, fmt.Errorf("SMTP_PORT %q is not a valid port — required for email alerter", portStr)
	}
	if os.Getenv("SMTP_USERNAME") == "" {
		return nil, errors.New("SMTP_HOST is set but SMTP_USERNAME is empty — required for email alerter")
	}
	if os.Getenv("SMTP_PASSWORD") == "" {
		return nil, errors.New("SMTP_HOST is set but SMTP_PASSWORD is empty — required for email alerter")
	}
	from := os.Getenv("SMTP_FROM")
	if from == "" {
		return nil, errors.New("SMTP_HOST is set but SMTP_FROM is empty — required for email alerter")
	}
	toRaw := os.Getenv("SMTP_TO")
	if toRaw == "" {
		return nil, errors.New("SMTP_HOST is set but SMTP_TO is empty — required for email alerter")
	}
	to := splitCSV(toRaw)
	if len(to) == 0 {
		return nil, errors.New("SMTP_TO contained no addresses after parsing")
	}
	return &AlerterConfig{
		Name:        "ops-email",
		Type:        "email",
		SMTPHost:    host,
		SMTPPort:    port,
		UserEnv:     "SMTP_USERNAME",
		PasswordEnv: "SMTP_PASSWORD",
		From:        from,
		To:          to,
	}, nil
}

// pagerdutyFromEnv returns a PagerDuty AlerterConfig when
// PAGERDUTY_ROUTING_KEY is set, nil when unset. The optional
// PAGERDUTY_SEVERITIES allow-list defaults to ["critical"] inside the
// alerter package when empty here.
func pagerdutyFromEnv() (*AlerterConfig, error) {
	if os.Getenv("PAGERDUTY_ROUTING_KEY") == "" {
		return nil, nil
	}
	return &AlerterConfig{
		Name:          "ops-pagerduty",
		Type:          "pagerduty",
		RoutingKeyEnv: "PAGERDUTY_ROUTING_KEY",
		Severities:    splitCSV(os.Getenv("PAGERDUTY_SEVERITIES")),
	}, nil
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// defaultStorePath returns /var/lib/foghorn/foghorn.db when that
// directory exists (the docker-compose mount point) and the working-
// directory fallback otherwise so a bare `./foghorn run` from source
// doesn't try to write to a system path it can't reach.
func defaultStorePath() string {
	if fi, err := os.Stat("/var/lib/foghorn"); err == nil && fi.IsDir() {
		return "/var/lib/foghorn/foghorn.db"
	}
	return "foghorn.db"
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
	hasConnectors := len(c.Connectors) > 0
	hasLegacy := len(c.Channels.Monitor) > 0
	if !hasConnectors && !hasLegacy {
		return errors.New("connectors: must list at least one entry (or use the deprecated channels.monitor shim)")
	}
	if hasConnectors {
		seen := make(map[string]bool, len(c.Connectors))
		for i, cc := range c.Connectors {
			if cc.Name == "" {
				return fmt.Errorf("connectors[%d]: name required", i)
			}
			if seen[cc.Name] {
				return fmt.Errorf("connectors[%d] %q: duplicate name", i, cc.Name)
			}
			seen[cc.Name] = true
			if cc.Type != "slack" {
				return fmt.Errorf("connectors[%d] %q: unknown type %q (only \"slack\" is supported)",
					i, cc.Name, cc.Type)
			}
			// Empty monitor is allowed: the connector auto-discovers
			// every channel the bot is a member of at startup.
		}
	}
	if c.Channels.AlertTo == "" && len(c.Alerters) == 0 {
		return errors.New("either alerters: or the deprecated channels.alert_to must be configured")
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
		case "pagerduty":
			if ac.RoutingKeyEnv == "" {
				return fmt.Errorf("alerters[%d] %q: routing_key_env required", i, ac.Name)
			}
		default:
			return fmt.Errorf("alerters[%d] %q: unknown type %q", i, ac.Name, ac.Type)
		}
	}
	return nil
}

// MigrateLegacySlack synthesises a single connector entry from the
// deprecated top-level slack: and channels.monitor blocks when no
// connectors: block was provided. Idempotent.
//
// "Legacy" is detected via channels.monitor being non-empty —
// applyDefaults always fills SlackConfig env names, so they aren't a
// reliable signal of user intent. If both connectors: and the legacy
// channels.monitor are present, connectors: wins and a conflict warning
// is emitted.
func (c *Config) MigrateLegacySlack(log *slog.Logger) {
	hasLegacy := len(c.Channels.Monitor) > 0
	if len(c.Connectors) > 0 {
		if hasLegacy && log != nil {
			log.Warn("config: both connectors: and the deprecated channels.monitor are present; connectors: wins")
		}
		return
	}
	if !hasLegacy {
		return
	}
	if log != nil {
		log.Warn("config: top-level slack:/channels.monitor is deprecated; use connectors:")
	}
	c.Connectors = []ConnectorConfig{{
		Name:        "default-slack",
		Type:        "slack",
		AppTokenEnv: c.Slack.AppTokenEnv,
		BotTokenEnv: c.Slack.BotTokenEnv,
		Monitor:     append([]string(nil), c.Channels.Monitor...),
	}}
}

// Tokens resolves the configured Slack app- and bot-token env vars for
// this connector. Errors if either is unset so startup fails fast
// rather than at first handshake.
func (cc ConnectorConfig) Tokens() (app, bot string, err error) {
	app = os.Getenv(cc.AppTokenEnv)
	bot = os.Getenv(cc.BotTokenEnv)
	if app == "" {
		return "", "", fmt.Errorf("env %s is empty", cc.AppTokenEnv)
	}
	if bot == "" {
		return "", "", fmt.Errorf("env %s is empty", cc.BotTokenEnv)
	}
	return app, bot, nil
}

// MonitorSet returns the connector's monitored channel IDs as a set
// for O(1) membership checks at ingest time.
func (cc ConnectorConfig) MonitorSet() map[string]struct{} {
	s := make(map[string]struct{}, len(cc.Monitor))
	for _, id := range cc.Monitor {
		s[id] = struct{}{}
	}
	return s
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
//
// Deprecated: use ConnectorConfig.Tokens() on a specific connector
// entry instead. Kept only for the legacy slack: shim path.
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

// MonitoredChannels returns the union of all configured connectors'
// monitored channel IDs as a set. Falls back to the deprecated
// channels.monitor list when no connectors are configured (pre-migration
// state).
func (c *Config) MonitoredChannels() map[string]struct{} {
	if len(c.Connectors) == 0 {
		s := make(map[string]struct{}, len(c.Channels.Monitor))
		for _, id := range c.Channels.Monitor {
			s[id] = struct{}{}
		}
		return s
	}
	s := make(map[string]struct{})
	for _, cc := range c.Connectors {
		for _, id := range cc.Monitor {
			s[id] = struct{}{}
		}
	}
	return s
}

// ConnectorForChannel returns the name of the first connector whose
// monitor list includes channelID, or "" if no connector matches.
// Used by alert formatting to attach the originating connector's name
// to outbound alerts without plumbing it through every call site.
func (c *Config) ConnectorForChannel(channelID string) string {
	for _, cc := range c.Connectors {
		for _, id := range cc.Monitor {
			if id == channelID {
				return cc.Name
			}
		}
	}
	return ""
}
