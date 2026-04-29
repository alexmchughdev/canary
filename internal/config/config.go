package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Slack     SlackConfig          `yaml:"slack"`
	Channels  ChannelsConfig       `yaml:"channels"`
	Detection DetectionConfig      `yaml:"detection"`
	Store     StoreConfig          `yaml:"store"`
	Metrics   MetricsConfig        `yaml:"metrics"`
	Senders   map[string]SenderCfg `yaml:"senders"`
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

type StoreConfig struct {
	Path string `yaml:"path"`
}

type MetricsConfig struct {
	Addr string `yaml:"addr"`
}

// SenderCfg is a per-sender override. The YAML shape accepts either a
// bare duration string ("5m"), the literal "auto", or a mapping with
// {interval, priority}. UnmarshalYAML normalises all three.
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
	if c.Store.Path == "" {
		c.Store.Path = "foghorn.db"
	}
	if c.Metrics.Addr == "" {
		c.Metrics.Addr = ":9090"
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
	if c.Channels.AlertTo == "" {
		return errors.New("channels.alert_to is required")
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
	return nil
}

// Tokens resolves Slack tokens from the configured env vars. Returns an
// error if either is unset — we fail fast at startup rather than during
// the first Slack handshake.
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

// MonitoredChannels returns the configured channel IDs as a set for
// O(1) membership checks in the message handler.
func (c *Config) MonitoredChannels() map[string]struct{} {
	s := make(map[string]struct{}, len(c.Channels.Monitor))
	for _, id := range c.Channels.Monitor {
		s[id] = struct{}{}
	}
	return s
}
