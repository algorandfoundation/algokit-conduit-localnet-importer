package importer

import (
	"fmt"
	"time"
)

const (
	// PluginName to use when configuring
	PluginName = "localnet_importer"

	// Lead sync configuration defaults
	defaultLeadPollInterval = 100 * time.Millisecond
	// Lead sync limits
	maxLeadPollInterval         = 60 * time.Second
	syncSignalChannelBufferSize = 10
)

// Config specific to the localnet importer
type Config struct {
	// LeadNodeURL is the URL of the lead algod node to poll for latest round information
	LeadNodeURL string `yaml:"lead-node-url"`
	// FollowerNodeURL is the follower Algod network address (must be http or https URL)
	FollowerNodeURL string `yaml:"follower-node-url"`
	// Token is the default API token used for both nodes if specific tokens are not provided
	Token string `yaml:"token"`
	// FollowerNodeToken is the API token for the follower node (defaults to Token if not specified)
	FollowerNodeToken string `yaml:"follower-node-token"`
	// LeadNodeToken is the API token for the lead node (defaults to Token if not specified)
	LeadNodeToken string `yaml:"lead-node-token"`
	// LeadNodePollInterval is how often to poll the lead node for status updates (default: 100ms)
	LeadNodePollInterval time.Duration `yaml:"lead-node-poll-interval"`
	// WaitForRoundTimeout is the maximum time to wait for lead to reach a round (default: 0 = no timeout)
	WaitForRoundTimeout time.Duration `yaml:"wait-for-round-timeout"`
}

// validateConfig validates required configuration fields
func (c *Config) validateRequired() error {
	if c.LeadNodeURL == "" {
		return fmt.Errorf("lead-node-url is required")
	}
	if c.FollowerNodeURL == "" {
		return fmt.Errorf("follower-node-url is required")
	}
	return nil
}

// setDefaults sets default values for optional configuration fields
func (c *Config) setDefaults() {
	if c.LeadNodePollInterval == 0 {
		c.LeadNodePollInterval = defaultLeadPollInterval
	}
}

// validateTimings validates timing configuration values
func (c *Config) validateTimings() error {
	if c.LeadNodePollInterval <= 0 || c.LeadNodePollInterval > maxLeadPollInterval {
		return fmt.Errorf("lead-node-poll-interval must be > 0 and <= %v, got: %v", maxLeadPollInterval, c.LeadNodePollInterval)
	}
	// Allow 0 for wait-for-round-timeout (means no timeout - wait indefinitely)
	if c.WaitForRoundTimeout < 0 {
		return fmt.Errorf("wait-for-round-timeout must be >= 0, got: %v", c.WaitForRoundTimeout)
	}
	return nil
}
