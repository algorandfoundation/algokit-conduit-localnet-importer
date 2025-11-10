package importer

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

//go:embed sample.yaml
var sampleConfig string

// metadata contains information about the plugin used for CLI helpers
var metadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Localnet importer with lead-based sync (follower mode only).",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	importers.Register(PluginName, importers.ImporterConstructorFunc(func() importers.Importer {
		return &localnetImporter{}
	}))
}

// localnetImporter is the object which implements the importer plugin interface
type localnetImporter struct {
	// Follower node client
	followerClient *algod.Client
	logger         *logrus.Logger
	cfg            Config
	ctx            context.Context
	cancel         context.CancelFunc
	genesis        *sdk.Genesis

	// Configuration-derived timeouts
	waitForRoundTimeout time.Duration

	// Lead node sync fields
	leadClient    *algod.Client
	leadState     atomic.Value       // stores leadNodeState
	pollingCtx    context.Context    // context for lead polling goroutine
	pollingCancel context.CancelFunc // cancel function for lead polling goroutine
	pollingWg     sync.WaitGroup     // wait group for lead polling goroutine
	syncSignal    chan uint64        // channel for lead advancement notifications
}

func (li *localnetImporter) Metadata() plugins.Metadata {
	return metadata
}

func (li *localnetImporter) Config() string {
	ret, _ := yaml.Marshal(li.cfg)
	return string(ret)
}

func (li *localnetImporter) OnComplete(input data.BlockData) error {
	// Advance the follower's sync round after successfully processing a block
	// This ensures the follower stays ahead of Conduit's processing position
	nextRound := input.Round() + 1

	// Check if lead has reached nextRound before advancing follower
	leadState := li.leadState.Load().(leadNodeState)
	if leadState.Round < nextRound {
		li.logger.Tracef("OnComplete(%d): skipping SetSyncRound(%d) - lead only at round %d",
			input.Round(), nextRound, leadState.Round)
		return nil
	}

	_, err := li.followerClient.SetSyncRound(nextRound).Do(li.ctx)
	li.logger.Tracef("OnComplete(%d): called SetSyncRound(%d) err: %v", input.Round(), nextRound, err)
	return err
}

func (li *localnetImporter) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	li.ctx, li.cancel = context.WithCancel(ctx)
	li.logger = logger

	// Unmarshal configuration
	if err := cfg.UnmarshalConfig(&li.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}

	// Validate all required configuration
	if err := li.cfg.validateRequired(); err != nil {
		return err
	}

	// Set defaults and validate timing configurations
	li.cfg.setDefaults()
	if err := li.cfg.validateTimings(); err != nil {
		return err
	}

	li.waitForRoundTimeout = li.cfg.WaitForRoundTimeout

	// Configure lead node (source of truth)
	li.logger.Info("Configuring lead node...")

	// Parse and validate lead node URL
	leadURL, err := url.Parse(li.cfg.LeadNodeURL)
	if err != nil {
		return fmt.Errorf("invalid lead-node-url: %w", err)
	}
	if leadURL.Scheme != "http" && leadURL.Scheme != "https" {
		li.cfg.LeadNodeURL = "http://" + li.cfg.LeadNodeURL
		li.logger.Infof("Added http prefix to lead node URL: %s", li.cfg.LeadNodeURL)
	}

	// Determine lead node token (use default token if not specified)
	leadToken := li.cfg.LeadNodeToken
	if leadToken == "" {
		leadToken = li.cfg.Token
		if leadToken != "" {
			li.logger.Info("Lead node token not specified, using default token")
		}
	}

	// Validate that we have a token for the lead node
	if leadToken == "" {
		return fmt.Errorf("no token provided for lead node: must set either 'lead-node-token' or 'token'")
	}

	// Create lead node client
	li.leadClient, err = algod.MakeClient(li.cfg.LeadNodeURL, leadToken)
	if err != nil {
		return fmt.Errorf("failed to create lead node client: %w", err)
	}

	// Initialize lead state with zero round (will be populated by polling goroutine)
	initialState := leadNodeState{
		Round:     0,
		Timestamp: time.Now().UTC(),
	}
	li.leadState.Store(initialState)

	// Configure follower node
	li.logger.Info("Configuring follower node...")

	// Parse and validate follower URL
	followerURL, err := url.Parse(li.cfg.FollowerNodeURL)
	if err != nil {
		return fmt.Errorf("invalid follower-node-url: %w", err)
	}
	if followerURL.Scheme != "http" && followerURL.Scheme != "https" {
		li.cfg.FollowerNodeURL = "http://" + li.cfg.FollowerNodeURL
		li.logger.Infof("Added http prefix to follower node URL: %s", li.cfg.FollowerNodeURL)
	}

	// Determine follower node token (use default token if not specified)
	followerToken := li.cfg.FollowerNodeToken
	if followerToken == "" {
		followerToken = li.cfg.Token
		if followerToken != "" {
			li.logger.Info("Follower node token not specified, using default token")
		}
	}

	// Validate that we have a token for the follower node
	if followerToken == "" {
		return fmt.Errorf("no token provided for follower node: must set either 'follower-node-token' or 'token'")
	}

	// Create follower client
	li.followerClient, err = algod.MakeClient(li.cfg.FollowerNodeURL, followerToken)
	if err != nil {
		return fmt.Errorf("failed to create follower client: %w", err)
	}

	// Fetch genesis from follower node
	genesisResponse, err := li.followerClient.GetGenesis().Do(li.ctx)
	if err != nil {
		return err
	}

	if reflect.DeepEqual(genesisResponse, models.Genesis{}) {
		return fmt.Errorf("unable to fetch genesis file from API at %s", li.cfg.FollowerNodeURL)
	}

	genesis := sdk.Genesis{
		SchemaID:    genesisResponse.Id,
		Network:     genesisResponse.Network,
		Proto:       genesisResponse.Proto,
		Allocation:  make([]sdk.GenesisAllocation, len(genesisResponse.Alloc)),
		RewardsPool: genesisResponse.Rwd,
		FeeSink:     genesisResponse.Fees,
		Timestamp:   int64(genesisResponse.Timestamp),
		Comment:     genesisResponse.Comment,
		DevMode:     genesisResponse.Devmode,
	}

	// Convert allocations
	for i, alloc := range genesisResponse.Alloc {
		var state sdk.Account
		stateBytes := json.Encode(alloc.State)
		if stateBytes == nil {
			return fmt.Errorf("error converting allocation state for address %s: %w", alloc.Addr, err)
		}
		err = json.LenientDecode(stateBytes, &state)
		if err != nil {
			return fmt.Errorf("error unmarshaling allocation state: %w", err)
		}
		genesis.Allocation[i] = sdk.GenesisAllocation{
			Address: alloc.Addr,
			Comment: alloc.Comment,
			State:   state,
		}
	}

	li.genesis = &genesis

	// Setup lead monitoring
	li.logger.Info("Setting up lead node monitoring...")

	// Create polling context for lead monitoring
	li.pollingCtx, li.pollingCancel = context.WithCancel(li.ctx)

	// Create sync signal channel for lead advancement notifications
	li.syncSignal = make(chan uint64, syncSignalChannelBufferSize)

	// Start lead polling goroutine (for monitoring lead position)
	li.startLeadNodePolling()

	// Check follower position relative to Conduit
	followerStatus, err := li.followerClient.Status().Do(li.ctx)
	if err != nil {
		return fmt.Errorf("failed to get follower status: %w", err)
	}

	conduitNextRound := uint64(initProvider.NextDBRound())

	if followerStatus.LastRound > conduitNextRound+100 {
		li.logger.Warnf(
			"WARNING: Follower is ahead (round %d) of Conduit (round %d). "+
				"Deltas may be unavailable for early rounds. "+
				"Consider resetting follower node to genesis.",
			followerStatus.LastRound, conduitNextRound)
	}

	li.logger.Infof("GetBlock-driven sync enabled. Follower will track Conduit's processing.")

	// Log current lead state
	currentState := li.leadState.Load().(leadNodeState)
	if currentState.Round > 0 {
		li.logger.Infof("Lead monitoring active: current round %d at %s (UTC), poll interval: %v",
			currentState.Round,
			formatTimestamp(currentState.Timestamp),
			li.cfg.LeadNodePollInterval)
	} else {
		li.logger.Infof("Lead monitoring active: waiting for first poll, poll interval: %v",
			li.cfg.LeadNodePollInterval)
	}

	return nil
}

func (li *localnetImporter) GetGenesis() (*sdk.Genesis, error) {
	if li.genesis != nil {
		return li.genesis, nil
	}
	return nil, fmt.Errorf("genesis not available: GetGenesis() should be called only after Init()")
}

func (li *localnetImporter) Close() error {
	// Stop lead polling goroutine
	if li.pollingCancel != nil {
		li.pollingCancel()
		li.pollingWg.Wait()
		li.logger.Debug("Lead polling goroutine stopped")
	}

	// Close sync signal channel
	if li.syncSignal != nil {
		close(li.syncSignal)
		li.logger.Debug("Sync signal channel closed")
	}

	if li.cancel != nil {
		li.cancel()
	}
	return nil
}
