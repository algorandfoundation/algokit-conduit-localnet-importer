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
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
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
	pollingCtx    context.Context    // context for background goroutine
	pollingCancel context.CancelFunc // cancel function for background goroutine
	pollingWg     sync.WaitGroup     // wait group for background goroutine
	syncSignal    chan uint64        // channel to signal follower sync requests
	syncWg        sync.WaitGroup     // wait group for sync handler goroutine
}

func (li *localnetImporter) Metadata() plugins.Metadata {
	return metadata
}

func (li *localnetImporter) Config() string {
	ret, _ := yaml.Marshal(li.cfg)
	return string(ret)
}

func (li *localnetImporter) OnComplete(input data.BlockData) error {
	// In lead-based sync mode, the sync handler goroutine manages SetSyncRound
	// We don't need to do anything here, but the function is required by the interface
	return nil
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

	// Get initial lead node status - blocks until available
	li.logger.Info("Fetching lead node status for initialization...")
	leadStatus, err := li.waitForLeadStatus()
	if err != nil {
		return fmt.Errorf("failed to get initial lead node status: %w", err)
	}

	leadRound := leadStatus.LastRound
	li.logger.Infof("Lead node is at round %d", leadRound)

	// Initialize lead state with initial round and timestamp
	initialState := leadNodeState{
		Round:     leadRound,
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
		return fmt.Errorf("failed to get genesis from follower: %w", err)
	}

	if reflect.DeepEqual(genesisResponse, models.Genesis{}) {
		return fmt.Errorf("unable to fetch genesis file from follower API at %s", li.cfg.FollowerNodeURL)
	}

	// Convert genesis response to SDK format
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
		stateBytes := msgpack.Encode(alloc.State)
		if stateBytes == nil {
			return fmt.Errorf("error converting allocation state for address %s", alloc.Addr)
		}
		err = msgpack.Decode(stateBytes, &state)
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

	// Connect lead and follower - initialize sync
	li.logger.Info("Connecting lead and follower nodes...")

	// Create polling context
	li.pollingCtx, li.pollingCancel = context.WithCancel(li.ctx)

	// Create sync signal channel with buffer
	li.syncSignal = make(chan uint64, syncSignalChannelBufferSize)

	// Set the sync round on the follower to start at the lead's current round
	li.logger.Infof("Setting follower sync round to %d", leadRound)
	_, err = li.followerClient.SetSyncRound(leadRound).Do(li.ctx)
	if err != nil {
		return fmt.Errorf("failed to set initial sync round on follower: %w", err)
	}

	// Start background goroutines for lead monitoring and follower sync
	li.logger.Info("Starting background sync goroutines...")
	li.startLeadNodePolling()
	li.startFollowerSyncHandler()

	li.logger.Infof("Lead-based sync enabled, initial round: %d at %s (UTC), poll interval: %v",
		initialState.Round,
		formatTimestamp(initialState.Timestamp),
		li.cfg.LeadNodePollInterval)

	return nil
}

func (li *localnetImporter) GetGenesis() (*sdk.Genesis, error) {
	if li.genesis != nil {
		return li.genesis, nil
	}
	return nil, fmt.Errorf("genesis not available: GetGenesis() should be called only after Init()")
}

func (li *localnetImporter) Close() error {
	// Cancel background goroutines
	if li.pollingCancel != nil {
		// First, stop the polling goroutine
		li.pollingCancel()
		li.pollingWg.Wait()
		li.logger.Debug("Lead polling goroutine stopped")

		// Then close the sync signal channel and wait for sync handler
		if li.syncSignal != nil {
			close(li.syncSignal)
			li.syncWg.Wait()
			li.logger.Debug("Follower sync handler stopped")
		}
	}

	if li.cancel != nil {
		li.cancel()
	}
	return nil
}
