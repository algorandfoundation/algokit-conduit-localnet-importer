package importer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/algorand/conduit/conduit"
	"github.com/algorand/conduit/conduit/plugins"
)

func TestImporterMetadata(t *testing.T) {
	t.Parallel()

	importer := &localnetImporter{}
	metadata := importer.Metadata()

	assert.Equal(t, PluginName, metadata.Name)
	assert.NotEmpty(t, metadata.Description)
	assert.False(t, metadata.Deprecated)
	assert.NotEmpty(t, metadata.SampleConfig)
}

func TestImporterGetGenesisBeforeInit(t *testing.T) {
	t.Parallel()

	importer := &localnetImporter{}
	genesis, err := importer.GetGenesis()

	assert.Error(t, err)
	assert.Nil(t, genesis)
	assert.Contains(t, err.Error(), "genesis not available")
}

func TestImporterInitWithValidConfig(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 5)
	cfgStr := createTestConfig(lead.server.URL, follower.server.URL)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.NoError(t, err)

	genesis, err := importer.GetGenesis()
	require.NoError(t, err)
	assert.NotNil(t, genesis)
	assert.Equal(t, "test-genesis-id", genesis.SchemaID)

	err = importer.Close()
	assert.NoError(t, err)
}

func TestImporterInitMissingLeadURL(t *testing.T) {
	t.Parallel()

	_, follower := requireMockServers(t, 10, 5)
	cfgStr := createTestConfig("", follower.server.URL)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lead-node-url is required")
}

func TestImporterInitMissingFollowerURL(t *testing.T) {
	t.Parallel()

	lead, _ := requireMockServers(t, 10, 5)
	cfgStr := createTestConfig(lead.server.URL, "")

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "follower-node-url is required")
}

func TestImporterInitMissingToken(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 5)
	cfgStr := createTestConfigWithTokens(lead.server.URL, follower.server.URL, "", "", "")

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no token provided")
}

func TestImporterInitInvalidTimings(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 5)
	cfg := map[string]interface{}{
		"lead-node-url":           lead.server.URL,
		"follower-node-url":       follower.server.URL,
		"token":                   "test-token",
		"lead-node-poll-interval": "61s",
		"wait-for-round-timeout":  "5s",
	}
	cfgBytes, _ := json.Marshal(cfg)
	cfgStr := string(cfgBytes)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lead-node-poll-interval must be")
}

func TestImporterGetBlock(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 10)
	cfgStr := createTestConfig(lead.server.URL, follower.server.URL)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.NoError(t, err)
	defer importer.Close()

	block, err := importer.GetBlock(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), block.Round())
	assert.NotNil(t, block.Delta)
}

func TestImporterGetBlockRound0(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 10)
	cfgStr := createTestConfig(lead.server.URL, follower.server.URL)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.NoError(t, err)
	defer importer.Close()

	block, err := importer.GetBlock(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), block.Round())
	assert.Nil(t, block.Delta)
}

func TestImporterSyncScenarios(t *testing.T) {
	t.Parallel()

	type burst func(*mockAlgodServer) uint64

	singleAdvance := func() burst {
		return func(lead *mockAlgodServer) uint64 {
			return lead.advanceRound()
		}
	}

	advanceMany := func(count int) burst {
		return func(lead *mockAlgodServer) uint64 {
			var round uint64
			for i := 0; i < count; i++ {
				round = lead.advanceRound()
			}
			return round
		}
	}

	tests := []struct {
		name    string
		bursts  []burst
		pollDur time.Duration
	}{
		{
			name:    "sequential advancements",
			pollDur: 50 * time.Millisecond,
			bursts: []burst{
				singleAdvance(),
				singleAdvance(),
				singleAdvance(),
			},
		},
		{
			name:    "burst advancements drain queue",
			pollDur: 50 * time.Millisecond,
			bursts: []burst{
				advanceMany(3),
				advanceMany(2),
				advanceMany(5),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lead, follower := requireMockServers(t, 10, 5)
			syncComplete := make(chan uint64, 5)
			follower.setOnSetSyncRound(func(round uint64) {
				syncComplete <- round
			})

			cfgStr := createTestConfigWithPollInterval(lead.server.URL, follower.server.URL, tt.pollDur)

			importer := &localnetImporter{}
			logger := logrus.New()
			logger.SetLevel(logrus.ErrorLevel)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pipelineRound := sdk.Round(0)
			err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
			require.NoError(t, err)
			defer importer.Close()

			waitForSyncRound(t, syncComplete, 10)
			follower.clearSyncRoundCalls()

			var expectedRounds []uint64
			for _, burst := range tt.bursts {
				expected := burst(lead)
				expectedRounds = append(expectedRounds, expected)
				waitForSyncRound(t, syncComplete, expected)
			}

			assertChannelEmpty(t, syncComplete)

			calls := follower.getSyncRoundCalls()
			require.Len(t, calls, len(expectedRounds))
			for i, call := range calls {
				assert.Equal(t, expectedRounds[i], call.Round)
			}

			if len(expectedRounds) > 0 {
				assert.Equal(t, expectedRounds[len(expectedRounds)-1], follower.currentRound.Load())
			}
		})
	}
}

func TestDrainSyncSignals(t *testing.T) {
	t.Parallel()

	importer := &localnetImporter{syncSignal: make(chan uint64, 4)}

	importer.syncSignal <- 4
	importer.syncSignal <- 8
	highest, count := importer.drainSyncSignals(2)
	assert.Equal(t, uint64(8), highest)
	assert.Equal(t, 2, count)

	importer.syncSignal <- 1
	close(importer.syncSignal)
	highest, count = importer.drainSyncSignals(5)
	assert.Equal(t, uint64(5), highest)
	assert.Equal(t, 1, count)
}

func TestWaitForRoundWithTimeoutSyncError(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/status/wait-for-block-after/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	})
	mux.HandleFunc("/v2/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(models.NodeStatus{LastRound: 5})
	})

	server := newIPv4HTTPServer(t, mux)

	client, err := algod.MakeClient(server.URL, "test-token")
	require.NoError(t, err)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	_, err = waitForRoundWithTimeout(context.Background(), logger, client, 10, 50*time.Millisecond)
	require.Error(t, err)

	var syncErr *SyncError
	require.True(t, errors.As(err, &syncErr))
	assert.Equal(t, uint64(5), syncErr.retrievedRound)
	assert.Equal(t, uint64(10), syncErr.expectedRound)
}

func TestWaitForRoundWithTimeoutStatusFailure(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/status/wait-for-block-after/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})
	mux.HandleFunc("/v2/status", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "status unavailable", http.StatusInternalServerError)
	})

	server := newIPv4HTTPServer(t, mux)

	client, err := algod.MakeClient(server.URL, "test-token")
	require.NoError(t, err)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	_, err = waitForRoundWithTimeout(context.Background(), logger, client, 10, 50*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to get status after block and status")
}

func TestImporterSetSyncRoundFailureRetry(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 5)

	syncAttempts := make(chan uint64, 10)
	follower.setOnSetSyncRound(func(round uint64) {
		syncAttempts <- round
	})

	cfgStr := createTestConfigWithPollInterval(lead.server.URL, follower.server.URL, 50*time.Millisecond)

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.NoError(t, err)
	defer importer.Close()

	// Wait for initial sync to complete
	waitForSyncRound(t, syncAttempts, 10)
	follower.clearSyncRoundCalls()

	// Inject SetSyncRound failure
	follower.setSetSyncRoundError(errors.New("simulated catchup failure"))

	// Advance lead to round 15
	lead.setRound(15)

	// Wait for sync attempts to be made (poll interval is 50ms, so wait ~3 poll cycles)
	time.Sleep(200 * time.Millisecond)

	// Verify sync was attempted but follower didn't advance due to error
	calls := follower.getSyncRoundCalls()
	assert.Greater(t, len(calls), 0, "SetSyncRound should have been attempted despite error")
	assert.Equal(t, uint64(10), follower.currentRound.Load(), "follower should still be at round 10 due to sync error")

	// Advance lead to round 18, verify more attempts but still no advancement
	previousCallCount := len(calls)
	lead.setRound(18)
	time.Sleep(200 * time.Millisecond)

	calls = follower.getSyncRoundCalls()
	assert.Greater(t, len(calls), previousCallCount, "more SetSyncRound attempts should have been made")
	assert.Equal(t, uint64(10), follower.currentRound.Load(), "follower should still be at round 10 due to sync error")

	// Clear the error and verify sync succeeds
	follower.clearSetSyncRoundError()
	lead.setRound(20)
	waitForSyncRound(t, syncAttempts, 20)

	assert.Equal(t, uint64(20), follower.currentRound.Load(), "follower should now be at round 20 after error cleared")

	// Verify final call count shows multiple attempts were made
	calls = follower.getSyncRoundCalls()
	assert.GreaterOrEqual(t, len(calls), 3, "should have made multiple sync attempts across all rounds")

	// Verify we can fetch the block at round 20
	block, err := importer.GetBlock(20)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), block.Round())
}

func TestImporterCloseWithoutInit(t *testing.T) {
	t.Parallel()

	importer := &localnetImporter{}
	err := importer.Close()
	assert.NoError(t, err)
}

func TestImporterTokenFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		token         string
		leadToken     string
		followerToken string
		expectError   bool
		errorContains string
	}{
		{
			name:          "Default token only",
			token:         "default-token",
			leadToken:     "",
			followerToken: "",
		},
		{
			name:          "Specific lead token",
			token:         "",
			leadToken:     "lead-token",
			followerToken: "follower-token",
		},
		{
			name:          "Mixed tokens",
			token:         "default-token",
			leadToken:     "lead-token",
			followerToken: "",
		},
		{
			name:          "No tokens provided",
			token:         "",
			leadToken:     "",
			followerToken: "",
			expectError:   true,
			errorContains: "no token provided",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lead, follower := requireMockServers(t, 10, 5)
			cfgStr := createTestConfigWithTokens(lead.server.URL, follower.server.URL, tt.token, tt.leadToken, tt.followerToken)

			importer := &localnetImporter{}
			logger := logrus.New()
			logger.SetLevel(logrus.ErrorLevel)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			pipelineRound := sdk.Round(0)
			err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				defer importer.Close()
			}
		})
	}
}

func waitForSyncRound(t *testing.T, ch <-chan uint64, expected uint64) {
	t.Helper()
	select {
	case round := <-ch:
		require.Equal(t, expected, round)
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for sync round %d", expected)
	}
}

func assertChannelEmpty(t *testing.T, ch <-chan uint64) {
	t.Helper()
	select {
	case round := <-ch:
		t.Fatalf("unexpected sync round %d", round)
	default:
	}
}
