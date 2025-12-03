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

func TestImporterInitNoToken(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 5)
	cfgStr := createTestConfigWithTokens(lead.server.URL, follower.server.URL, "", "", "")

	importer := &localnetImporter{}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipelineRound := sdk.Round(0)
	err := importer.Init(ctx, conduit.MakePipelineInitProvider(&pipelineRound, nil, nil), plugins.MakePluginConfig(cfgStr), logger)
	require.NoError(t, err)
	defer importer.Close()
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

func TestGetBlockWaitsForLead(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 10)
	importer := setupTestImporter(t, lead, follower)

	sendLeadSignal(importer, 15)
	lead.setRound(15)

	block, err := importer.GetBlock(15)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), block.Round())
}

func TestGetBlockCallsSetSyncRound(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 20, 10)
	importer := setupTestImporter(t, lead, follower)

	follower.clearSyncRoundCalls()

	block, err := importer.GetBlock(15)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), block.Round())

	calls := follower.getSyncRoundCalls()
	require.GreaterOrEqual(t, len(calls), 1, "SetSyncRound should have been called")
	assert.Equal(t, uint64(15), calls[len(calls)-1].Round, "SetSyncRound should be called with requested round")
}

func TestOnCompleteAdvancesFollower(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 20, 10)
	importer := setupTestImporter(t, lead, follower)

	block, err := importer.GetBlock(15)
	require.NoError(t, err)

	follower.clearSyncRoundCalls()

	err = importer.OnComplete(block)
	require.NoError(t, err)

	calls := follower.getSyncRoundCalls()
	require.Len(t, calls, 1, "OnComplete should call SetSyncRound once")
	assert.Equal(t, uint64(16), calls[0].Round, "OnComplete should call SetSyncRound with round+1")
}

func TestWaitForLeadTimeout(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 10, 10)
	importer := setupTestImporter(t, lead, follower)

	importer.waitForRoundTimeout = 1 * time.Millisecond

	_, err := importer.GetBlock(100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout waiting for lead to reach round")
}

func TestWaitForLeadFastPath(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 20, 10)
	importer := setupTestImporter(t, lead, follower)

	block, err := importer.GetBlock(15)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), block.Round())
}

func TestGetBlockAndOnCompleteFlow(t *testing.T) {
	t.Parallel()

	lead, follower := requireMockServers(t, 20, 5)
	importer := setupTestImporter(t, lead, follower)

	follower.clearSyncRoundCalls()

	for round := uint64(10); round <= 12; round++ {
		block, err := importer.GetBlock(round)
		require.NoError(t, err, "GetBlock(%d) should succeed", round)
		assert.Equal(t, round, block.Round())

		err = importer.OnComplete(block)
		require.NoError(t, err, "OnComplete(%d) should succeed", round)
	}

	calls := follower.getSyncRoundCalls()

	require.GreaterOrEqual(t, len(calls), 6, "Should have SetSyncRound calls for GetBlock and OnComplete")

	lastCall := calls[len(calls)-1]
	assert.Equal(t, uint64(13), lastCall.Round, "Last SetSyncRound should be for round 13")
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
