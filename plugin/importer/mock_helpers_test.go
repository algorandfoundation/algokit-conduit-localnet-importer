package importer

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

// mockAlgodServer represents a mock algod node with controllable behavior
type mockAlgodServer struct {
	server       *httptest.Server
	currentRound atomic.Uint64
	genesis      models.Genesis
	blocks       map[uint64]*models.BlockResponse
	deltas       map[uint64]*sdk.LedgerStateDelta

	// Instrumentation for testing
	mu                  sync.Mutex
	setSyncRoundCalls   []setSyncRoundCall
	setSyncRoundLatency time.Duration
	setSyncRoundError   error        // If set, SetSyncRound returns this error
	onSetSyncRound      func(uint64) // Callback when SetSyncRound completes
}

// setSyncRoundCall records a call to SetSyncRound
type setSyncRoundCall struct {
	Round     uint64
	Timestamp time.Time
}

// newMockAlgodServer creates a new mock algod server
func newMockAlgodServer(t *testing.T, initialRound uint64) *mockAlgodServer {
	t.Helper()

	mock := &mockAlgodServer{
		genesis: createTestGenesis(),
		blocks:  make(map[uint64]*models.BlockResponse),
		deltas:  make(map[uint64]*sdk.LedgerStateDelta),
	}
	mock.currentRound.Store(initialRound)

	// Create test blocks for rounds 0 to initialRound
	for i := uint64(0); i <= initialRound; i++ {
		mock.blocks[i] = createTestBlock(i)
		if i > 0 {
			mock.deltas[i] = createTestDelta(i)
		}
	}

	mux := http.NewServeMux()

	// Status endpoint
	mux.HandleFunc("/v2/status", func(w http.ResponseWriter, r *http.Request) {
		status := models.NodeStatus{
			LastRound:   mock.currentRound.Load(),
			LastVersion: "v1",
		}
		json.NewEncoder(w).Encode(status)
	})

	// Status after block endpoint
	mux.HandleFunc("/v2/status/wait-for-block-after/", func(w http.ResponseWriter, r *http.Request) {
		var afterRound int64
		fmt.Sscanf(r.URL.Path, "/v2/status/wait-for-block-after/%d", &afterRound)

		currentRound := mock.currentRound.Load()

		// Handle negative round (e.g., round 0 requests StatusAfterBlock(-1))
		// Return immediately if we're past that round
		if afterRound < 0 || uint64(afterRound) < currentRound {
			status := models.NodeStatus{
				LastRound:   currentRound,
				LastVersion: "v1",
			}
			json.NewEncoder(w).Encode(status)
			return
		}

		// Simple implementation: wait for round to advance or timeout
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				currentRound := mock.currentRound.Load()
				if currentRound > uint64(afterRound) {
					status := models.NodeStatus{
						LastRound:   currentRound,
						LastVersion: "v1",
					}
					json.NewEncoder(w).Encode(status)
					return
				}
			case <-timeout:
				// Return current round even if not advanced
				status := models.NodeStatus{
					LastRound:   mock.currentRound.Load(),
					LastVersion: "v1",
				}
				json.NewEncoder(w).Encode(status)
				return
			case <-r.Context().Done():
				return
			}
		}
	})

	// Genesis endpoint
	mux.HandleFunc("/genesis", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(mock.genesis)
	})

	// Block endpoint
	mux.HandleFunc("/v2/blocks/", func(w http.ResponseWriter, r *http.Request) {
		var round uint64
		fmt.Sscanf(r.URL.Path, "/v2/blocks/%d", &round)

		block, exists := mock.blocks[round]
		if !exists {
			http.Error(w, "block not found", http.StatusNotFound)
			return
		}

		blockBytes := msgpack.Encode(block)
		w.Header().Set("Content-Type", "application/msgpack")
		w.Write(blockBytes)
	})

	// Delta endpoint
	mux.HandleFunc("/v2/deltas/", func(w http.ResponseWriter, r *http.Request) {
		var round uint64
		fmt.Sscanf(r.URL.Path, "/v2/deltas/%d", &round)

		delta, exists := mock.deltas[round]
		if !exists {
			http.Error(w, "delta not found", http.StatusNotFound)
			return
		}

		deltaBytes := msgpack.Encode(delta)
		w.Header().Set("Content-Type", "application/msgpack")
		w.Write(deltaBytes)
	})

	// SetSyncRound endpoint
	mux.HandleFunc("/v2/ledger/sync/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the round number from the URL path
		var targetRound uint64
		fmt.Sscanf(r.URL.Path, "/v2/ledger/sync/%d", &targetRound)

		// Record the call (before any latency or errors)
		mock.mu.Lock()
		mock.setSyncRoundCalls = append(mock.setSyncRoundCalls, setSyncRoundCall{
			Round:     targetRound,
			Timestamp: time.Now(),
		})

		// Check if we should return an error
		syncErr := mock.setSyncRoundError
		latency := mock.setSyncRoundLatency
		callback := mock.onSetSyncRound
		mock.mu.Unlock()

		// Return error if configured (before doing any work)
		if syncErr != nil {
			http.Error(w, syncErr.Error(), http.StatusInternalServerError)
			return
		}

		// Simulate catchup latency (before advancing the round)
		if latency > 0 {
			time.Sleep(latency)
		}

		// Actually advance the follower to the target round (simulating real SetSyncRound behavior)
		if targetRound > 0 {
			currentRound := mock.currentRound.Load()

			// Create blocks/deltas for any missing rounds
			for i := currentRound + 1; i <= targetRound; i++ {
				if _, exists := mock.blocks[i]; !exists {
					mock.blocks[i] = createTestBlock(i)
				}
				if i > 0 {
					if _, exists := mock.deltas[i]; !exists {
						mock.deltas[i] = createTestDelta(i)
					}
				}
			}

			// Advance the follower to the target round
			mock.currentRound.Store(targetRound)
		}

		w.WriteHeader(http.StatusOK)

		// Invoke callback after successful completion (outside lock)
		if callback != nil {
			callback(targetRound)
		}
	})

	mock.server = newIPv4HTTPServer(t, mux)
	return mock
}

// advanceRound advances the mock node to the next round
func (m *mockAlgodServer) advanceRound() uint64 {
	newRound := m.currentRound.Add(1)
	m.blocks[newRound] = createTestBlock(newRound)
	m.deltas[newRound] = createTestDelta(newRound)
	return newRound
}

// setRound sets the mock node to a specific round
func (m *mockAlgodServer) setRound(round uint64) {
	currentRound := m.currentRound.Load()
	for i := currentRound + 1; i <= round; i++ {
		m.blocks[i] = createTestBlock(i)
		if i > 0 {
			m.deltas[i] = createTestDelta(i)
		}
	}
	m.currentRound.Store(round)
}

// close shuts down the mock server
func (m *mockAlgodServer) close() {
	m.server.Close()
}

// setSetSyncRoundLatency configures the simulated latency for SetSyncRound operations
func (m *mockAlgodServer) setSetSyncRoundLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSyncRoundLatency = d
}

// setSetSyncRoundError configures SetSyncRound to return an error
func (m *mockAlgodServer) setSetSyncRoundError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSyncRoundError = err
}

// clearSetSyncRoundError clears any configured SetSyncRound error
func (m *mockAlgodServer) clearSetSyncRoundError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSyncRoundError = nil
}

// getSyncRoundCalls returns a copy of all SetSyncRound calls
func (m *mockAlgodServer) getSyncRoundCalls() []setSyncRoundCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]setSyncRoundCall, len(m.setSyncRoundCalls))
	copy(calls, m.setSyncRoundCalls)
	return calls
}

// clearSyncRoundCalls clears the SetSyncRound call history
func (m *mockAlgodServer) clearSyncRoundCalls() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setSyncRoundCalls = nil
}

// setOnSetSyncRound sets a callback to be invoked when SetSyncRound completes
func (m *mockAlgodServer) setOnSetSyncRound(callback func(uint64)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSetSyncRound = callback
}

// createTestGenesis creates a test genesis block
func createTestGenesis() models.Genesis {
	return models.Genesis{
		Id:        "test-genesis-id",
		Network:   "testnet",
		Proto:     "future",
		Alloc:     []models.GenesisAllocation{},
		Rwd:       "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Fees:      "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Timestamp: 1234567890,
		Comment:   "test genesis block",
		Devmode:   true,
	}
}

// createTestBlock creates a test block for a given round
func createTestBlock(round uint64) *models.BlockResponse {
	var genesisHash sdk.Digest
	copy(genesisHash[:], []byte{1, 2, 3, 4})

	return &models.BlockResponse{
		Block: sdk.Block{
			BlockHeader: sdk.BlockHeader{
				Round:       sdk.Round(round),
				GenesisID:   "test-genesis-id",
				GenesisHash: genesisHash,
				TimeStamp:   1234567890 + int64(round),
			},
			Payset: sdk.Payset{},
		},
		Cert: &map[string]interface{}{},
	}
}

// createTestDelta creates a test ledger state delta for a given round
func createTestDelta(round uint64) *sdk.LedgerStateDelta {
	return &sdk.LedgerStateDelta{
		Hdr: &sdk.BlockHeader{
			Round: sdk.Round(round),
		},
		Accts: sdk.AccountDeltas{
			Accts: []sdk.BalanceRecord{},
		},
	}
}

// createTestConfig creates a valid test configuration and returns it as a JSON string
func createTestConfig(leadURL, followerURL string) string {
	cfg := map[string]interface{}{
		"lead-node-url":           leadURL,
		"follower-node-url":       followerURL,
		"token":                   "test-token",
		"lead-node-poll-interval": "50ms",
		"wait-for-round-timeout":  "5s",
	}
	cfgBytes, _ := json.Marshal(cfg)
	return string(cfgBytes)
}

// createTestConfigWithTokens creates a test configuration with specific tokens
func createTestConfigWithTokens(leadURL, followerURL, token, leadToken, followerToken string) string {
	cfg := map[string]interface{}{
		"lead-node-url":           leadURL,
		"follower-node-url":       followerURL,
		"lead-node-poll-interval": "50ms",
		"wait-for-round-timeout":  "5s",
	}
	if token != "" {
		cfg["token"] = token
	}
	if leadToken != "" {
		cfg["lead-node-token"] = leadToken
	}
	if followerToken != "" {
		cfg["follower-node-token"] = followerToken
	}
	cfgBytes, _ := json.Marshal(cfg)
	return string(cfgBytes)
}

// createTestConfigWithPollInterval creates a test configuration with custom poll interval
func createTestConfigWithPollInterval(leadURL, followerURL string, pollInterval time.Duration) string {
	cfg := map[string]interface{}{
		"lead-node-url":           leadURL,
		"follower-node-url":       followerURL,
		"token":                   "test-token",
		"lead-node-poll-interval": pollInterval.String(),
		"wait-for-round-timeout":  "5s",
	}
	cfgBytes, _ := json.Marshal(cfg)
	return string(cfgBytes)
}

// requireMockServers creates and starts lead and follower mock servers
func requireMockServers(t *testing.T, leadRound, followerRound uint64) (lead *mockAlgodServer, follower *mockAlgodServer) {
	t.Helper()

	lead = newMockAlgodServer(t, leadRound)
	follower = newMockAlgodServer(t, followerRound)

	t.Cleanup(func() {
		lead.close()
		follower.close()
	})

	return lead, follower
}

// newIPv4HTTPServer starts an httptest.Server bound to IPv4 loopback so tests do not rely on IPv6 availability.
func newIPv4HTTPServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create IPv4 listener: %v", err)
	}

	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: handler},
	}
	server.Start()
	t.Cleanup(server.Close)
	return server
}
