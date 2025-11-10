package importer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/algorand/conduit/conduit/data"
)

// getDelta fetches the ledger state delta for a given round
func (li *localnetImporter) getDelta(rnd uint64) (sdk.LedgerStateDelta, error) {
	var delta sdk.LedgerStateDelta
	params := struct {
		Format string `url:"format,omitempty"`
	}{Format: "msgp"}
	err := (*common.Client)(li.followerClient).GetRawMsgpack(li.ctx, &delta, fmt.Sprintf("/v2/deltas/%d", rnd), params, nil)
	li.logger.Tracef("importer localnet.getDelta() called /v2/deltas/%d err: %v", rnd, err)
	if err != nil {
		return sdk.LedgerStateDelta{}, err
	}

	return delta, nil
}

// waitForRoundWithTimeout blocks until the node reaches the specified round or times out
func waitForRoundWithTimeout(ctx context.Context, l *logrus.Logger, c *algod.Client, rnd uint64, to time.Duration) (uint64, error) {
	// If timeout is 0, wait indefinitely (don't create a timeout context)
	var ctxWithTimeout context.Context
	var cf context.CancelFunc
	if to > 0 {
		ctxWithTimeout, cf = context.WithTimeout(ctx, to)
		defer cf()
	} else {
		ctxWithTimeout = ctx
		cf = func() {} // No-op cancel function
		defer cf()
	}

	status, err := c.StatusAfterBlock(rnd - 1).Do(ctxWithTimeout)
	l.Tracef("importer localnet.waitForRoundWithTimeout() called StatusAfterBlock(%d) err: %v", rnd-1, err)

	if err == nil {
		// When c.StatusAfterBlock has a server-side timeout it returns the current status.
		// We use a context with timeout and the algod default timeout is 1 minute, so technically
		// with the current versions, this check should never be required.
		if rnd <= status.LastRound {
			return status.LastRound, nil
		}
		// algod's timeout should not be reached because context.WithTimeout is used
		return 0, NewSyncError(status.LastRound, rnd, fmt.Errorf("sync error, likely due to status after block timeout"))
	}

	// If there was a different error and the node is responsive, call status before returning a SyncError
	status2, err2 := c.Status().Do(ctx)
	l.Tracef("importer localnet.waitForRoundWithTimeout() called Status() err: %v", err2)
	if err2 != nil {
		// If there was an error getting status, return the original error
		return 0, fmt.Errorf("unable to get status after block and status: %w", errors.Join(err, err2))
	}
	if status2.LastRound < rnd {
		return 0, NewSyncError(status2.LastRound, rnd, fmt.Errorf("status2.LastRound mismatch: %w", err))
	}

	// This is probably a connection error, not a SyncError
	return 0, fmt.Errorf("unknown errors: StatusAfterBlock(%w), Status(%w)", err, err2)
}

// getBlockInner fetches a block and its associated delta from the follower node
// This matches the original conduit algod importer structure, but without waitForRoundWithTimeout
// (which is called explicitly in GetBlock)
func (li *localnetImporter) getBlockInner(rnd uint64, nodeRound uint64) (data.BlockData, error) {
	var blockbytes []byte
	var blk data.BlockData

	blockbytes, err := li.followerClient.BlockRaw(rnd).Do(li.ctx)
	li.logger.Tracef("importer localnet.GetBlock() called BlockRaw(%d) err: %v", rnd, err)
	if err != nil {
		err = fmt.Errorf("error getting block for round %d: %w", rnd, err)
		li.logger.Error(err.Error())
		return data.BlockData{}, err
	}

	tmpBlk := new(models.BlockResponse)
	err = msgpack.Decode(blockbytes, tmpBlk)
	if err != nil {
		return blk, fmt.Errorf("error decoding block for round %d: %w", rnd, err)
	}

	blk.BlockHeader = tmpBlk.Block.BlockHeader
	blk.Payset = tmpBlk.Block.Payset
	blk.Certificate = tmpBlk.Cert

	// Fetch the state delta (follower mode always has deltas)
	// Round 0 has no delta associated with it
	if rnd != 0 {
		var delta sdk.LedgerStateDelta
		delta, err = li.getDelta(rnd)
		if err != nil {
			if nodeRound < rnd {
				err = fmt.Errorf("ledger state delta not found: node round (%d) is behind required round (%d), ensure follower node has its sync round set to the required round: %w", nodeRound, rnd, err)
			} else {
				err = fmt.Errorf("ledger state delta not found: node round (%d), required round (%d): verify follower node configuration and ensure follower node has its sync round set to the required round, re-deploying the follower node may be necessary: %w", nodeRound, rnd, err)
			}
			li.logger.Error(err.Error())
			return data.BlockData{}, err
		}
		blk.Delta = &delta
	}

	return blk, err
}

// waitForLeadToReach blocks until the lead node has reached or passed the target round
// If waitForRoundTimeout is 0, it will wait indefinitely
func (li *localnetImporter) waitForLeadToReach(rnd uint64) error {
	state := li.leadState.Load().(leadNodeState)

	// Fast path: lead already has this round
	if state.Round >= rnd {
		li.logger.Tracef("Lead already at round %d (requested: %d)", state.Round, rnd)
		return nil
	}

	li.logger.Debugf("Waiting for lead to reach round %d (currently at %d)", rnd, state.Round)

	// Setup timeout channel (nil if disabled)
	var timeoutChan <-chan time.Time
	if li.waitForRoundTimeout > 0 {
		timeoutChan = time.After(li.waitForRoundTimeout)
		li.logger.Tracef("Timeout enabled: %v", li.waitForRoundTimeout)
	} else {
		li.logger.Tracef("Timeout disabled, will wait indefinitely")
	}

	// Wait for lead to advance
	for {
		select {
		case <-timeoutChan:
			// This case only fires if timeoutChan is not nil
			state = li.leadState.Load().(leadNodeState)
			return fmt.Errorf(
				"timeout waiting for lead to reach round %d (lead at %d after %v)",
				rnd, state.Round, li.waitForRoundTimeout)

		case <-li.ctx.Done():
			return fmt.Errorf("context cancelled while waiting for lead round %d: %w", rnd, li.ctx.Err())

		case leadRound, ok := <-li.syncSignal:
			if !ok {
				return fmt.Errorf("sync signal channel closed while waiting for lead round %d", rnd)
			}

			li.logger.Tracef("Received lead advancement signal: round %d", leadRound)

			// Check if lead has reached our target
			if leadRound >= rnd {
				li.logger.Debugf("Lead reached round %d (requested: %d)", leadRound, rnd)
				return nil
			}

			// Not there yet, continue waiting
			li.logger.Tracef("Lead at %d, still waiting for %d", leadRound, rnd)
		}
	}
}

// GetBlock implements the Importer interface, fetching a block from the follower node
func (li *localnetImporter) GetBlock(rnd uint64) (data.BlockData, error) {
	// Wait for lead to have this round available
	err := li.waitForLeadToReach(rnd)
	if err != nil {
		return data.BlockData{}, fmt.Errorf("GetBlock(%d): %w", rnd, err)
	}

	// Tell follower to sync to this round (idempotent - safe to call multiple times)
	li.logger.Tracef("GetBlock(%d): calling SetSyncRound", rnd)
	_, err = li.followerClient.SetSyncRound(rnd).Do(li.ctx)
	
	if err != nil {
		return data.BlockData{}, fmt.Errorf("GetBlock(%d): SetSyncRound failed: %w", rnd, err)
	}

	// Wait for follower to reach the round
	nodeRound, err := waitForRoundWithTimeout(li.ctx, li.logger, li.followerClient, rnd, li.waitForRoundTimeout)
	if err != nil {
		target := &SyncError{}
		if errors.As(err, &target) {
			li.logger.Warnf("GetBlock(%d) sync error: %s", rnd, err.Error())
		} else {
			err = fmt.Errorf("GetBlock(%d): waitForRoundWithTimeout failed: %w", rnd, err)
			li.logger.Error(err.Error())
		}
		return data.BlockData{}, err
	}

	// Fetch the block - getBlockInner will fetch block and delta
	blk, err := li.getBlockInner(rnd, nodeRound)
	if err != nil {
		return data.BlockData{}, err
	}

	return blk, nil
}
