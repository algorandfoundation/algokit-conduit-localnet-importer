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
	ctxWithTimeout, cf := context.WithTimeout(ctx, to)
	defer cf()
	status, err := c.StatusAfterBlock(rnd - 1).Do(ctxWithTimeout)
	l.Tracef("importer localnet.waitForRoundWithTimeout() called StatusAfterBlock(%d) err: %v", rnd-1, err)

	if err == nil {
		if rnd <= status.LastRound {
			return status.LastRound, nil
		}
		return 0, NewSyncError(status.LastRound, rnd, fmt.Errorf("sync error, likely due to status after block timeout"))
	}

	// If there was a different error and the node is responsive, call status before returning a SyncError
	status2, err2 := c.Status().Do(ctx)
	l.Tracef("importer localnet.waitForRoundWithTimeout() called Status() err: %v", err2)
	if err2 != nil {
		return 0, fmt.Errorf("unable to get status after block and status: %w", errors.Join(err, err2))
	}
	if status2.LastRound < rnd {
		return 0, NewSyncError(status2.LastRound, rnd, fmt.Errorf("status2.LastRound mismatch: %w", err))
	}

	return 0, fmt.Errorf("unknown errors: StatusAfterBlock(%w), Status(%w)", err, err2)
}

// getBlockInner fetches a block and its associated delta from the follower node
func (li *localnetImporter) getBlockInner(rnd uint64) (data.BlockData, error) {
	var blockbytes []byte
	var blk data.BlockData

	nodeRound, err := waitForRoundWithTimeout(li.ctx, li.logger, li.followerClient, rnd, li.waitForRoundTimeout)
	if err != nil {
		err = fmt.Errorf("called waitForRoundWithTimeout: %w", err)
		li.logger.Error(err.Error())
		return data.BlockData{}, err
	}

	blockbytes, err = li.followerClient.BlockRaw(rnd).Do(li.ctx)
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

// GetBlock implements the Importer interface, fetching a block from the follower node
func (li *localnetImporter) GetBlock(rnd uint64) (data.BlockData, error) {
	state := li.leadState.Load().(leadNodeState)
	li.logger.Tracef("GetBlock(%d) fetching block (lead at %d, sync managed by handler)", rnd, state.Round)

	// Fetch the block - waitForRoundWithTimeout will block until follower has it
	blk, err := li.getBlockInner(rnd)

	if err != nil {
		target := &SyncError{}
		if errors.As(err, &target) {
			li.logger.Warnf("importer localnet.GetBlock() sync error detected: %s (sync handler manages SetSyncRound)", err.Error())
		} else {
			err = fmt.Errorf("importer localnet.GetBlock() error getting block for round %d: %s", rnd, err)
			li.logger.Error(err.Error())
		}
		return data.BlockData{}, err
	}

	return blk, nil
}
