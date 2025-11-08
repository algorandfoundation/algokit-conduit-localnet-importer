package importer

// drainSyncSignals drains all pending signals from the sync signal channel and returns the highest round number
func (li *localnetImporter) drainSyncSignals(initial uint64) (highest uint64, count int) {
	highest = initial
	for {
		select {
		case r, ok := <-li.syncSignal:
			if !ok {
				// Channel closed while draining
				return highest, count
			}
			count++
			if r > highest {
				highest = r
			}
		default:
			// No more signals in queue
			return highest, count
		}
	}
}

// startFollowerSyncHandler starts a background goroutine that processes sync signals
// and calls SetSyncRound to keep the follower synchronized with the lead node
func (li *localnetImporter) startFollowerSyncHandler() {
	li.syncWg.Add(1)
	go func() {
		defer li.syncWg.Done()

		// Panic recovery to ensure goroutine never crashes unexpectedly
		defer func() {
			if r := recover(); r != nil {
				li.logger.Errorf("PANIC in follower sync handler goroutine (recovered): %v", r)
				li.logger.Error("Follower sync handler goroutine terminated due to panic - this should not happen")
			}
		}()

		li.logger.Info("Follower sync handler goroutine started")

		for {
			select {
			case targetRound, ok := <-li.syncSignal:
				if !ok {
					// Channel closed, exit gracefully (only happens during shutdown)
					li.logger.Info("Sync signal channel closed, stopping sync handler goroutine")
					return
				}

				// Validate round number is sane
				if targetRound == 0 {
					li.logger.Warn("Received sync signal with round 0, ignoring")
					continue
				}

				// Drain channel to get the highest round in queue
				highest, drained := li.drainSyncSignals(targetRound)
				if drained > 0 {
					li.logger.Debugf("Drained %d sync signals, syncing to highest round %d (first was %d)", drained, highest, targetRound)
				}

				// Call SetSyncRound with the highest round (this is the only place we call it in lead-sync mode)
				li.logger.Infof("Calling SetSyncRound(%d) to sync follower to lead", highest)
				_, err := li.followerClient.SetSyncRound(highest).Do(li.ctx)
				if err != nil {
					li.logger.Errorf("SetSyncRound(%d) failed: %v (will retry on next signal)", highest, err)
					continue
				}

				// Block until follower reaches the target round
				li.logger.Debugf("Waiting for follower to reach round %d", highest)
				nodeRound, err := waitForRoundWithTimeout(li.ctx, li.logger, li.followerClient, highest, li.waitForRoundTimeout)
				if err != nil {
					li.logger.Errorf("Wait for follower to reach round %d failed: %v (sync handler ready for next signal)", highest, err)
					continue
				}

				li.logger.Infof("Follower reached round %d (node reports: %d)", highest, nodeRound)

			case <-li.ctx.Done():
				li.logger.Info("Follower sync handler goroutine stopped gracefully (context cancelled)")
				return
			}
		}
	}()
}
