package importer

import (
	"time"
)

// startLeadNodePolling starts a background goroutine that polls the lead node for status updates
func (li *localnetImporter) startLeadNodePolling() {
	li.pollingWg.Add(1)
	go func() {
		defer li.pollingWg.Done()

		// Panic recovery to ensure goroutine never crashes unexpectedly
		defer func() {
			if r := recover(); r != nil {
				li.logger.Errorf("PANIC in lead node polling goroutine (recovered): %v", r)
				li.logger.Error("Lead node polling goroutine terminated due to panic - this should not happen")
			}
		}()

		ticker := time.NewTicker(li.cfg.LeadNodePollInterval)
		defer ticker.Stop()

		previousRound := uint64(0)
		li.logger.Info("Lead node polling goroutine started")

		for {
			select {
			case <-ticker.C:
				status, err := li.leadClient.Status().Do(li.pollingCtx)
				if err != nil {
					li.logger.Warnf("Failed to poll lead node status: %v", err)
					continue
				}

				newRound := status.LastRound

				// Log the status response details
				li.logger.Tracef("Lead node status response: LastRound=%d, LastVersion=%s, NextVersion=%s, NextVersionRound=%d, TimeSinceLastRound=%d",
					status.LastRound,
					status.LastVersion,
					status.NextVersion,
					status.NextVersionRound,
					status.TimeSinceLastRound)

				if newRound != previousRound {
					// Create new state with current round and timestamp
					newState := leadNodeState{
						Round:     newRound,
						Timestamp: time.Now().UTC(),
					}

					// Store atomically
					li.leadState.Store(newState)

					li.logger.Infof("Lead node advanced to round %d at %s (previous: %d, delta: +%d)",
						newRound,
						formatTimestamp(newState.Timestamp),
						previousRound,
						newRound-previousRound)

					previousRound = newRound

					// Signal any waiters that lead has advanced to this round (non-blocking)
					select {
					case li.syncSignal <- newRound:
						li.logger.Tracef("Sent sync signal for round %d", newRound)
					default:
						li.logger.Tracef("Sync signal channel full, waiters may be busy")
					}
				}

			case <-li.pollingCtx.Done():
				li.logger.Info("Lead node polling goroutine stopped gracefully (context cancelled)")
				return
			}
		}
	}()
}
