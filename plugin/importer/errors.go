package importer

import "fmt"

// SyncError is used to indicate algod and conduit are not synchronized
type SyncError struct {
	// retrievedRound is the round returned from an algod status call
	retrievedRound uint64

	// expectedRound is the round conduit expected to have gotten back
	expectedRound uint64

	// err is the error that was received from the endpoint caller
	err error
}

// NewSyncError creates a new SyncError
func NewSyncError(retrievedRound, expectedRound uint64, err error) *SyncError {
	return &SyncError{
		retrievedRound: retrievedRound,
		expectedRound:  expectedRound,
		err:            err,
	}
}

func (e *SyncError) Error() string {
	return fmt.Sprintf("wrong round returned from status for round: retrieved(%d) != expected(%d): %v", e.retrievedRound, e.expectedRound, e.err)
}

func (e *SyncError) Unwrap() error {
	return e.err
}
