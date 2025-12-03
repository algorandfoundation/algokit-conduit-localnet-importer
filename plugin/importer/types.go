package importer

import "time"

// leadNodeState holds the lead node's current round and the timestamp when it was last updated
type leadNodeState struct {
	Round     uint64
	Timestamp time.Time
}

// formatTimestamp formats a timestamp in RFC3339 format for consistent logging
func formatTimestamp(t time.Time) string {
	return t.Format(time.RFC3339)
}
