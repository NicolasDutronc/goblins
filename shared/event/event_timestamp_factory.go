package event

import (
	"time"
)

// Cassandra stores timestamp as an int64 containing the milliseconds.
// The timestamp is truncated to keep consistency between written and read data.
func NewWorkflowEventTimestamp() time.Time {
	return time.Now().Truncate(time.Millisecond)
}
