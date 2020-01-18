package internal

import "time"

type QueueAttribute struct {
	Name                   string
	RawName                string
	DelaySeconds           int64
	VisibilityTimeout      int64
	MaximumMessageSize     int64
	MessageRetentionPeriod int64
	DeadLetterTarget       string
	MaxReceiveCount        int64

	CachePeriod time.Time
}

func (q QueueAttribute) HasDeadLetter() bool {
	return q.DeadLetterTarget != ""
}
