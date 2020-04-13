package dbconn

type QueueAttributes struct {
	Name                   string
	RawName                string
	DelaySeconds           int64
	VisibilityTimeout      int64
	MaximumMessageSize     int64
	MessageRetentionPeriod int64
	DeadLetterTarget       string
	MaxReceiveCount        int64
}

func (q QueueAttributes) HasDeadLetter() bool {
	return q.DeadLetterTarget != ""
}
