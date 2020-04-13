package dbconn

type QueueAttributes struct {
	Name              string
	RawName           string
	DelaySeconds      int64
	VisibilityTimeout int64
	DeadLetterTarget  *string
	MaxReceiveCount   int64 // If the value is zero, retry infinitely
}

func (q QueueAttributes) HasDeadLetter() (string, bool) {
	ok := q.DeadLetterTarget != nil && *q.DeadLetterTarget != ""
	if ok {
		return *q.DeadLetterTarget, ok
	}
	return "", ok
}
