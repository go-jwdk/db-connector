package dbconnector

type QueueAttributes struct {
	Name              string
	RawName           string
	DelaySeconds      int64
	VisibilityTimeout int64
	MaxReceiveCount   int64 // If the value is zero, retry infinitely
	DeadLetterTarget  *string
}

func (q QueueAttributes) HasDeadLetter() (string, bool) {
	ok := q.DeadLetterTarget != nil && *q.DeadLetterTarget != ""
	if ok {
		return *q.DeadLetterTarget, ok
	}
	return "", ok
}
