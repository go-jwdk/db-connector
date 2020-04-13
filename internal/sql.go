package internal

type SQLTemplate interface {
	NewFindJobDML(queueRawName string, jobID string) (string, []interface{})
	NewFindJobsDML(queueRawName string, limit int64) (string, []interface{})
	NewHideJobDML(queueRawName string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (string, []interface{})
	NewEnqueueJobDML(queueRawName, jobID, args string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{})
	NewEnqueueJobWithTimeDML(queueRawName, jobID, args string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{})
	NewDeleteJobDML(queueRawName, jobID string) (string, []interface{})
	NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (string, []interface{})

	NewAddQueueAttributeDML(queueName, queueRawName string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{})
	NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queueRawName string) (string, []interface{})
	NewFindQueueAttributeDML(queueName string) (string, []interface{})

	NewCreateQueueAttributesDDL() string
	NewCreateQueueDDL(queueRawName string) string
}
