package internal

type SQLTemplate interface {
	NewFindJobDML(queueRawName string, jobID string) (string, []interface{})
	NewFindJobsDML(queueRawName string, limit int64) (string, []interface{})
	NewHideJobDML(queueRawName string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (string, []interface{})
	NewEnqueueJobDML(queueRawName, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{})
	NewEnqueueJobWithTimeDML(queueRawName, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{})
	NewDeleteJobDML(queueRawName, jobID string) (string, []interface{})
	NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (string, []interface{})

	NewAddQueueAttributesDML(queueName, queueRawName string, delaySeconds, maxReceiveCount, visibilityTimeout int64, deadLetterTarget *string) (stmt string, args []interface{})
	NewUpdateQueueAttributesDML(queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (stmt string, args []interface{})
	NewFindQueueAttributesDML(queueName string) (string, []interface{})

	NewCreateQueueAttributesDDL() string
	NewCreateQueueDDL(queueRawName string) string
}
