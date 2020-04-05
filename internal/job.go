package internal

import (
	"strconv"

	"github.com/go-jwdk/jobworker"
)

func NewJob(queueName string, job *Job, conn jobworker.Connector) *jobworker.Job {
	return &jobworker.Job{
		Conn:            conn,
		QueueName:       queueName,
		Content:         job.Content,
		Metadata:        newMetadata(job),
		CustomAttribute: make(map[string]*jobworker.CustomAttribute),
		Raw:             job,
	}
}

type Job struct {
	SecID           int64
	JobID           string
	Content         string
	DeduplicationID *string
	GroupID         *string
	InvisibleUntil  int64
	RetryCount      int64
	EnqueueAt       int64
}

const (
	MetadataKeySecID           = "SecID"
	MetadataKeyJobID           = "JobID"
	MetadataKeyDeduplicationID = "DeduplicationID"
	MetadataKeyGroupID         = "GroupID"
	MetadataKeyInvisibleUntil  = "InvisibleUntil"
	MetadataKeyRetryCount      = "RetryCount"
	MetadataKeyEnqueueAt       = "EnqueueAt"

	MetadataKeyDelaySeconds = "DelaySeconds"
)

func newMetadata(job *Job) map[string]string {
	metadata := make(map[string]string)
	metadata[MetadataKeySecID] = strconv.FormatInt(job.SecID, 10)
	metadata[MetadataKeyJobID] = job.JobID
	if job.DeduplicationID != nil {
		metadata[MetadataKeyDeduplicationID] = *job.DeduplicationID
	}
	if job.GroupID != nil {
		metadata[MetadataKeyGroupID] = *job.GroupID
	}
	metadata[MetadataKeyInvisibleUntil] = strconv.FormatInt(job.InvisibleUntil, 10)
	metadata[MetadataKeyRetryCount] = strconv.FormatInt(job.RetryCount, 10)
	metadata[MetadataKeyEnqueueAt] = strconv.FormatInt(job.EnqueueAt, 10)
	return metadata
}
