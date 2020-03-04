package internal

import (
	"strconv"

	"github.com/go-jwdk/jobworker"
)

type Job struct {
	SecID           int64
	JobID           string
	Args            string
	Class           *string
	DeduplicationID *string
	GroupID         *string
	InvisibleUntil  int64
	RetryCount      int64
	EnqueueAt       int64
}

func newJob(queueName string, job *Job, conn jobworker.Connector) *jobworker.Job {

	metadata := make(map[string]string)
	metadata["SecID"] = strconv.FormatInt(job.SecID, 10)
	metadata["JobID"] = job.JobID
	if job.DeduplicationID != nil {
		metadata["DeduplicationID"] = *job.DeduplicationID
	}
	if job.GroupID != nil {
		metadata["GroupID"] = *job.GroupID
	}
	metadata["InvisibleUntil"] = strconv.FormatInt(job.InvisibleUntil, 10)
	metadata["RetryCount"] = strconv.FormatInt(job.RetryCount, 10)
	metadata["EnqueueAt"] = strconv.FormatInt(job.EnqueueAt, 10)

	return &jobworker.Job{
		Conn:conn,
		QueueName:queueName,
		Content:         job.Args,
		Metadata:        metadata,
		CustomAttribute: make(map[string]*jobworker.CustomAttribute),
		Raw:             job,
	}
}
