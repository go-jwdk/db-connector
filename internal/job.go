package internal

import "github.com/go-job-worker-development-kit/jobworker"

type Job struct {
	SecID           int64
	JobID           string
	Class           string
	ReceiptID       string
	Args            string
	DeduplicationID *string
	GroupID         *string
	InvisibleUntil  int64
	RetryCount      int64
	EnqueueAt       int64
}

func (j *Job) ToJob(queue string, conn jobworker.Connector, logger jobworker.Logger) *jobworker.Job {

	var deduplicationID string
	if j.DeduplicationID != nil {
		deduplicationID = *j.DeduplicationID
	}
	var groupID string
	if j.GroupID != nil {
		groupID = *j.GroupID
	}

	job := jobworker.NewJob(
		queue,
		j.JobID,
		j.Class,
		j.ReceiptID,
		j.Args,
		deduplicationID,
		groupID,
		j.InvisibleUntil,
		j.RetryCount,
		j.EnqueueAt,
		conn,
	)
	return job
}
