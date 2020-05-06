// +build mock

package dbconnector

import (
	"context"

	"github.com/go-jwdk/db-connector/internal"
)

type repositoryFactory struct {
	enqueueJobFunc                 func(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error
	enqueueJobWithTimeFunc         func(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error
	deleteJobFunc                  func(ctx context.Context, queue string, jobID string) error
	getJobFunc                     func(ctx context.Context, queue string, jobID string) (*internal.Job, error)
	getJobsFunc                    func(ctx context.Context, queue string, limit int64) ([]*internal.Job, error)
	grabJobFunc                    func(ctx context.Context, queue string, jobID string, currentRetryCount, currentInvisibleUntil, invisibleTime int64) (grabbed bool, err error)
	updateJobVisibilityFunc        func(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error)
	getQueueAttributesFunc         func(ctx context.Context, queueName string) (*QueueAttributes, error)
	createQueueAttributesFunc      func(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error
	updateQueueAttributesFunc      func(ctx context.Context, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error)
	createQueueTableFunc           func(ctx context.Context, queueRawName string) error
	createQueueAttributesTableFunc func(ctx context.Context) error
}

func (rf *repositoryFactory) new(querier internal.Querier) *repository {
	return &repository{
		EnqueueJobFunc:                 rf.EnqueueJobFunc,
		EnqueueJobWithTimeFunc:         rf.EnqueueJobWithTimeFunc,
		DeleteJobFunc:                  rf.DeleteJobFunc,
		GetJobFunc:                     rf.GetJobFunc,
		GetJobsFunc:                    rf.GetJobsFunc,
		GrabJobFunc:                    rf.GrabJobFunc,
		UpdateJobVisibilityFunc:        rf.UpdateJobVisibilityFunc,
		GetQueueAttributesFunc:         rf.GetQueueAttributesFunc,
		CreateQueueAttributesFunc:      rf.CreateQueueAttributesFunc,
		UpdateQueueAttributesFunc:      rf.UpdateQueueAttributesFunc,
		CreateQueueTableFunc:           rf.CreateQueueTableFunc,
		CreateQueueAttributesTableFunc: rf.CreateQueueAttributesTableFunc,
	}
}

type repository struct {
	enqueueJobFunc                 func(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error
	enqueueJobWithTimeFunc         func(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error
	deleteJobFunc                  func(ctx context.Context, queue string, jobID string) error
	getJobFunc                     func(ctx context.Context, queue string, jobID string) (*internal.Job, error)
	getJobsFunc                    func(ctx context.Context, queue string, limit int64) ([]*internal.Job, error)
	grabJobFunc                    func(ctx context.Context, queue string, jobID string, currentRetryCount, currentInvisibleUntil, invisibleTime int64) (grabbed bool, err error)
	updateJobVisibilityFunc        func(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error)
	getQueueAttributesFunc         func(ctx context.Context, queueName string) (*QueueAttributes, error)
	createQueueAttributesFunc      func(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error
	updateQueueAttributesFunc      func(ctx context.Context, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error)
	createQueueTableFunc           func(ctx context.Context, queueRawName string) error
	createQueueAttributesTableFunc func(ctx context.Context) error
}

func (m *repository) enqueueJob(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error {
	if m.EnqueueJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.EnqueueJobFunc(ctx, queue, jobID, content, deduplicationID, groupID, delaySeconds)
}

func (m *repository) enqueueJobWithTime(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error {
	if m.EnqueueJobWithTimeFunc == nil {
		panic("This method is not defined.")
	}
	return m.EnqueueJobWithTimeFunc(ctx, queue, jobID, content, deduplicationID, groupID, enqueueAt)
}

func (m *repository) deleteJob(ctx context.Context, queue string, jobID string) error {
	if m.DeleteJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.DeleteJobFunc(ctx, queue, jobID)
}

func (m *repository) getJob(ctx context.Context, queue string, jobID string) (*internal.Job, error) {
	if m.GetJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.GetJobFunc(ctx, queue, jobID)
}

func (m *repository) getJobs(ctx context.Context, queue string, limit int64) ([]*internal.Job, error) {
	if m.GetJobsFunc == nil {
		panic("This method is not defined.")
	}
	return m.GetJobsFunc(ctx, queue, limit)
}

func (m *repository) grabJob(ctx context.Context, queue string, jobID string, currentRetryCount, currentInvisibleUntil, invisibleTime int64) (grabbed bool, err error) {
	if m.GrabJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.GrabJobFunc(ctx, queue, jobID, currentRetryCount, currentInvisibleUntil, invisibleTime)
}

func (m *repository) updateJobVisibility(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error) {
	if m.UpdateJobVisibilityFunc == nil {
		panic("This method is not defined.")
	}
	return m.UpdateJobVisibilityFunc(ctx, queueRawName, jobID, visibilityTimeout)
}

func (m *repository) getQueueAttributes(ctx context.Context, queueName string) (*QueueAttributes, error) {
	if m.GetQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.GetQueueAttributesFunc(ctx, queueName)
}

func (m *repository) createQueueAttributes(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error {
	if m.CreateQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.CreateQueueAttributesFunc(ctx, queueName, queueRawName, visibilityTimeout, delaySeconds, maxReceiveCount, deadLetterTarget)
}

func (m *repository) updateQueueAttributes(ctx context.Context, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error) {
	if m.UpdateQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.UpdateQueueAttributesFunc(ctx, queueRawName, visibilityTimeout, delaySeconds, maxReceiveCount, deadLetterTarget)
}

func (m *repository) createQueueTable(ctx context.Context, queueRawName string) error {
	if m.CreateQueueTableFunc == nil {
		panic("This method is not defined.")
	}
	return m.CreateQueueTableFunc(ctx, queueRawName)
}

func (m *repository) createQueueAttributesTable(ctx context.Context) error {
	if m.CreateQueueAttributesTableFunc == nil {
		panic("This method is not defined.")
	}
	return m.CreateQueueAttributesTableFunc(ctx)
}
