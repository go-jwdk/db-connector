package dbconnector

import (
	"context"

	"github.com/go-jwdk/db-connector/internal"
)

type repositoryMock struct {
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

func (m *repositoryMock) renew(querier internal.Querier) repository {
	return m
}

func (m *repositoryMock) enqueueJob(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error {
	if m.enqueueJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.enqueueJobFunc(ctx, queue, jobID, content, deduplicationID, groupID, delaySeconds)
}

func (m *repositoryMock) enqueueJobWithTime(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error {
	if m.enqueueJobWithTimeFunc == nil {
		panic("This method is not defined.")
	}
	return m.enqueueJobWithTimeFunc(ctx, queue, jobID, content, deduplicationID, groupID, enqueueAt)
}

func (m *repositoryMock) deleteJob(ctx context.Context, queue string, jobID string) error {
	if m.deleteJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.deleteJobFunc(ctx, queue, jobID)
}

func (m *repositoryMock) getJob(ctx context.Context, queue string, jobID string) (*internal.Job, error) {
	if m.getJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.getJobFunc(ctx, queue, jobID)
}

func (m *repositoryMock) getJobs(ctx context.Context, queue string, limit int64) ([]*internal.Job, error) {
	if m.getJobsFunc == nil {
		panic("This method is not defined.")
	}
	return m.getJobsFunc(ctx, queue, limit)
}

func (m *repositoryMock) grabJob(ctx context.Context, queue string, jobID string, currentRetryCount, currentInvisibleUntil, invisibleTime int64) (grabbed bool, err error) {
	if m.grabJobFunc == nil {
		panic("This method is not defined.")
	}
	return m.grabJobFunc(ctx, queue, jobID, currentRetryCount, currentInvisibleUntil, invisibleTime)
}

func (m *repositoryMock) updateJobVisibility(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error) {
	if m.updateJobVisibilityFunc == nil {
		panic("This method is not defined.")
	}
	return m.updateJobVisibilityFunc(ctx, queueRawName, jobID, visibilityTimeout)
}

func (m *repositoryMock) getQueueAttributes(ctx context.Context, queueName string) (*QueueAttributes, error) {
	if m.getQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.getQueueAttributesFunc(ctx, queueName)
}

func (m *repositoryMock) createQueueAttributes(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error {
	if m.createQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.createQueueAttributesFunc(ctx, queueName, queueRawName, visibilityTimeout, delaySeconds, maxReceiveCount, deadLetterTarget)
}

func (m *repositoryMock) updateQueueAttributes(ctx context.Context, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error) {
	if m.updateQueueAttributesFunc == nil {
		panic("This method is not defined.")
	}
	return m.updateQueueAttributesFunc(ctx, queueRawName, visibilityTimeout, delaySeconds, maxReceiveCount, deadLetterTarget)
}

func (m *repositoryMock) createQueueTable(ctx context.Context, queueRawName string) error {
	if m.createQueueTableFunc == nil {
		panic("This method is not defined.")
	}
	return m.createQueueTableFunc(ctx, queueRawName)
}

func (m *repositoryMock) createQueueAttributesTable(ctx context.Context) error {
	if m.createQueueAttributesTableFunc == nil {
		panic("This method is not defined.")
	}
	return m.createQueueAttributesTableFunc(ctx)
}
