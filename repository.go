package dbconnector

import (
	"context"

	"github.com/go-jwdk/db-connector/internal"
)

type repository interface {
	renew(querier internal.Querier) repository
	enqueueJob(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error
	enqueueJobWithTime(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error
	deleteJob(ctx context.Context, queue string, jobID string) error
	getJob(ctx context.Context, queue string, jobID string) (*internal.Job, error)
	getJobs(ctx context.Context, queue string, limit int64) ([]*internal.Job, error)
	grabJob(ctx context.Context,
		queue string, jobID string, currentReceiveCount, currentInvisibleUntil, invisibleTime int64) (*internal.Job, error)
	updateJobVisibility(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error)
	getQueueAttributes(ctx context.Context, queueName string) (*QueueAttributes, error)
	createQueueAttributes(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error
	updateQueueAttributes(ctx context.Context, queueRawName string,
		visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error)
	createQueueTable(ctx context.Context, queueRawName string) error
	createQueueAttributesTable(ctx context.Context) error
}

type repositoryOnDB struct {
	querier internal.Querier
	tmpl    SQLTemplate
}

func (r *repositoryOnDB) renew(querier internal.Querier) repository {
	return &repositoryOnDB{
		querier: querier,
		tmpl:    r.tmpl,
	}
}

func (r *repositoryOnDB) enqueueJob(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error {
	stmt, stmtArgs := r.tmpl.NewEnqueueJobDML(queue,
		jobID, content, deduplicationID, groupID, delaySeconds)
	_, err := r.querier.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (r *repositoryOnDB) enqueueJobWithTime(ctx context.Context, queue string, jobID, content string, deduplicationID, groupID *string, enqueueAt int64) error {
	stmt, stmtArgs := r.tmpl.NewEnqueueJobWithTimeDML(queue,
		jobID, content, deduplicationID, groupID, enqueueAt)
	_, err := r.querier.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (r *repositoryOnDB) deleteJob(ctx context.Context, queue string, jobID string) error {
	stmt, args := r.tmpl.NewDeleteJobDML(queue, jobID)
	_, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *repositoryOnDB) getJob(ctx context.Context, queue string, jobID string) (*internal.Job, error) {
	stmt, args := r.tmpl.NewFindJobDML(queue, jobID)
	row := r.querier.QueryRowContext(ctx, stmt, args...)

	var job internal.Job
	if err := row.Scan(
		&job.SecID,
		&job.JobID,
		&job.Content,
		&job.DeduplicationID,
		&job.GroupID,
		&job.InvisibleUntil,
		&job.ReceiveCount,
		&job.EnqueueAt,
	); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *repositoryOnDB) getJobs(ctx context.Context, queue string, limit int64) ([]*internal.Job, error) {

	if limit == 0 {
		limit = 1
	}

	stmt, args := r.tmpl.NewFindJobsDML(queue, limit)
	rows, err := r.querier.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	var jobs []*internal.Job
	for rows.Next() {
		var job internal.Job
		if err := rows.Scan(
			&job.SecID,
			&job.JobID,
			&job.Content,
			&job.DeduplicationID,
			&job.GroupID,
			&job.InvisibleUntil,
			&job.ReceiveCount,
			&job.EnqueueAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (r *repositoryOnDB) grabJob(ctx context.Context,
	queue string, jobID string, currentReceiveCount, currentInvisibleUntil, invisibleTime int64) (*internal.Job, error) {
	stmt, args := r.tmpl.NewHideJobDML(queue, jobID, currentReceiveCount, currentInvisibleUntil, invisibleTime)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return nil, nil
	}
	return r.getJob(ctx, queue, jobID)
}

func (r *repositoryOnDB) updateJobVisibility(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error) {
	stmt, args := r.tmpl.NewUpdateJobByVisibilityTimeoutDML(queueRawName, jobID, visibilityTimeout)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (r *repositoryOnDB) getQueueAttributes(ctx context.Context, queueName string) (*QueueAttributes, error) {
	stmt, args := r.tmpl.NewFindQueueAttributesDML(queueName)
	row := r.querier.QueryRowContext(ctx, stmt, args...)
	var q QueueAttributes
	err := row.Scan(
		&q.Name,
		&q.RawName,
		&q.VisibilityTimeout,
		&q.DelaySeconds,
		&q.MaxReceiveCount,
		&q.DeadLetterTarget,
	)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

func (r *repositoryOnDB) createQueueAttributes(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maxReceiveCount int64, deadLetterTarget *string) error {
	stmt, args := r.tmpl.NewAddQueueAttributesDML(queueName, queueRawName, delaySeconds, maxReceiveCount, visibilityTimeout, deadLetterTarget)
	_, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *repositoryOnDB) updateQueueAttributes(ctx context.Context, queueRawName string,
	visibilityTimeout, delaySeconds, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error) {
	stmt, args := r.tmpl.NewUpdateQueueAttributesDML(
		queueRawName,
		visibilityTimeout,
		delaySeconds,
		maxReceiveCount,
		deadLetterTarget)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (r *repositoryOnDB) createQueueTable(ctx context.Context, queueRawName string) error {
	stmt := r.tmpl.NewCreateQueueDDL(queueRawName)
	_, err := r.querier.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}
	return nil
}

func (r *repositoryOnDB) createQueueAttributesTable(ctx context.Context) error {
	stmt := r.tmpl.NewCreateQueueAttributesDDL()
	_, err := r.querier.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}
	return nil
}
