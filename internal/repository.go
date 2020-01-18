package internal

import (
	"context"
	"database/sql"
)

type Querier interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

func NewRepository(querier Querier, tmpl SQLTemplate) *Repository {
	return &Repository{querier: querier, tmpl: tmpl}
}

type Repository struct {
	querier Querier
	tmpl    SQLTemplate
}

func (r *Repository) EnqueueJob(ctx context.Context, queue, jobID, class, args string, deduplicationID, groupID *string, delaySeconds int64) error {
	stmt, stmtArgs := r.tmpl.NewEnqueueJobDML(queue,
		jobID, class, args, deduplicationID, groupID, delaySeconds)
	_, err := r.querier.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) EnqueueJobWithTime(ctx context.Context, queue string, jobID, class, args string, deduplicationID, groupID *string, enqueueAt int64) error {
	stmt, stmtArgs := r.tmpl.NewEnqueueJobWithTimeDML(queue,
		jobID, class, args, deduplicationID, groupID, enqueueAt)
	_, err := r.querier.ExecContext(ctx, stmt, stmtArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) DeleteJob(ctx context.Context, queue string, jobID string) error {
	stmt, args := r.tmpl.NewDeleteJobDML(queue, jobID)
	_, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) FindJob(ctx context.Context, queue string, jobID string) (*Job, error) {
	stmt, args := r.tmpl.NewFindJobDML(queue, jobID)
	row := r.querier.QueryRowContext(ctx, stmt, args...)

	var job Job
	if err := row.Scan(
		&job.SecID,
		&job.JobID,
		&job.Class,
		&job.Args,
		&job.DeduplicationID,
		&job.GroupID,
		&job.InvisibleUntil,
		&job.RetryCount,
		&job.EnqueueAt,
	); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *Repository) FindJobs(ctx context.Context, queue string, limit int64) ([]*Job, error) {

	if limit == 0 {
		limit = 1
	}

	stmt, args := r.tmpl.NewFindJobsDML(queue, limit)
	rows, err := r.querier.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}

	var jobs []*Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(
			&job.SecID,
			&job.JobID,
			&job.Class,
			&job.Args,
			&job.DeduplicationID,
			&job.GroupID,
			&job.InvisibleUntil,
			&job.RetryCount,
			&job.EnqueueAt,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (r *Repository) GrabJob(ctx context.Context, queue string, job *Job, invisibleTime int64) (grabbed bool, err error) {
	stmt, args := r.tmpl.NewHideJobDML(queue, job.JobID, job.RetryCount, job.InvisibleUntil, invisibleTime)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (r *Repository) UpdateJobVisibility(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error) {
	stmt, args := r.tmpl.NewUpdateJobByVisibilityTimeoutDML(queueRawName, jobID, visibilityTimeout)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (r *Repository) FindQueueAttribute(ctx context.Context, queue string) (*QueueAttribute, error) {
	stmt, args := r.tmpl.NewFindQueueAttributeDML(queue)
	row := r.querier.QueryRowContext(ctx, stmt, args...)
	var q QueueAttribute
	err := row.Scan(
		&q.Name,
		&q.RawName,
		&q.DelaySeconds,
		&q.VisibilityTimeout,
		&q.MaximumMessageSize,
		&q.MessageRetentionPeriod,
		&q.DeadLetterTarget,
		&q.MaxReceiveCount,
	)

	if err != nil {
		return nil, err
	}

	return &q, nil
}

func (r *Repository) CreateQueueAttribute(ctx context.Context, queueName, queueRawName string, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, maxReceiveCount int64, deadLetterTarget string) error {
	stmt, args := r.tmpl.NewAddQueueAttributeDML(queueName, queueRawName, delaySeconds, maximumMessageSize, messageRetentionPeriod, deadLetterTarget, maxReceiveCount, visibilityTimeout)
	_, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) UpdateQueueAttribute(ctx context.Context, queueRawName string, visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod, maxReceiveCount *int64, deadLetterTarget *string) (updated bool, err error) {
	stmt, args := r.tmpl.NewUpdateQueueAttributeDML(
		visibilityTimeout,
		delaySeconds,
		maximumMessageSize,
		messageRetentionPeriod,
		deadLetterTarget,
		maxReceiveCount,
		queueRawName)
	result, err := r.querier.ExecContext(ctx, stmt, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (r *Repository) DefineQueue(ctx context.Context, queueRawName string) error {
	stmt := r.tmpl.NewCreateQueueDDL(queueRawName)
	_, err := r.querier.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) DefineQueueAttribute(ctx context.Context) error {
	stmt := r.tmpl.NewCreateQueueAttributeDDL()
	_, err := r.querier.ExecContext(ctx, stmt)
	if err != nil {
		return err
	}
	return nil
}
