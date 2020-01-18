package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-job-worker-development-kit/jobworker"
	"github.com/vvatanabe/goretryer/exponential"

	uuid "github.com/satori/go.uuid"
)

const (
	PackageName = "jwdk"
)

const (
	ConnAttributeNameDSN             = "DSN"
	ConnAttributeNameMaxOpenConns    = "MaxOpenConns"
	ConnAttributeNameMaxIdleConns    = "MaxMaxIdleConns"
	ConnAttributeNameConnMaxLifetime = "ConnMaxLifetime"

	ConnAttributeNameNumMaxRetries = "NumMaxRetries"

	queueAttributeNameVisibilityTimeout      = "JobVisibilityTimeout"
	queueAttributeNameDelaySeconds           = "DelaySeconds"
	queueAttributeNameMaximumMessageSize     = "MaximumMessageSize"
	queueAttributeNameMessageRetentionPeriod = "MessageRetentionPeriod"
	queueAttributeNameDeadLetterTarget       = "DeadLetterTarget"
	queueAttributeNameMaxReceiveCount        = "MaxReceiveCount"

	queueAttributeValueVisibilityTimeoutDefault      = int64(30)
	queueAttributeValueDelaySecondsDefault           = int64(0)
	queueAttributeValueMaximumMessageSizeDefault     = int64(0)
	queueAttributeValueMessageRetentionPeriodDefault = int64(0)
	queueAttributeValueMaxReceiveCountDefault        = int64(0)
	queueAttributeValueDeadLetterTargetDefault       = ""
)

type Values struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime *time.Duration
	NumMaxRetries   *int
}

func (v *Values) ApplyDefaultValues() {
	if v.NumMaxRetries == nil {
		i := 3
		v.NumMaxRetries = &i
	}
}

func ConnAttrsToValues(attrs map[string]interface{}) *Values {
	var values Values
	for k, v := range attrs {
		switch k {
		case ConnAttributeNameDSN:
			s := v.(string)
			values.DSN = s
		case ConnAttributeNameMaxOpenConns:
			s := v.(int)
			values.MaxOpenConns = s
		case ConnAttributeNameMaxIdleConns:
			s := v.(int)
			values.MaxIdleConns = s
		case ConnAttributeNameConnMaxLifetime:
			s := v.(time.Duration)
			values.ConnMaxLifetime = &s
		case ConnAttributeNameNumMaxRetries:
			s := v.(int)
			values.NumMaxRetries = &s
		}
	}
	return &values
}

type SQLTemplate interface {
	NewFindJobDML(queueRawName string, jobID string) (string, []interface{})
	NewFindJobsDML(queueRawName string, limit int64) (string, []interface{})
	NewHideJobDML(queueRawName string, jobID string, oldRetryCount, oldInvisibleUntil, invisibleTime int64) (string, []interface{})
	NewEnqueueJobDML(queueRawName, jobID, class, args string, deduplicationID, groupID *string, delaySeconds int64) (string, []interface{})
	NewEnqueueJobWithTimeDML(queueRawName, jobID, class, args string, deduplicationID, groupID *string, enqueueAt int64) (string, []interface{})
	NewDeleteJobDML(queueRawName, jobID string) (string, []interface{})
	NewUpdateJobByVisibilityTimeoutDML(queueRawName string, jobID string, visibilityTimeout int64) (string, []interface{})

	NewAddQueueAttributeDML(queueName, queueRawName string, delaySeconds, maximumMessageSize, messageRetentionPeriod int64, deadLetterTarget string, maxReceiveCount, visibilityTimeout int64) (string, []interface{})
	NewUpdateQueueAttributeDML(visibilityTimeout, delaySeconds, maximumMessageSize, messageRetentionPeriod *int64, deadLetterTarget *string, maxReceiveCount *int64, queueRawName string) (string, []interface{})
	NewFindQueueAttributeDML(queueName string) (string, []interface{})

	NewCreateQueueAttributeDDL() string
	NewCreateQueueDDL(queueRawName string) string
}

type Connector struct {
	Name               string
	DB                 *sql.DB
	SQLTemplate        SQLTemplate
	IsUniqueViolation  func(err error) bool
	IsDeadlockDetected func(err error) bool

	name2QueueDef       sync.Map
	queueDefCachePeriod int64

	logger jobworker.Logger

	Retryer exponential.Retryer
}

func (c *Connector) GetName() string {
	return c.Name
}

var (
	ErrNoFoundQueue = fmt.Errorf("no found queue")
)

func (c *Connector) ReceiveJobs(ctx context.Context, ch chan<- *jobworker.Job, input *jobworker.ReceiveJobsInput, opts ...func(*jobworker.Option)) (*jobworker.ReceiveJobsOutput, error) {

	var jobs []*jobworker.Job
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {

		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			return err
		}

		grabbedJobs, err := c.grabJobs(ctx, queue, queue.MaximumMessageSize, queue.VisibilityTimeout, func(deadJobs []*Job) error {
			if queue.HasDeadLetter() {
				deadLetterQueue, err := c.resolveQueue(ctx, queue.DeadLetterTarget)
				if err != nil {
					return err
				}
				err = c.moveJobBatch(ctx, queue, deadLetterQueue, deadJobs)
				if err != nil {
					c.logger.Debug("could not clean job batch:", err)
				}
			} else {
				err := c.cleanJobBatch(ctx, queue, deadJobs)
				if err != nil {
					c.logger.Debug("could not clean job batch:", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		jobs = grabbedJobs

		return nil
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	if err != nil {
		return nil, err
	}

	for _, job := range jobs {
		ch <- job
	}

	return &jobworker.ReceiveJobsOutput{
		NoJob: len(jobs) == 0,
	}, nil
}

func (c *Connector) resolveQueue(ctx context.Context, queueName string) (*QueueAttribute, error) {

	v, ok := c.name2QueueDef.Load(queueName)
	if !ok || v.(*QueueAttribute).CachePeriod.Before(time.Now()) {

		repo := NewRepository(c.DB, c.SQLTemplate)

		q, err := repo.FindQueueAttribute(ctx, queueName)
		if isErrNoRows(err) {
			return nil, ErrNoFoundQueue
		}

		if err != nil {
			return nil, err
		}

		q.CachePeriod = time.Now().Add(time.Duration(c.queueDefCachePeriod) * time.Second)

		v = q
		c.name2QueueDef.Store(queueName, v)
	}
	return v.(*QueueAttribute), nil
}

func (c *Connector) cleanJobBatch(ctx context.Context, queue *QueueAttribute, jobs []*Job) error {
	return withTransaction(c.DB, func(tx *sql.Tx) error {
		repo := NewRepository(tx, c.SQLTemplate)
		for _, job := range jobs {
			err := repo.DeleteJob(ctx, queue.RawName, job.JobID)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Connector) moveJobBatch(ctx context.Context, from, to *QueueAttribute, jobs []*Job) error {
	return withTransaction(c.DB, func(tx *sql.Tx) error {
		repo := NewRepository(tx, c.SQLTemplate)
		for _, job := range jobs {
			err := repo.EnqueueJobWithTime(ctx,
				to.RawName,
				job.JobID,
				job.Class,
				job.Args,
				job.DeduplicationID,
				job.GroupID,
				job.EnqueueAt,
			)
			if err != nil {
				if c.IsUniqueViolation(err) {
					continue
				}
				return err
			}

			err = repo.DeleteJob(ctx, from.RawName, job.JobID)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Connector) grabJobs(ctx context.Context, queue *QueueAttribute, maxNumberOfJobs, invisibleTime int64, handleDeadJob func(deadJobs []*Job) error) ([]*jobworker.Job, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	jobs, err := repo.FindJobs(ctx, queue.RawName, maxNumberOfJobs)
	if err != nil {
		return nil, err
	}

	var (
		deadJobs  []*Job
		aliveJobs []*Job
	)

	for _, job := range jobs {
		if job.RetryCount >= queue.MaxReceiveCount {
			deadJobs = append(deadJobs, job)
		} else {
			aliveJobs = append(aliveJobs, job)
		}
	}

	if len(deadJobs) > 0 {
		err := handleDeadJob(deadJobs)
		if err != nil {
			return nil, err
		}
	}

	shuffled := map[*Job]struct{}{}
	for _, j := range jobs {
		shuffled[j] = struct{}{}
	}

	var deliveries []*jobworker.Job
	for job := range shuffled {

		grabbed, err := repo.GrabJob(ctx, queue.RawName, job, invisibleTime)
		if err != nil || !grabbed {
			continue
		}

		deliveries = append(deliveries, job.ToJob(queue.Name, c, c.logger))
	}
	return deliveries, nil
}

func (c *Connector) EnqueueJob(ctx context.Context, input *jobworker.EnqueueJobInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobOutput, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	var jobID string
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {

		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			return err
		}

		jobID = newJobID()

		var deduplicationID *string
		if input.Payload.DeduplicationID != "" {
			deduplicationID = &input.Payload.DeduplicationID
		}
		var groupID *string
		if input.Payload.GroupID != "" {
			groupID = &input.Payload.GroupID
		}

		return repo.EnqueueJob(ctx,
			queue.RawName,
			jobID,
			input.Payload.Class,
			input.Payload.Args,
			deduplicationID,
			groupID,
			input.Payload.DelaySeconds,
		)

	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	if err != nil {
		if c.IsUniqueViolation(err) {
			return nil, jobworker.ErrJobDuplicationDetected
		}
		return nil, err
	}

	return &jobworker.EnqueueJobOutput{JobID: jobID}, nil
}

func (c *Connector) EnqueueJobBatch(ctx context.Context, input *jobworker.EnqueueJobBatchInput, opts ...func(*jobworker.Option)) (*jobworker.EnqueueJobBatchOutput, error) {

	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		queue, err := c.resolveQueue(ctx, input.Queue)
		if err != nil {
			return err
		}
		return c.enqueueJobBatch(ctx, queue, input.Id2Payload)

	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	if err != nil {
		return nil, err
	}

	var ids []string
	for id := range input.Id2Payload {
		ids = append(ids, id)
	}

	return &jobworker.EnqueueJobBatchOutput{
		Successful: ids,
	}, nil
}

func (c *Connector) enqueueJobBatch(ctx context.Context, queue *QueueAttribute, id2Payload map[string]*jobworker.Payload) error {
	return withTransaction(c.DB, func(tx *sql.Tx) error {
		repo := NewRepository(tx, c.SQLTemplate)
		for _, v := range id2Payload {
			jobID := newJobID()
			var deduplicationID *string
			if v.DeduplicationID != "" {
				deduplicationID = &v.DeduplicationID
			}
			var groupID *string
			if v.GroupID != "" {
				groupID = &v.GroupID
			}
			err := repo.EnqueueJob(ctx,
				queue.RawName,
				jobID,
				v.Class,
				v.Args,
				deduplicationID,
				groupID,
				v.DelaySeconds,
			)
			if err != nil {
				if c.IsUniqueViolation(err) {
					continue
				}
				return err
			}

		}
		return nil
	})
}

func newJobID() string {
	return uuid.NewV4().String()
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput, opts ...func(*jobworker.Option)) (*jobworker.CompleteJobOutput, error) {
	repo := NewRepository(c.DB, c.SQLTemplate)
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		queue, err := c.resolveQueue(ctx, input.Job.Queue)
		if err != nil {
			return err
		}
		err = repo.DeleteJob(ctx, queue.RawName, input.Job.JobID)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput, opts ...func(*jobworker.Option)) (*jobworker.FailJobOutput, error) {
	_, err := c.ChangeJobVisibility(ctx, &jobworker.ChangeJobVisibilityInput{
		Job:               input.Job,
		VisibilityTimeout: 1,
	})
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) ChangeJobVisibility(ctx context.Context, input *jobworker.ChangeJobVisibilityInput, opts ...func(*jobworker.Option)) (*jobworker.ChangeJobVisibilityOutput, error) {
	repo := NewRepository(c.DB, c.SQLTemplate)
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		queue, err := c.resolveQueue(ctx, input.Job.Queue)
		if err != nil {
			return err
		}
		_, err = repo.UpdateJobVisibility(ctx, queue.RawName, input.Job.JobID, input.VisibilityTimeout)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &jobworker.ChangeJobVisibilityOutput{}, nil
}

func (c *Connector) RedriveJob(ctx context.Context, input *jobworker.RedriveJobInput, opts ...func(*jobworker.Option)) (*jobworker.RedriveJobOutput, error) {
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		from, err := c.resolveQueue(ctx, input.From)
		if err != nil {
			return err
		}
		to, err := c.resolveQueue(ctx, input.To)
		if err != nil {
			return err
		}
		return c.redriveJobBatch(ctx, from, to, input.Target, input.DelaySeconds)
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	return &jobworker.RedriveJobOutput{}, err
}

func (c *Connector) redriveJobBatch(ctx context.Context, from, to *QueueAttribute, jobID string, delaySeconds int64) error {
	repo := NewRepository(c.DB, c.SQLTemplate)
	job, err := repo.FindJob(ctx, from.RawName, jobID)
	if err != nil {
		if isErrNoRows(err) {
			// TODO logging
			return nil
		}
		return err
	}
	return withTransaction(c.DB, func(tx *sql.Tx) error {
		repo := NewRepository(tx, c.SQLTemplate)

		err = repo.EnqueueJob(ctx,
			to.RawName,
			job.JobID,
			job.Class,
			job.Args,
			job.DeduplicationID,
			job.GroupID,
			delaySeconds,
		)
		if err != nil {
			return err
		}

		err = repo.DeleteJob(ctx, from.RawName, job.JobID)
		if err != nil {
			return err
		}

		return nil
	})
}

type values struct {
	visibilityTimeout,
	delaySeconds,
	maximumMessageSize,
	messageRetentionPeriod,
	maxReceiveCount *int64
	deadLetterTarget *string
}

func (v *values) applyDefaultValues() {
	if v.visibilityTimeout == nil {
		i := queueAttributeValueVisibilityTimeoutDefault
		v.visibilityTimeout = &i
	}
	if v.delaySeconds == nil {
		i := queueAttributeValueDelaySecondsDefault
		v.delaySeconds = &i
	}
	if v.maximumMessageSize == nil {
		i := queueAttributeValueMaximumMessageSizeDefault
		v.maximumMessageSize = &i
	}
	if v.messageRetentionPeriod == nil {
		i := queueAttributeValueMessageRetentionPeriodDefault
		v.messageRetentionPeriod = &i
	}
	if v.maxReceiveCount == nil {
		i := queueAttributeValueMaxReceiveCountDefault
		v.maxReceiveCount = &i
	}
	if v.deadLetterTarget == nil {
		i := queueAttributeValueDeadLetterTargetDefault
		v.deadLetterTarget = &i
	}
}

func queueAttrsToValues(attrs map[string]interface{}) *values {
	var values values
	for k, v := range attrs {
		switch k {
		case queueAttributeNameVisibilityTimeout:
			i := v.(int64)
			values.visibilityTimeout = &i
		case queueAttributeNameDelaySeconds:
			i := v.(int64)
			values.delaySeconds = &i
		case queueAttributeNameMaximumMessageSize:
			i := v.(int64)
			values.maximumMessageSize = &i
		case queueAttributeNameMessageRetentionPeriod:
			i := v.(int64)
			values.messageRetentionPeriod = &i
		case queueAttributeNameDeadLetterTarget:
			s := v.(string)
			values.deadLetterTarget = &s
		case queueAttributeNameMaxReceiveCount:
			i := v.(int64)
			values.maxReceiveCount = &i
		}
	}
	return &values
}

func newQueueRawName(name string) string {
	rawName := strings.Replace(name, ".", "_", -1)
	return fmt.Sprintf("%s_%s", PackageName, rawName)
}

func (c *Connector) CreateQueue(ctx context.Context, input *jobworker.CreateQueueInput, opts ...func(*jobworker.Option)) (*jobworker.CreateQueueOutput, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	v := queueAttrsToValues(input.Attributes)
	v.applyDefaultValues()

	queueRawName := newQueueRawName(input.Name)

	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		err := repo.DefineQueueAttribute(ctx)
		if err != nil {
			return err
		}
		err = repo.DefineQueue(ctx, queueRawName)
		if err != nil {
			return err
		}
		err = repo.CreateQueueAttribute(ctx, input.Name, queueRawName, *v.visibilityTimeout, *v.delaySeconds, *v.maximumMessageSize, *v.messageRetentionPeriod, *v.maxReceiveCount, *v.deadLetterTarget)
		if err != nil {
			if !c.IsUniqueViolation(err) {
				return err
			}
		}
		return nil
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	if err != nil {
		return nil, err
	}

	return &jobworker.CreateQueueOutput{}, nil

}

func (c *Connector) UpdateQueue(ctx context.Context, input *jobworker.UpdateQueueInput, opts ...func(*jobworker.Option)) (*jobworker.UpdateQueueOutput, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	v := queueAttrsToValues(input.Attributes)

	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		queue, err := c.resolveQueue(ctx, input.Name)
		if err != nil {
			return err
		}
		_, err = repo.UpdateQueueAttribute(
			ctx,
			queue.RawName,
			v.visibilityTimeout,
			v.delaySeconds,
			v.maximumMessageSize,
			v.messageRetentionPeriod,
			v.maxReceiveCount,
			v.deadLetterTarget,
		)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})

	if err != nil {
		return nil, err
	}

	return &jobworker.UpdateQueueOutput{}, nil
}

func (c *Connector) Close() error {
	return c.DB.Close()
}

func (c *Connector) SetLogger(logger jobworker.Logger) {
	c.logger = logger
}

func withTransaction(db *sql.DB, ope func(tx *sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	err = ope(tx)
	return
}

func isErrNoRows(err error) bool {
	return err == sql.ErrNoRows
}
