package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vvatanabe/expiremap"

	"github.com/go-jwdk/jobworker"
	"github.com/vvatanabe/goretryer/exponential"

	uuid "github.com/satori/go.uuid"
)

const (
	TablePrefix = "jwdk"
	PackageName = ""
	LogPrefix   = "[" + TablePrefix + "]"

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

	defaultNumMaxRetries = 3
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
		i := defaultNumMaxRetries
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

type Connector struct {
	ConnName           string
	DB                 *sql.DB
	SQLTemplate        SQLTemplate
	IsUniqueViolation  func(err error) bool
	IsDeadlockDetected func(err error) bool

	name2Queue expiremap.Map

	loggerFunc jobworker.LoggerFunc

	Retryer exponential.Retryer
}

func (c *Connector) Name() string {
	return c.ConnName
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput) (*jobworker.SubscribeOutput, error) {
	var sub *Subscription
	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, fmt.Errorf("could not resolve queue: %w", err)
	}
	sub = NewSubscription(queue, c, input.Metadata)
	go sub.Start()
	return &jobworker.SubscribeOutput{
		Subscription: sub,
	}, nil
}

func (c *Connector) Enqueue(ctx context.Context, input *jobworker.EnqueueInput) (*jobworker.EnqueueOutput, error) {
	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, err
	}
	repo := NewRepository(c.DB, c.SQLTemplate)
	deduplicationID, groupID, delaySeconds := extractMetadata(input.Metadata)
	_, err = c.Retryer.Do(ctx, func(ctx context.Context) error {
		jobID := newJobID()
		return repo.EnqueueJob(ctx,
			queue.RawName,
			jobID,
			input.Content,
			deduplicationID,
			groupID,
			delaySeconds,
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
	return &jobworker.EnqueueOutput{}, nil
}

func isDuplicateEntryID(entries []*jobworker.EnqueueBatchEntry) bool {
	entryCnt := len(entries)
	entrySet := make(map[string]struct{})
	for _, entry := range entries {
		entrySet[entry.ID] = struct{}{}
	}
	return len(entrySet) < entryCnt
}

func (c *Connector) EnqueueBatch(ctx context.Context, input *jobworker.EnqueueBatchInput) (*jobworker.EnqueueBatchOutput, error) {

	if isDuplicateEntryID(input.Entries) {
		return nil, jobworker.ErrDuplicateEntryID
	}

	queue, err := c.resolveQueueAttributes(ctx, input.Queue)
	if err != nil {
		return nil, err
	}

	_, err = c.Retryer.Do(ctx, func(ctx context.Context) error {
		return c.enqueueJobBatch(ctx, queue, input.Entries)
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}

	var ids []string
	for _, entry := range input.Entries {
		ids = append(ids, entry.ID)
	}
	return &jobworker.EnqueueBatchOutput{
		Successful: ids,
	}, nil
}

func (c *Connector) CompleteJob(ctx context.Context, input *jobworker.CompleteJobInput) (*jobworker.CompleteJobOutput, error) {
	repo := NewRepository(c.DB, c.SQLTemplate)

	queue, err := c.resolveQueueAttributes(ctx, input.Job.QueueName)
	if err != nil {
		return nil, err
	}

	rawJob := input.Job.Raw.(*Job)
	_, err = c.Retryer.Do(ctx, func(ctx context.Context) error {
		return repo.DeleteJob(ctx, queue.RawName, rawJob.JobID)
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}

	return &jobworker.CompleteJobOutput{}, nil
}

func (c *Connector) FailJob(ctx context.Context, input *jobworker.FailJobInput) (*jobworker.FailJobOutput, error) {
	_, err := c.ChangeJobVisibility(ctx, &ChangeJobVisibilityInput{
		Job:               input.Job,
		VisibilityTimeout: 0,
	})
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) Close() error {
	return c.DB.Close()
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

var (
	ErrNoFoundQueue = fmt.Errorf("no found queue")
)

func (c *Connector) resolveQueueAttributes(ctx context.Context, name string) (*QueueAttributes, error) {
	v, ok := c.name2Queue.Load(name)
	if !ok || v == nil {
		repo := NewRepository(c.DB, c.SQLTemplate)
		q, err := repo.FindQueueAttribute(ctx, name)
		if isErrNoRows(err) {
			return nil, ErrNoFoundQueue
		}
		if err != nil {
			return nil, fmt.Errorf("could not find queue %w", err)
		}
		v = q
		// TODO Expiration date can be specified from outside
		c.name2Queue.StoreWithExpire(name, v, time.Minute)
	}
	return v.(*QueueAttributes), nil
}

func (c *Connector) cleanJobBatch(ctx context.Context, queue *QueueAttributes, jobs []*Job) error {
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
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
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	return err
}

func (c *Connector) moveJobBatch(ctx context.Context, from, to *QueueAttributes, jobs []*Job) error {
	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		return withTransaction(c.DB, func(tx *sql.Tx) error {
			repo := NewRepository(tx, c.SQLTemplate)
			for _, job := range jobs {
				err := repo.EnqueueJobWithTime(ctx,
					to.RawName,
					job.JobID,
					job.Content,
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
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	return err
}

func (c *Connector) grabJobs(ctx context.Context,
	queueAttr *QueueAttributes, maxNumberOfJobs, visibilityTimeout int64) ([]*Job, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	jobs, err := repo.FindJobs(ctx, queueAttr.RawName, maxNumberOfJobs)
	if err != nil {
		return nil, err
	}

	var (
		deadJobs  []*Job
		aliveJobs []*Job
	)

	for _, job := range jobs {
		if job.RetryCount > queueAttr.MaxReceiveCount {
			deadJobs = append(deadJobs, job)
		} else {
			aliveJobs = append(aliveJobs, job)
		}
	}

	if len(deadJobs) > 0 {
		err = c.handleDeadJobs(ctx, queueAttr, deadJobs)
		if err != nil {
			// TODO
			c.loggerFunc("could not handle dead job", err)
		}
	}

	shuffled := map[*Job]struct{}{}
	for _, j := range jobs {
		shuffled[j] = struct{}{}
	}

	var deliveries []*Job
	for job := range shuffled {

		grabbed, err := repo.GrabJob(ctx, queueAttr.RawName, job, visibilityTimeout)
		if err != nil || !grabbed {
			continue
		}

		deliveries = append(deliveries, job)
	}
	return deliveries, nil
}

func (c *Connector) handleDeadJobs(ctx context.Context, queueAttr *QueueAttributes, deadJobs []*Job) error {
	if queueAttr.HasDeadLetter() {
		deadLetterQueue, err := c.resolveQueueAttributes(ctx, queueAttr.DeadLetterTarget)
		if err != nil {
			return err
		}
		err = c.moveJobBatch(ctx, queueAttr, deadLetterQueue, deadJobs)
		if err != nil {
			return fmt.Errorf("could not move job batch: %s", err)
		}
	} else {
		err := c.cleanJobBatch(ctx, queueAttr, deadJobs)
		if err != nil {
			return fmt.Errorf("could not clean job batch: %s", err)
		}
	}
	return nil
}

func extractMetadata(metadata map[string]string) (
	deduplicationID *string,
	groupID *string,
	delaySeconds int64) {
	if v, ok := metadata[MetadataKeyDeduplicationID]; ok && v != "" {
		deduplicationID = &v
	}
	if v, ok := metadata[MetadataKeyGroupID]; ok && v != "" {
		groupID = &v
	}
	if v, ok := metadata[MetadataKeyDelaySeconds]; ok && v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			delaySeconds = i
		}
	}
	return
}

func (c *Connector) enqueueJobBatch(ctx context.Context, queue *QueueAttributes, entries []*jobworker.EnqueueBatchEntry) error {
	return withTransaction(c.DB, func(tx *sql.Tx) error {
		repo := NewRepository(tx, c.SQLTemplate)
		for _, entry := range entries {
			deduplicationID, groupID, delaySeconds := extractMetadata(entry.Metadata)
			err := repo.EnqueueJob(ctx,
				queue.RawName,
				newJobID(),
				entry.Content,
				deduplicationID,
				groupID,
				delaySeconds,
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

func (c *Connector) ChangeJobVisibility(ctx context.Context, input *ChangeJobVisibilityInput) (*ChangeJobVisibilityOutput, error) {
	repo := NewRepository(c.DB, c.SQLTemplate)
	queue, err := c.resolveQueueAttributes(ctx, input.Job.QueueName)
	if err != nil {
		return nil, err
	}
	_, err = c.Retryer.Do(ctx, func(ctx context.Context) error {
		_, err = repo.UpdateJobVisibility(ctx,
			queue.RawName, input.Job.Metadata[MetadataKeyJobID], input.VisibilityTimeout)
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
	return &ChangeJobVisibilityOutput{}, nil
}

func (c *Connector) RedriveJob(ctx context.Context, input *RedriveJobInput) (*RedriveJobOutput, error) {
	from, err := c.resolveQueueAttributes(ctx, input.From)
	if err != nil {
		return nil, err
	}
	to, err := c.resolveQueueAttributes(ctx, input.To)
	if err != nil {
		return nil, err
	}
	_, err = c.Retryer.Do(ctx, func(ctx context.Context) error {
		return c.redriveJobBatch(ctx, from, to, input.Target, input.DelaySeconds)
	}, func(err error) bool {
		return c.IsDeadlockDetected(err)
	})
	return &RedriveJobOutput{}, err
}

func (c *Connector) redriveJobBatch(ctx context.Context, from, to *QueueAttributes, jobID string, delaySeconds int64) error {
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
			job.Content,
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
	return fmt.Sprintf("%s_%s", TablePrefix, rawName)
}

func (c *Connector) CreateQueue(ctx context.Context, input *CreateQueueInput) (*CreateQueueOutput, error) {

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

	return &CreateQueueOutput{}, nil

}

func (c *Connector) UpdateQueue(ctx context.Context, input *UpdateQueueInput) (*UpdateQueueOutput, error) {

	repo := NewRepository(c.DB, c.SQLTemplate)

	v := queueAttrsToValues(input.Attributes)

	_, err := c.Retryer.Do(ctx, func(ctx context.Context) error {
		queue, err := c.resolveQueueAttributes(ctx, input.Name)
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

	return &UpdateQueueOutput{}, nil
}

type ChangeJobVisibilityInput struct {
	Job               *jobworker.Job
	VisibilityTimeout int64
}

type ChangeJobVisibilityOutput struct{}

type CreateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type CreateQueueOutput struct{}

type UpdateQueueInput struct {
	Name       string
	Attributes map[string]interface{}
}

type UpdateQueueOutput struct{}

type RedriveJobInput struct {
	From         string
	To           string
	Target       string
	DelaySeconds int64
}

type RedriveJobOutput struct{}
