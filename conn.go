package dbconnector

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-jwdk/db-connector/internal"
	"github.com/go-jwdk/jobworker"
	uuid "github.com/satori/go.uuid"
	"github.com/vvatanabe/expiremap"
	"github.com/vvatanabe/goretryer/exponential"
)

const (
	TablePrefix              = "jwdk"
	logPrefix                = "[" + TablePrefix + "]"
	defaultVisibilityTimeout = int64(30)
)

var (
	defaultIsisUniqueViolation = func(err error) bool {
		return false
	}
	defaultIsDeadlockDetected = func(err error) bool {
		return false
	}
)

type Config struct {
	Name                  string
	DB                    *sql.DB
	NumMaxRetries         int
	QueueAttributesExpire time.Duration

	SQLTemplate        SQLTemplate
	IsUniqueViolation  func(err error) bool
	IsDeadlockDetected func(err error) bool
}

func Open(cfg *Config) (*Connector, error) {

	retryer := exponential.Retryer{
		NumMaxRetries: cfg.NumMaxRetries,
	}

	isUniqueViolation := defaultIsisUniqueViolation
	if cfg.IsUniqueViolation != nil {
		isUniqueViolation = cfg.IsUniqueViolation
	}
	isDeadlockDetected := defaultIsDeadlockDetected
	if cfg.IsDeadlockDetected != nil {
		isDeadlockDetected = cfg.IsDeadlockDetected
	}

	conn := &Connector{
		name:                  cfg.Name,
		db:                    cfg.DB,
		isUniqueViolation:     isUniqueViolation,
		isDeadlockDetected:    isDeadlockDetected,
		queueAttributesExpire: cfg.QueueAttributesExpire,
		retryer:               retryer,
		repo: &repositoryOnDB{
			querier: cfg.DB,
			tmpl:    cfg.SQLTemplate,
		},
	}
	err := conn.repo.createQueueAttributesTable(context.Background())
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type Connector struct {
	name                  string
	db                    *sql.DB
	isUniqueViolation     func(err error) bool
	isDeadlockDetected    func(err error) bool
	queueAttributesExpire time.Duration

	retryer exponential.Retryer
	repo    repository

	name2Queue expiremap.Map

	loggerFunc jobworker.LoggerFunc
}

func (c *Connector) Name() string {
	return c.name
}

func (c *Connector) Subscribe(ctx context.Context, input *jobworker.SubscribeInput) (*jobworker.SubscribeOutput, error) {
	var sub *subscription
	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get queue attributes: %w", err)
	}
	sub = newSubscription(out.Attributes, c, c)
	if len(input.Metadata) > 0 {
		sub.SetMetadata(input.Metadata)
	}
	go sub.Start()
	return &jobworker.SubscribeOutput{
		Subscription: sub,
	}, nil
}

func (c *Connector) Enqueue(ctx context.Context, input *jobworker.EnqueueInput) (*jobworker.EnqueueOutput, error) {
	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get queue attributes: %w", err)
	}
	deduplicationID, groupID, delaySeconds := extractMetadata(input.Metadata)
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		jobID := newJobID()
		return c.repo.enqueueJob(ctx,
			out.Attributes.RawName,
			jobID,
			input.Content,
			deduplicationID,
			groupID,
			delaySeconds,
		)
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		if c.isUniqueViolation(err) {
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

	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Queue,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get queue attributes: %w", err)
	}

	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		return c.enqueueJobBatch(ctx, out.Attributes, input.Entries)
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
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

	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Job.QueueName,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get queue attributes: %w", err)
	}

	rawJob := input.Job.Raw.(*internal.Job)
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		return c.repo.deleteJob(ctx, out.Attributes.RawName, rawJob.JobID)
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
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
	if err != nil {
		return nil, err
	}
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) Close() error {
	return c.db.Close()
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

// --------------------
// Proprietary API
// --------------------

var (
	ErrNotFoundQueue = fmt.Errorf("not found queue")
)

type GetQueueAttributesInput struct {
	QueueName string
}

type GetQueueAttributesOutput struct {
	Attributes *QueueAttributes
}

func (c *Connector) GetQueueAttributes(ctx context.Context, input *GetQueueAttributesInput) (*GetQueueAttributesOutput, error) {
	v, ok := c.name2Queue.Load(input.QueueName)
	if !ok || v == nil {
		q, err := c.repo.getQueueAttributes(ctx, input.QueueName)
		if internal.IsErrNoRows(err) {
			return nil, ErrNotFoundQueue
		}
		if err != nil {
			return nil, err
		}
		v = q
		c.name2Queue.StoreWithExpire(input.QueueName, v, c.queueAttributesExpire)
	}
	return &GetQueueAttributesOutput{
		Attributes: v.(*QueueAttributes),
	}, nil
}

type DeleteJobBatchInput struct {
	Jobs []*jobworker.Job
}

type DeleteJobBatchOutput struct {
}

func (c *Connector) DeleteJobBatch(ctx context.Context, input *DeleteJobBatchInput) (*DeleteJobBatchOutput, error) {
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		return internal.WithTransaction(c.db, func(tx internal.Querier) error {
			repo := c.repo.renew(tx)
			for _, job := range input.Jobs {
				raw := job.Raw.(*internal.Job)
				out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
					QueueName: job.QueueName,
				})
				if err != nil {
					return err
				}
				err = repo.deleteJob(ctx, out.Attributes.RawName, raw.JobID)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &DeleteJobBatchOutput{}, nil
}

type MoveJobBatchInput struct {
	Jobs []*jobworker.Job
	To   string
}

type MoveJobBatchOutput struct {
}

func (c *Connector) MoveJobBatch(ctx context.Context, input *MoveJobBatchInput) (*MoveJobBatchOutput, error) {
	to, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.To,
	})
	if err != nil {
		return nil, err
	}
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		return internal.WithTransaction(c.db, func(tx internal.Querier) error {
			repo := c.repo.renew(tx)
			for _, job := range input.Jobs {
				raw := job.Raw.(*internal.Job)
				err := repo.enqueueJobWithTime(ctx,
					to.Attributes.RawName,
					raw.JobID,
					job.Content,
					raw.DeduplicationID,
					raw.GroupID,
					raw.EnqueueAt,
				)
				if err != nil {
					if c.isUniqueViolation(err) {
						continue
					}
					return err
				}
				from, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
					QueueName: job.QueueName,
				})
				if err != nil {
					return err
				}
				err = repo.deleteJob(ctx, from.Attributes.RawName, raw.JobID)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &MoveJobBatchOutput{}, nil
}

type GrabJobsInput struct {
	QueueName         string
	MaxNumberOfJobs   int64
	VisibilityTimeout int64
}

type GrabJobsOutput struct {
	Jobs []*jobworker.Job
}

func (c *Connector) GrabJobs(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error) {

	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.QueueName,
	})
	if err != nil {
		return nil, err
	}

	rawJobs, err := c.repo.getJobs(ctx, out.Attributes.RawName, input.MaxNumberOfJobs)
	if err != nil {
		return nil, err
	}

	var (
		deadJobs  []*jobworker.Job
		aliveJobs []*jobworker.Job
	)

	for _, rawJob := range rawJobs {
		job := internal.NewJob(out.Attributes.Name, rawJob, c)
		if rawJob.RetryCount > out.Attributes.MaxReceiveCount {
			deadJobs = append(deadJobs, job)
		} else {
			aliveJobs = append(aliveJobs, job)
		}
	}

	if len(deadJobs) > 0 {
		err = c.handleDeadJobs(ctx, out.Attributes, deadJobs)
		if err != nil {
			c.loggerFunc("could not handle dead job", err)
		}
	}

	shuffled := map[*jobworker.Job]struct{}{}
	for _, j := range aliveJobs {
		shuffled[j] = struct{}{}
	}

	var deliveries []*jobworker.Job
	for job := range shuffled {
		rawJob := job.Raw.(*internal.Job)
		grabbed, err := c.repo.grabJob(ctx, out.Attributes.RawName,
			rawJob.JobID, rawJob.RetryCount, rawJob.InvisibleUntil, input.VisibilityTimeout)
		if err != nil {
			c.loggerFunc("could not grab job", err)
			continue
		}
		if !grabbed {
			continue
		}
		deliveries = append(deliveries, job)
	}
	return &GrabJobsOutput{
		Jobs: deliveries,
	}, nil
}

func (c *Connector) handleDeadJobs(ctx context.Context, queueAttributes *QueueAttributes, deadJobs []*jobworker.Job) error {
	if name, ok := queueAttributes.HasDeadLetter(); ok {
		out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
			QueueName: name,
		})
		if err != nil {
			return err
		}
		_, err = c.MoveJobBatch(ctx, &MoveJobBatchInput{
			Jobs: deadJobs,
			To:   out.Attributes.Name,
		})
		if err != nil {
			return fmt.Errorf("could not move job to dead letter queue: %s", err)
		}
	} else {
		_, err := c.DeleteJobBatch(ctx, &DeleteJobBatchInput{
			Jobs: deadJobs,
		})
		if err != nil {
			return fmt.Errorf("could not delete job batch: %s", err)
		}
	}
	return nil
}

func extractMetadata(metadata map[string]string) (
	deduplicationID *string,
	groupID *string,
	delaySeconds int64) {
	if v, ok := metadata[internal.MetadataKeyDeduplicationID]; ok && v != "" {
		deduplicationID = &v
	}
	if v, ok := metadata[internal.MetadataKeyGroupID]; ok && v != "" {
		groupID = &v
	}
	if v, ok := metadata[internal.MetadataKeyDelaySeconds]; ok && v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			delaySeconds = i
		}
	}
	return
}

func (c *Connector) enqueueJobBatch(ctx context.Context, queue *QueueAttributes, entries []*jobworker.EnqueueBatchEntry) error {
	return internal.WithTransaction(c.db, func(tx internal.Querier) error {
		repo := c.repo.renew(tx)
		for _, entry := range entries {
			deduplicationID, groupID, delaySeconds := extractMetadata(entry.Metadata)
			err := repo.enqueueJob(ctx,
				queue.RawName,
				newJobID(),
				entry.Content,
				deduplicationID,
				groupID,
				delaySeconds,
			)
			if err != nil {
				if c.isUniqueViolation(err) {
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

type ChangeJobVisibilityInput struct {
	Job               *jobworker.Job
	VisibilityTimeout int64
}

type ChangeJobVisibilityOutput struct{}

func (c *Connector) ChangeJobVisibility(ctx context.Context, input *ChangeJobVisibilityInput) (*ChangeJobVisibilityOutput, error) {
	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Job.QueueName,
	})
	if err != nil {
		return nil, err
	}
	raw := input.Job.Raw.(*internal.Job)
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		_, err = c.repo.updateJobVisibility(ctx,
			out.Attributes.RawName,
			raw.JobID,
			input.VisibilityTimeout)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &ChangeJobVisibilityOutput{}, nil
}

type RedriveJobInput struct {
	From         string
	To           string
	Target       string
	DelaySeconds int64
}

type RedriveJobOutput struct{}

func (c *Connector) RedriveJob(ctx context.Context, input *RedriveJobInput) (*RedriveJobOutput, error) {
	fromOut, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.From,
	})
	if err != nil {
		return nil, err
	}
	toOut, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.To,
	})
	if err != nil {
		return nil, err
	}
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		return c.redriveJob(ctx, fromOut.Attributes, toOut.Attributes, input.Target, input.DelaySeconds)
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	return &RedriveJobOutput{}, err
}

func (c *Connector) redriveJob(ctx context.Context, from, to *QueueAttributes, jobID string, delaySeconds int64) error {
	job, err := c.repo.getJob(ctx, from.RawName, jobID)
	if err != nil {
		if internal.IsErrNoRows(err) {
			// TODO logging
			return nil
		}
		return err
	}
	return internal.WithTransaction(c.db, func(tx internal.Querier) error {
		repo := c.repo.renew(tx)
		err = repo.enqueueJob(ctx,
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
		err = repo.deleteJob(ctx, from.RawName, job.JobID)
		if err != nil {
			return err
		}
		return nil
	})
}

type CreateQueueInput struct {
	Name              string
	DelaySeconds      int64
	VisibilityTimeout int64
	MaxReceiveCount   int64
	DeadLetterTarget  string
}

func (in *CreateQueueInput) applyDefaultValues() *CreateQueueInput {
	v := *in
	if v.VisibilityTimeout == 0 {
		v.VisibilityTimeout = defaultVisibilityTimeout
	}
	return &v
}

type CreateQueueOutput struct{}

func (c *Connector) CreateQueue(ctx context.Context, input *CreateQueueInput) (*CreateQueueOutput, error) {
	input = input.applyDefaultValues()
	queueRawName := queueRawName(input.Name)
	var deadLetterTarget *string
	if input.DeadLetterTarget != "" {
		deadLetterTarget = &input.DeadLetterTarget
	}
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		err := c.repo.createQueueTable(ctx, queueRawName)
		if err != nil {
			return err
		}
		err = c.repo.createQueueAttributes(ctx,
			input.Name,
			queueRawName,
			input.VisibilityTimeout,
			input.DelaySeconds,
			input.MaxReceiveCount,
			deadLetterTarget)
		if err != nil && !c.isUniqueViolation(err) {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &CreateQueueOutput{}, nil
}

func queueRawName(name string) string {
	rawName := strings.Replace(name, ".", "_", -1)
	return fmt.Sprintf("%s_%s", TablePrefix, rawName)
}

type SetQueueAttributesInput struct {
	QueueName              string
	DelaySeconds           *int64
	VisibilityTimeout      *int64
	MaximumMessageSize     *int64
	MessageRetentionPeriod *int64
	DeadLetterTarget       *string
	MaxReceiveCount        *int64
}

type SetQueueAttributesOutput struct{}

func (c *Connector) SetQueueAttributes(ctx context.Context, input *SetQueueAttributesInput) (*SetQueueAttributesOutput, error) {
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
			QueueName: input.QueueName,
		})
		if err != nil {
			return err
		}
		_, err = c.repo.updateQueueAttributes(
			ctx,
			out.Attributes.RawName,
			input.VisibilityTimeout,
			input.DelaySeconds,
			input.MaxReceiveCount,
			input.DeadLetterTarget,
		)
		if err != nil {
			return err
		}
		return nil
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	if err != nil {
		return nil, err
	}
	return &SetQueueAttributesOutput{}, nil
}

func (c *Connector) debug(args ...interface{}) {
	if c.verbose() {
		args = append([]interface{}{logPrefix}, args...)
		c.loggerFunc(args...)
	}
}

func (c *Connector) verbose() bool {
	return c.loggerFunc != nil
}
