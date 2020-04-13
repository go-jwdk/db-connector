package dbconn

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
	TablePrefix = "jwdk"
	LogPrefix   = "[" + TablePrefix + "]"

	connAttributeNameDSN             = "DSN"
	connAttributeNameMaxOpenConns    = "MaxOpenConns"
	connAttributeNameMaxIdleConns    = "MaxMaxIdleConns"
	connAttributeNameConnMaxLifetime = "ConnMaxLifetime"
	connAttributeNameNumMaxRetries   = "NumMaxRetries"

	defaultNumMaxRetries         = 3
	defaultQueueAttributesExpire = time.Minute
	defaultVisibilityTimeout     = int64(30)
)

type connAttributeValues struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime *time.Duration
	NumMaxRetries   *int
}

func (v *connAttributeValues) ApplyDefaultValues() {
	if v.NumMaxRetries == nil {
		i := defaultNumMaxRetries
		v.NumMaxRetries = &i
	}
}

func ConnAttrsToValues(attrs map[string]interface{}) *connAttributeValues {
	var values connAttributeValues
	for k, v := range attrs {
		switch k {
		case connAttributeNameDSN:
			s := v.(string)
			values.DSN = s
		case connAttributeNameMaxOpenConns:
			s := v.(int)
			values.MaxOpenConns = s
		case connAttributeNameMaxIdleConns:
			s := v.(int)
			values.MaxIdleConns = s
		case connAttributeNameConnMaxLifetime:
			s := v.(time.Duration)
			values.ConnMaxLifetime = &s
		case connAttributeNameNumMaxRetries:
			s := v.(int)
			values.NumMaxRetries = &s
		}
	}
	return &values
}

type Setting struct {
	Name                  string
	DB                    *sql.DB
	SQLTemplate           internal.SQLTemplate
	IsUniqueViolation     func(err error) bool
	IsDeadlockDetected    func(err error) bool
	NumMaxRetries         *int
	QueueAttributesExpire *int64
}

func Open(s Setting) (*Connector, error) {
	repo := internal.NewRepository(s.DB, s.SQLTemplate)
	err := repo.DefineQueueAttributes(context.Background())
	if err != nil {
		return nil, err
	}
	var er exponential.Retryer
	if s.NumMaxRetries != nil {
		er.NumMaxRetries = *s.NumMaxRetries
	}
	queueAttributesExpire := defaultQueueAttributesExpire
	if s.QueueAttributesExpire != nil {
		queueAttributesExpire = time.Duration(*s.QueueAttributesExpire) * time.Minute
	}
	return &Connector{
		name:                  s.Name,
		db:                    s.DB,
		sqlTmpl:               s.SQLTemplate,
		isUniqueViolation:     s.IsUniqueViolation,
		isDeadlockDetected:    s.IsDeadlockDetected,
		queueAttributesExpire: queueAttributesExpire,
		retryer:               er,
	}, nil
}

type Connector struct {
	name                  string
	db                    *sql.DB
	sqlTmpl               internal.SQLTemplate
	isUniqueViolation     func(err error) bool
	isDeadlockDetected    func(err error) bool
	queueAttributesExpire time.Duration

	retryer    exponential.Retryer
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
	sub = newSubscription(out.Attributes, c, c, input.Metadata)
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
	repo := internal.NewRepository(c.db, c.sqlTmpl)
	deduplicationID, groupID, delaySeconds := extractMetadata(input.Metadata)
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		jobID := newJobID()
		return repo.EnqueueJob(ctx,
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

	repo := internal.NewRepository(c.db, c.sqlTmpl)
	rawJob := input.Job.Raw.(*internal.Job)
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		return repo.DeleteJob(ctx, out.Attributes.RawName, rawJob.JobID)
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
	return &jobworker.FailJobOutput{}, err
}

func (c *Connector) Close() error {
	return c.db.Close()
}

func (c *Connector) SetLoggerFunc(f jobworker.LoggerFunc) {
	c.loggerFunc = f
}

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
		repo := internal.NewRepository(c.db, c.sqlTmpl)
		q, err := repo.GetQueueAttributes(ctx, input.QueueName)
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
		return internal.WithTransaction(c.db, func(tx *sql.Tx) error {
			repo := internal.NewRepository(tx, c.sqlTmpl)
			for _, job := range input.Jobs {
				raw := job.Raw.(*internal.Job)
				out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
					QueueName: job.QueueName,
				})
				if err != nil {
					return err
				}
				err = repo.DeleteJob(ctx, out.Attributes.RawName, raw.JobID)
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
		return internal.WithTransaction(c.db, func(tx *sql.Tx) error {
			repo := internal.NewRepository(tx, c.sqlTmpl)
			for _, job := range input.Jobs {
				raw := job.Raw.(*internal.Job)
				err := repo.EnqueueJobWithTime(ctx,
					to.Attributes.RawName,
					raw.JobID,
					raw.Content,
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
				err = repo.DeleteJob(ctx, from.Attributes.RawName, raw.JobID)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}, func(err error) bool {
		return c.isDeadlockDetected(err)
	})
	return nil, err
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

	repo := internal.NewRepository(c.db, c.sqlTmpl)
	rawJobs, err := repo.GetJobs(ctx, out.Attributes.RawName, input.MaxNumberOfJobs)
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
		grabbed, err := repo.GrabJob(ctx, out.Attributes.RawName,
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
	if queueAttributes.HasDeadLetter() {
		out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
			QueueName: queueAttributes.DeadLetterTarget,
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
	return internal.WithTransaction(c.db, func(tx *sql.Tx) error {
		repo := internal.NewRepository(tx, c.sqlTmpl)
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
	repo := internal.NewRepository(c.db, c.sqlTmpl)
	out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
		QueueName: input.Job.QueueName,
	})
	_, err = c.retryer.Do(ctx, func(ctx context.Context) error {
		_, err = repo.UpdateJobVisibility(ctx,
			out.Attributes.RawName,
			input.Job.Metadata[internal.MetadataKeyJobID],
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
	repo := internal.NewRepository(c.db, c.sqlTmpl)
	job, err := repo.GetJob(ctx, from.RawName, jobID)
	if err != nil {
		if internal.IsErrNoRows(err) {
			// TODO logging
			return nil
		}
		return err
	}
	return internal.WithTransaction(c.db, func(tx *sql.Tx) error {
		repo := internal.NewRepository(tx, c.sqlTmpl)
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

type CreateQueueInput struct {
	Name                   string
	DelaySeconds           int64
	VisibilityTimeout      int64
	MaximumMessageSize     int64
	MessageRetentionPeriod int64
	DeadLetterTarget       string
	MaxReceiveCount        int64
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
	repo := internal.NewRepository(c.db, c.sqlTmpl)
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		err := repo.DefineQueue(ctx, queueRawName)
		if err != nil {
			return err
		}
		err = repo.CreateQueueAttributes(ctx,
			input.Name,
			queueRawName,
			input.VisibilityTimeout,
			input.DelaySeconds,
			input.MaximumMessageSize,
			input.MessageRetentionPeriod,
			input.MaxReceiveCount,
			input.DeadLetterTarget)
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
	repo := internal.NewRepository(c.db, c.sqlTmpl)
	_, err := c.retryer.Do(ctx, func(ctx context.Context) error {
		out, err := c.GetQueueAttributes(ctx, &GetQueueAttributesInput{
			QueueName: input.QueueName,
		})
		if err != nil {
			return err
		}
		_, err = repo.UpdateQueueAttributes(
			ctx,
			out.Attributes.RawName,
			input.VisibilityTimeout,
			input.DelaySeconds,
			input.MaximumMessageSize,
			input.MessageRetentionPeriod,
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
