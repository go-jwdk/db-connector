package dbconnector

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-jwdk/db-connector/internal"

	"github.com/go-jwdk/jobworker"
	"github.com/vvatanabe/goretryer/exponential"
)

func TestConnector_Subscribe(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			return &QueueAttributes{
				Name: "foo",
			}, nil
		},
		getJobsFunc: func(ctx context.Context, queue string, limit int64) ([]*internal.Job, error) {
			return []*internal.Job{
				{}, {}, {},
			}, nil
		},
		grabJobFunc: func(ctx context.Context, queue string, jobID string, currentRetryCount, currentInvisibleUntil, invisibleTime int64) (grabbed bool, err error) {
			return true, nil
		},
	}

	type fields struct {
		retryer exponential.Retryer
		repo    repository
	}
	type args struct {
		ctx   context.Context
		input *jobworker.SubscribeInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				retryer: exponential.Retryer{},
				repo:    repo,
			},
			args: args{
				ctx: nil,
				input: &jobworker.SubscribeInput{
					Queue: "foo",
					Metadata: map[string]string{
						"PollingInterval":   "1",
						"VisibilityTimeout": "2",
						"MaxNumberOfJobs":   "3",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				retryer: exponential.Retryer{},
				repo:    repo,
			},
			args: args{
				ctx: nil,
				input: &jobworker.SubscribeInput{
					Queue: "",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				retryer: tt.fields.retryer,
				repo:    tt.fields.repo,
			}
			got, err := c.Subscribe(tt.args.ctx, tt.args.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Subscribe() error = %v, wantErr %v", err, tt.wantErr)
				}
				if got != nil {
					t.Errorf("Subscribe() got = %v", got)
				}
				return
			}
			if got == nil {
				t.Errorf("Subscribe() got = %v", got)
			}
		})
	}
}

func TestConnector_Enqueue(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			return &QueueAttributes{
				Name: "foo",
			}, nil
		},
		enqueueJobFunc: func(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error {
			if content == "" {
				return errors.New("content is empty")
			}
			return nil
		},
	}

	type fields struct {
		isUniqueViolation  func(err error) bool
		isDeadlockDetected func(err error) bool
		retryer            exponential.Retryer
		repo               repository
	}
	type args struct {
		ctx   context.Context
		input *jobworker.EnqueueInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.EnqueueOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				isDeadlockDetected: defaultIsDeadlockDetected,
				isUniqueViolation:  defaultIsisUniqueViolation,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Queue:   "foo",
					Content: "hello",
				},
			},
			want:    &jobworker.EnqueueOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				isDeadlockDetected: defaultIsDeadlockDetected,
				isUniqueViolation:  defaultIsisUniqueViolation,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Content: "hello",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isDeadlockDetected: defaultIsDeadlockDetected,
				isUniqueViolation:  defaultIsisUniqueViolation,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueInput{
					Queue: "foo",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				isUniqueViolation:  tt.fields.isUniqueViolation,
				isDeadlockDetected: tt.fields.isDeadlockDetected,
				retryer:            tt.fields.retryer,
				repo:               tt.fields.repo,
			}
			got, err := c.Enqueue(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Enqueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Enqueue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_EnqueueBatch(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			return &QueueAttributes{
				Name: "foo",
			}, nil
		},
		enqueueJobFunc: func(ctx context.Context, queue, jobID, content string, deduplicationID, groupID *string, delaySeconds int64) error {
			if content == "" {
				return errors.New("content is empty")
			}
			return nil
		},
	}

	type fields struct {
		isUniqueViolation  func(err error) bool
		isDeadlockDetected func(err error) bool
		retryer            exponential.Retryer
		repo               repository
	}
	type args struct {
		ctx   context.Context
		input *jobworker.EnqueueBatchInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.EnqueueBatchOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue: "foo",
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "uniq-1",
							Content: "hello",
						},
						{
							ID:      "uniq-2",
							Content: "hello",
						},
						{
							ID:      "uniq-3",
							Content: "hello",
						},
					},
				},
			},
			want: &jobworker.EnqueueBatchOutput{
				Failed: nil,
				Successful: []string{
					"uniq-1", "uniq-2", "uniq-3",
				},
			},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue: "",
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "uniq-1",
							Content: "hello",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue: "foo",
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "uniq-1",
							Content: "hello",
						},
						{
							ID:      "uniq-1",
							Content: "hello",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.EnqueueBatchInput{
					Queue: "foo",
					Entries: []*jobworker.EnqueueBatchEntry{
						{
							ID:      "uniq-1",
							Content: "",
						},
						{
							ID:      "uniq-2",
							Content: "hello",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				isUniqueViolation:  tt.fields.isUniqueViolation,
				isDeadlockDetected: tt.fields.isDeadlockDetected,
				retryer:            tt.fields.retryer,
				repo:               tt.fields.repo,
			}
			got, err := c.EnqueueBatch(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("EnqueueBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EnqueueBatch() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_CompleteJob(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			return &QueueAttributes{
				Name: "foo",
			}, nil
		},
		deleteJobFunc: func(ctx context.Context, queue string, jobID string) error {
			if jobID == "" {
				return errors.New("job id is empty")
			}
			return nil
		},
	}

	type fields struct {
		isUniqueViolation  func(err error) bool
		isDeadlockDetected func(err error) bool
		retryer            exponential.Retryer
		repo               repository
	}
	type args struct {
		ctx   context.Context
		input *jobworker.CompleteJobInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.CompleteJobOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &internal.Job{
							JobID: "foo-1",
						},
					},
				},
			},
			want:    &jobworker.CompleteJobOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						Raw: &internal.Job{
							JobID: "foo-1",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.CompleteJobInput{
					Job: &jobworker.Job{
						Raw: &internal.Job{
							JobID: "",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				isUniqueViolation:  tt.fields.isUniqueViolation,
				isDeadlockDetected: tt.fields.isDeadlockDetected,
				retryer:            tt.fields.retryer,
				repo:               tt.fields.repo,
			}
			got, err := c.CompleteJob(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompleteJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CompleteJob() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_FailJob(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			return &QueueAttributes{
				Name:    "foo",
				RawName: "raw_foo",
			}, nil
		},
		updateJobVisibilityFunc: func(ctx context.Context, queueRawName, jobID string, visibilityTimeout int64) (updated bool, err error) {
			if jobID == "" {
				return false, errors.New("job id is empty")
			}
			return true, nil
		},
	}

	type fields struct {
		isUniqueViolation  func(err error) bool
		isDeadlockDetected func(err error) bool
		retryer            exponential.Retryer
		repo               repository
	}
	type args struct {
		ctx   context.Context
		input *jobworker.FailJobInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *jobworker.FailJobOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &internal.Job{
							JobID: "foo-1",
						},
					},
				},
			},
			want:    &jobworker.FailJobOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						QueueName: "",
						Raw: &internal.Job{
							JobID: "foo-1",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &jobworker.FailJobInput{
					Job: &jobworker.Job{
						QueueName: "foo",
						Raw: &internal.Job{
							JobID: "",
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				isUniqueViolation:  tt.fields.isUniqueViolation,
				isDeadlockDetected: tt.fields.isDeadlockDetected,
				retryer:            tt.fields.retryer,
				repo:               tt.fields.repo,
			}
			got, err := c.FailJob(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("FailJob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FailJob() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_GetQueueAttributes(t *testing.T) {

	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue mame is empty")
			}
			if queueName == "bar" {
				return nil, sql.ErrNoRows
			}
			return &QueueAttributes{
				Name:    "foo",
				RawName: "raw_foo",
			}, nil
		},
	}

	type fields struct {
		queueAttributesExpire time.Duration
		retryer               exponential.Retryer
		repo                  repository
	}
	type args struct {
		ctx   context.Context
		input *GetQueueAttributesInput
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      *GetQueueAttributesOutput
		wantErr   bool
		withCache bool
	}{
		{
			name: "normal case",
			fields: fields{
				queueAttributesExpire: 0,
				retryer:               exponential.Retryer{},
				repo:                  repo,
			},
			args: args{
				ctx: context.Background(),
				input: &GetQueueAttributesInput{
					QueueName: "foo",
				},
			},
			want: &GetQueueAttributesOutput{
				Attributes: &QueueAttributes{
					Name:    "foo",
					RawName: "raw_foo",
				},
			},
			wantErr: false,
		},
		{
			name: "normal case with cache",
			fields: fields{
				queueAttributesExpire: 0,
				retryer:               exponential.Retryer{},
				repo:                  repo,
			},
			args: args{
				ctx: context.Background(),
				input: &GetQueueAttributesInput{
					QueueName: "foo",
				},
			},
			want: &GetQueueAttributesOutput{
				Attributes: &QueueAttributes{
					Name:    "foo",
					RawName: "raw_foo",
				},
			},
			wantErr:   false,
			withCache: true,
		},
		{
			name: "error case",
			fields: fields{
				queueAttributesExpire: 0,
				retryer:               exponential.Retryer{},
				repo:                  repo,
			},
			args: args{
				ctx: context.Background(),
				input: &GetQueueAttributesInput{
					QueueName: "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				queueAttributesExpire: 0,
				retryer:               exponential.Retryer{},
				repo:                  repo,
			},
			args: args{
				ctx: context.Background(),
				input: &GetQueueAttributesInput{
					QueueName: "bar",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				queueAttributesExpire: tt.fields.queueAttributesExpire,
				retryer:               tt.fields.retryer,
				repo:                  tt.fields.repo,
			}
			if tt.withCache {
				c.name2Queue.Store(tt.args.input.QueueName, tt.want.Attributes)
			}
			got, err := c.GetQueueAttributes(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetQueueAttributes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetQueueAttributes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnector_DeleteJobBatch(t *testing.T) {
	repo := &repositoryMock{
		getQueueAttributesFunc: func(ctx context.Context, queueName string) (*QueueAttributes, error) {
			if queueName == "" {
				return nil, errors.New("queue name is empty")
			}
			return &QueueAttributes{
				Name:    "foo",
				RawName: "raw_foo",
			}, nil
		},
		deleteJobFunc: func(ctx context.Context, queue string, jobID string) error {
			if jobID == "" {
				return errors.New("job id is empty")
			}
			return nil
		},
	}

	type fields struct {
		isUniqueViolation  func(err error) bool
		isDeadlockDetected func(err error) bool
		retryer            exponential.Retryer
		repo               repository
	}
	type args struct {
		ctx   context.Context
		input *DeleteJobBatchInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DeleteJobBatchOutput
		wantErr bool
	}{
		{
			name: "normal case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &DeleteJobBatchInput{
					Jobs: []*jobworker.Job{
						{
							QueueName: "foo",
							Raw: &internal.Job{
								JobID: "1",
							},
						},
						{
							QueueName: "foo",
							Raw: &internal.Job{
								JobID: "2",
							},
						},
					},
				},
			},
			want:    &DeleteJobBatchOutput{},
			wantErr: false,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &DeleteJobBatchInput{
					Jobs: []*jobworker.Job{
						{
							QueueName: "foo",
							Raw: &internal.Job{
								JobID: "1",
							},
						},
						{
							QueueName: "",
							Raw: &internal.Job{
								JobID: "2",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "error case",
			fields: fields{
				isUniqueViolation:  defaultIsisUniqueViolation,
				isDeadlockDetected: defaultIsDeadlockDetected,
				retryer:            exponential.Retryer{},
				repo:               repo,
			},
			args: args{
				ctx: context.Background(),
				input: &DeleteJobBatchInput{
					Jobs: []*jobworker.Job{
						{
							QueueName: "foo",
							Raw: &internal.Job{
								JobID: "1",
							},
						},
						{
							QueueName: "foo",
							Raw: &internal.Job{
								JobID: "",
							},
						},
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{
				isUniqueViolation:  tt.fields.isUniqueViolation,
				isDeadlockDetected: tt.fields.isDeadlockDetected,
				retryer:            tt.fields.retryer,
				repo:               tt.fields.repo,
			}
			got, err := c.DeleteJobBatch(tt.args.ctx, tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteJobBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeleteJobBatch() got = %v, want %v", got, tt.want)
			}
		})
	}
}
