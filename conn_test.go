package dbconnector

import (
	"context"
	"errors"
	"testing"

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
