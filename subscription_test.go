package dbconnector

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-jwdk/jobworker"
)

func Test_newSubscription(t *testing.T) {

	type args struct {
		queueAttributes *QueueAttributes
		conn            jobworker.Connector
		grabber         grabber
	}
	tests := []struct {
		name string
		args args
		want *subscription
	}{
		{
			name: "normal case",
			args: args{
				queueAttributes: &QueueAttributes{
					Name:              "foo",
					RawName:           "raw_foo",
					DelaySeconds:      1,
					VisibilityTimeout: 2,
					MaxReceiveCount:   3,
					DeadLetterTarget:  nil,
				},
				conn:    &jobworker.ConnectorMock{},
				grabber: &grabberMock{},
			},
			want: &subscription{
				queueAttributes: &QueueAttributes{
					Name:              "foo",
					RawName:           "raw_foo",
					DelaySeconds:      1,
					VisibilityTimeout: 2,
					MaxReceiveCount:   3,
					DeadLetterTarget:  nil,
				},
				pollingInterval:   defaultPollingInterval,
				visibilityTimeout: 2,
				maxNumberOfJobs:   defaultMaxNumberOfJobs,
				state:             0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newSubscription(tt.args.queueAttributes, tt.args.conn, tt.args.grabber)
			if !reflect.DeepEqual(got.queueAttributes, tt.want.queueAttributes) {
				t.Errorf("queueAttributes = %v, want %v", got.queueAttributes, tt.want.queueAttributes)
			}
			if !reflect.DeepEqual(got.pollingInterval, tt.want.pollingInterval) {
				t.Errorf("pollingInterval = %v, want %v", got.pollingInterval, tt.want.pollingInterval)
			}
			if !reflect.DeepEqual(got.visibilityTimeout, tt.want.visibilityTimeout) {
				t.Errorf("visibilityTimeout = %v, want %v", got.visibilityTimeout, tt.want.visibilityTimeout)
			}
			if !reflect.DeepEqual(got.maxNumberOfJobs, tt.want.maxNumberOfJobs) {
				t.Errorf("maxNumberOfJobs = %v, want %v", got.maxNumberOfJobs, tt.want.maxNumberOfJobs)
			}
			if reflect.DeepEqual(got.queue, nil) {
				t.Errorf("queue = %v, want %v", got.queue, "a not nil")
			}
			if !reflect.DeepEqual(got.state, subStateActive) {
				t.Errorf("state = %v, want %v", got.state, subStateActive)
			}
		})
	}
}

func Test_subscription_SetMetadata(t *testing.T) {
	type fields struct {
		queueAttributes *QueueAttributes
	}
	type args struct {
		meta map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *subscription
	}{
		{
			name: "normal case",
			fields: fields{
				queueAttributes: &QueueAttributes{
					VisibilityTimeout: defaultVisibilityTimeout,
				},
			},
			args: args{
				meta: map[string]string{
					"PollingInterval":   "1",
					"VisibilityTimeout": "2",
					"MaxNumberOfJobs":   "3",
				},
			},
			want: &subscription{
				queueAttributes: &QueueAttributes{
					VisibilityTimeout: defaultVisibilityTimeout,
				},
				pollingInterval:   time.Second,
				visibilityTimeout: 2,
				maxNumberOfJobs:   3,
			},
		},
		{
			name: "empty",
			fields: fields{
				queueAttributes: &QueueAttributes{
					VisibilityTimeout: defaultVisibilityTimeout,
				},
			},
			args: args{
				meta: map[string]string{},
			},
			want: &subscription{
				queueAttributes:   &QueueAttributes{},
				pollingInterval:   0,
				visibilityTimeout: 0,
				maxNumberOfJobs:   0,
			},
		},
		{
			name: "invalid value",
			fields: fields{
				queueAttributes: &QueueAttributes{
					VisibilityTimeout: defaultVisibilityTimeout,
				},
			},
			args: args{
				meta: map[string]string{
					"PollingInterval":   "foo",
					"VisibilityTimeout": "bar",
					"MaxNumberOfJobs":   "baz",
				},
			},
			want: &subscription{
				queueAttributes:   &QueueAttributes{},
				pollingInterval:   0,
				visibilityTimeout: 0,
				maxNumberOfJobs:   0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &subscription{
				queueAttributes: tt.fields.queueAttributes,
			}
			got.SetMetadata(tt.args.meta)
			if !reflect.DeepEqual(got.pollingInterval, tt.want.pollingInterval) {
				t.Errorf("pollingInterval = %v, want %v", got.pollingInterval, tt.want.pollingInterval)
			}
			if !reflect.DeepEqual(got.visibilityTimeout, tt.want.visibilityTimeout) {
				t.Errorf("visibilityTimeout = %v, want %v", got.visibilityTimeout, tt.want.visibilityTimeout)
			}
			if !reflect.DeepEqual(got.maxNumberOfJobs, tt.want.maxNumberOfJobs) {
				t.Errorf("maxNumberOfJobs = %v, want %v", got.maxNumberOfJobs, tt.want.maxNumberOfJobs)
			}
		})
	}
}

func TestSubscription_Active(t *testing.T) {
	type fields struct {
		state int32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "active",
			fields: fields{
				state: subStateActive,
			},
			want: true,
		},
		{
			name: "no active",
			fields: fields{
				state: subStateClosing,
			},
			want: false,
		},
		{
			name: "no active",
			fields: fields{
				state: subStateClosed,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &subscription{
				state: tt.fields.state,
			}
			if got := s.Active(); got != tt.want {
				t.Errorf("subscription.Active() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubscription_UnSubscribe(t *testing.T) {
	type fields struct {
		state int32
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				state: subStateActive,
			},
			wantErr: false,
		},
		{
			name: "fail",
			fields: fields{
				state: subStateClosing,
			},
			wantErr: true,
		},
		{
			name: "fail",
			fields: fields{
				state: subStateClosed,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &subscription{
				state: tt.fields.state,
			}
			if err := s.UnSubscribe(); (err != nil) != tt.wantErr {
				t.Errorf("subscription.UnSubscribe() error = %v, wantClosedChan %v", err, tt.wantErr)
			}
		})
	}
}

func Test_subscription_writeChan(t *testing.T) {
	type fields struct {
		queueAttributes *QueueAttributes
		conn            jobworker.Connector
		grabber         grabber
		state           int32
	}
	type args struct {
		ch chan *jobworker.Job
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantJobSize    int
		wantClosedChan bool
	}{
		{
			name: "normal case",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				conn:            &jobworker.ConnectorMock{},
				grabber: &grabberMock{
					GrabJobsFunc: func(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error) {
						time.Sleep(time.Second / 3)
						return &GrabJobsOutput{
							Jobs: []*jobworker.Job{
								{}, {}, {},
							},
						}, nil
					},
				},
				state: subStateActive,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    3,
			wantClosedChan: false,
		},
		{
			name: "state closing",
			fields: fields{
				state: subStateClosing,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    0,
			wantClosedChan: true,
		},
		{
			name: "state closed",
			fields: fields{
				state: subStateClosed,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    0,
			wantClosedChan: true,
		},
		{
			name: "state closed",
			fields: fields{
				state: subStateClosed,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    0,
			wantClosedChan: true,
		},
		{
			name: "grabJob func return error",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				conn:            &jobworker.ConnectorMock{},
				grabber: &grabberMock{
					GrabJobsFunc: func(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error) {
						time.Sleep(time.Second / 3)
						return nil, errors.New("test")
					},
				},
				state: subStateActive,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    0,
			wantClosedChan: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &subscription{
				queueAttributes: tt.fields.queueAttributes,
				conn:            tt.fields.conn,
				grabber:         tt.fields.grabber,
				state:           tt.fields.state,
			}
			go s.writeChan(tt.args.ch)
			time.Sleep(time.Second / 2)
			if tt.wantClosedChan {
				if _, ok := <-tt.args.ch; ok {
					t.Errorf("subscription.writeMessageChan() result = %v, wantClosedChan %v", ok, tt.wantClosedChan)
				}
				return
			}
			if len(tt.args.ch) != tt.wantJobSize {
				t.Errorf("subscription.writeMessageChan() size = %v, wantSize %v", len(tt.args.ch), tt.wantJobSize)
			}
		})
	}
}

func Test_subscription_readChan(t *testing.T) {
	type fields struct {
		queueAttributes *QueueAttributes
		queue           chan *jobworker.Job
		state           int32
	}
	type args struct {
		ch chan *jobworker.Job
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantJobSize    int
		wantClosedChan bool
	}{
		{
			name: "normal case",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				queue:           make(chan *jobworker.Job),
				state:           subStateActive,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    1,
			wantClosedChan: false,
		},
		{
			name: "closed job chan and state is active",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				queue:           make(chan *jobworker.Job),
				state:           subStateActive,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    1,
			wantClosedChan: true,
		},
		{
			name: "closed job chan and state is closing",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				queue:           make(chan *jobworker.Job),
				state:           subStateClosing,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    1,
			wantClosedChan: true,
		},
		{
			name: "closed job chan and state is closed",
			fields: fields{
				queueAttributes: &QueueAttributes{},
				queue: func() chan *jobworker.Job {
					ch := make(chan *jobworker.Job)
					close(ch)
					return ch
				}(),
				state: subStateClosed,
			},
			args: args{
				ch: make(chan *jobworker.Job, 10),
			},
			wantJobSize:    1,
			wantClosedChan: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &subscription{
				queueAttributes: tt.fields.queueAttributes,
				queue:           tt.fields.queue,
				state:           tt.fields.state,
			}
			go s.readChan(tt.args.ch)
			if tt.wantClosedChan {
				close(tt.args.ch)
				if _, ok := <-s.queue; ok {
					t.Errorf("subscription.readMessageChan() result = %v, wantClosedChan %v", ok, tt.wantClosedChan)
				}
				return
			}

			for i := 0; i < tt.wantJobSize; i++ {
				tt.args.ch <- &jobworker.Job{}
			}
			if len(tt.args.ch) != tt.wantJobSize {
				t.Errorf("subscription.readMessageChan() size = %v, wantSize %v", len(tt.args.ch), tt.wantJobSize)
			}
		})
	}
}
