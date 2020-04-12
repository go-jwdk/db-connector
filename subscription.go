package db

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-jwdk/jobworker"
)

const (
	subStateActive  = int32(0)
	subStateClosing = int32(1)
	subStateClosed  = int32(2)

	subMetadataKeyPollingInterval   = "PollingInterval"
	subMetadataKeyVisibilityTimeout = "VisibilityTimeout"
	subMetadataKeyMaxNumberOfJobs   = "MaxNumberOfJobs"

	defaultPollingInterval = 3 * time.Second
	defaultMaxNumberOfJobs = int64(1)
)

func newSubscription(queueAttributes *QueueAttributes,
	conn jobworker.Connector,
	grabber grabber,
	meta map[string]string) *subscription {

	pollingInterval := defaultPollingInterval
	if v := meta[subMetadataKeyPollingInterval]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			pollingInterval = time.Duration(i) * time.Second
		}
	}

	visibilityTimeout := queueAttributes.VisibilityTimeout
	if v := meta[subMetadataKeyVisibilityTimeout]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			visibilityTimeout = i
		}
	}

	maxNumberOfMessages := defaultMaxNumberOfJobs
	if v := meta[subMetadataKeyMaxNumberOfJobs]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			maxNumberOfMessages = i
		}
	}

	return &subscription{
		pollingInterval:   pollingInterval,
		queueAttributes:   queueAttributes,
		conn:              conn,
		grabber:           grabber,
		visibilityTimeout: visibilityTimeout,
		maxNumberOfJobs:   maxNumberOfMessages,
		queue:             make(chan *jobworker.Job),
	}
}

type subscription struct {
	queueAttributes *QueueAttributes
	conn            jobworker.Connector

	pollingInterval   time.Duration
	visibilityTimeout int64
	maxNumberOfJobs   int64

	grabber grabber
	queue   chan *jobworker.Job
	state   int32
}

func (s *subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

func (s *subscription) Queue() chan *jobworker.Job {
	return s.queue
}

var ErrCompletedSubscription = errors.New("subscription is unsubscribed")

func (s *subscription) UnSubscribe() error {
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}
	return nil
}

func (s *subscription) Start() {
	ch := make(chan *jobworker.Job)
	go s.writeChan(ch)
	s.readChan(ch)
}

func (s *subscription) writeChan(ch chan *jobworker.Job) {
	for {
		if atomic.LoadInt32(&s.state) != subStateActive {
			close(ch)
			return
		}
		ctx := context.Background()
		grabbed, err := s.grabber.GrabJobs(ctx, &GrabJobsInput{
			QueueName:         s.queueAttributes.Name,
			MaxNumberOfJobs:   s.maxNumberOfJobs,
			VisibilityTimeout: s.visibilityTimeout,
		})
		if err != nil {
			close(ch)
			return
		}
		if len(grabbed.Jobs) == 0 {
			time.Sleep(s.pollingInterval)
			continue
		}
		for _, job := range grabbed.Jobs {
			ch <- job
		}
	}
}

func (s *subscription) readChan(ch chan *jobworker.Job) {
	for {
		job, ok := <-ch
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeQueue()
			}
			return
		}
		s.queue <- job
	}
}

func (s *subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}

type grabber interface {
	GrabJobs(ctx context.Context, input *GrabJobsInput) (*GrabJobsOutput, error)
}
