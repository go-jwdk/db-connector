package internal

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-jwdk/db-connector"

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

func NewSubscription(attributes *db.QueueAttributes,
	conn jobworker.Connector,
	grabber Grabber,
	meta map[string]string) *Subscription {
	pollingInterval, visibilityTimeout, maxNumberOfMessages := extractMetadata(meta, attributes)
	return &Subscription{
		pollingInterval:   pollingInterval,
		queueAttributes:   attributes,
		conn:              conn,
		grabber:           grabber,
		visibilityTimeout: visibilityTimeout,
		maxNumberOfJobs:   maxNumberOfMessages,
		queue:             make(chan *jobworker.Job),
	}
}

func extractMetadata(meta map[string]string, queueAttr *db.QueueAttributes) (
	pollingInterval time.Duration,
	visibilityTimeout int64,
	maxNumberOfMessages int64,
) {

	pollingInterval = defaultPollingInterval
	if v := meta[subMetadataKeyPollingInterval]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			pollingInterval = time.Duration(i) * time.Second
		}
	}

	visibilityTimeout = queueAttr.VisibilityTimeout
	if v := meta[subMetadataKeyVisibilityTimeout]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			visibilityTimeout = i
		}
	}

	maxNumberOfMessages = defaultMaxNumberOfJobs
	if v := meta[subMetadataKeyMaxNumberOfJobs]; v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			maxNumberOfMessages = i
		}
	}

	return
}

type Subscription struct {
	queueAttributes *db.QueueAttributes
	conn            jobworker.Connector

	pollingInterval   time.Duration
	visibilityTimeout int64
	maxNumberOfJobs   int64

	grabber Grabber
	queue   chan *jobworker.Job
	state   int32
}

func (s *Subscription) Active() bool {
	return atomic.LoadInt32(&s.state) == subStateActive
}

func (s *Subscription) Queue() chan *jobworker.Job {
	return s.queue
}

var ErrCompletedSubscription = errors.New("subscription is unsubscribed")

func (s *Subscription) UnSubscribe() error {
	if !atomic.CompareAndSwapInt32(&s.state, subStateActive, subStateClosing) {
		return ErrCompletedSubscription
	}
	return nil
}

func (s *Subscription) Start() {
	msgCh := make(chan *Job)
	go s.writeChan(msgCh)
	s.readChan(msgCh)
}

func (s *Subscription) writeChan(ch chan *Job) {
	for {
		if atomic.LoadInt32(&s.state) != subStateActive {
			close(ch)
			return
		}
		ctx := context.Background()
		grabbedJobs, err := s.grabber.GrabJobs(ctx, s.queueAttributes, s.maxNumberOfJobs, s.visibilityTimeout)
		if err != nil {
			close(ch)
			return
		}
		if len(grabbedJobs) == 0 {
			time.Sleep(s.pollingInterval)
			continue
		}
		for _, job := range grabbedJobs {
			ch <- job
		}
	}
}

func (s *Subscription) readChan(ch chan *Job) {
	for {
		job, ok := <-ch
		if !ok {
			state := atomic.LoadInt32(&s.state)
			if state == subStateActive || state == subStateClosing {
				s.closeQueue()
			}
			return
		}
		s.queue <- NewJob(s.queueAttributes.Name, job, s.conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}

type Grabber interface {
	GrabJobs(ctx context.Context, queueAttr *db.QueueAttributes, maxNumberOfJobs, visibilityTimeout int64) ([]*Job, error)
}
