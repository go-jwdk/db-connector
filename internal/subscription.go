package internal

import (
	"context"
	"errors"
	"fmt"
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

func NewSubscription(queueAttr *QueueAttributes,
	conn *Connector, meta map[string]string) *Subscription {
	pollingInterval, visibilityTimeout, maxNumberOfMessages := extractSubMetadata(meta, queueAttr)
	return &Subscription{
		pollingInterval:   pollingInterval,
		queueAttr:         queueAttr,
		conn:              conn,
		visibilityTimeout: visibilityTimeout,
		maxNumberOfJobs:   maxNumberOfMessages,
		queue:             make(chan *jobworker.Job),
	}
}

func extractSubMetadata(meta map[string]string, queueAttr *QueueAttributes) (
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
	queueAttr *QueueAttributes
	conn      *Connector

	pollingInterval   time.Duration
	visibilityTimeout int64
	maxNumberOfJobs   int64

	grabJobs func(ctx context.Context, queueRawName string, maxReceiveCount, maxNumberOfJobs, visibilityTimeout int64, handleDeadJob func(deadJobs []*Job) error) ([]*Job, error)
	queue    chan *jobworker.Job
	state    int32
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
		grabbedJobs, err := s.grabJobs(ctx,
			s.queueAttr.RawName,
			s.queueAttr.MaxReceiveCount, s.maxNumberOfJobs, s.visibilityTimeout,
			func(deadJobs []*Job) error {
				if s.queueAttr.HasDeadLetter() {
					deadLetterQueue, err := s.conn.resolveQueueAttributes(ctx, s.queueAttr.DeadLetterTarget)
					if err != nil {
						return err
					}
					err = s.conn.moveJobBatch(ctx, s.queueAttr, deadLetterQueue, deadJobs)
					if err != nil {
						return fmt.Errorf("could not move job batch: %s", err)
					}
				} else {
					err := s.conn.cleanJobBatch(ctx, s.queueAttr, deadJobs)
					if err != nil {
						return fmt.Errorf("could not clean job batch: %s", err)
					}
				}
				return nil
			})
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
		s.queue <- newJob(s.queueAttr.Name, job, s.conn)
	}
}

func (s *Subscription) closeQueue() {
	atomic.StoreInt32(&s.state, subStateClosed)
	close(s.queue)
}
