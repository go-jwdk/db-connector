package internal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-jwdk/jobworker"
)

const (
	subStateActive  = 0
	subStateClosing = 1
	subStateClosed  = 2
)

func NewSubscription(interval time.Duration,
	queueAttr *QueueAttribute, maxNumberOfJobs int64, visibilityTimeout *int64, conn *Connector) *Subscription {
	return &Subscription{
		interval:          interval,
		queueAttr:         queueAttr,
		conn:              conn,
		visibilityTimeout: visibilityTimeout,
		maxNumberOfJobs:   maxNumberOfJobs,
		queue:             make(chan *jobworker.Job),
	}
}

type Subscription struct {
	interval          time.Duration
	queueAttr         *QueueAttribute
	maxNumberOfJobs   int64
	visibilityTimeout *int64
	conn              *Connector

	queue chan *jobworker.Job
	state int32
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

func (s *Subscription) ReadLoop() {

	ch := make(chan *Job)

	go func() {
		for {

			if atomic.LoadInt32(&s.state) != subStateActive {
				// TODO logging
				return
			}

			ctx := context.Background()

			visibilityTimeout := s.queueAttr.VisibilityTimeout
			if s.visibilityTimeout != nil {
				visibilityTimeout = *s.visibilityTimeout
			}

			grabbedJobs, err := s.conn.grabJobs(ctx,
				s.queueAttr.RawName,
				s.queueAttr.MaxReceiveCount, s.maxNumberOfJobs, visibilityTimeout,
				func(deadJobs []*Job) error {
					if s.queueAttr.HasDeadLetter() {
						deadLetterQueue, err := s.conn.resolveQueue(ctx, s.queueAttr.DeadLetterTarget)
						if err != nil {
							return err
						}
						err = s.conn.moveJobBatch(ctx, s.queueAttr, deadLetterQueue, deadJobs)
						if err != nil {
							return fmt.Errorf("could not move job batch: %w", err)
						}
					} else {
						err := s.conn.cleanJobBatch(ctx, s.queueAttr, deadJobs)
						if err != nil {
							return fmt.Errorf("could not clean job batch: %w", err)
						}
					}
					return nil
				})
			if err != nil {
				close(ch)
				// TODO logging
				return
			}

			if len(grabbedJobs) == 0 {
				time.Sleep(s.interval)
				continue
			}
			for _, job := range grabbedJobs {
				ch <- job
			}
		}
	}()

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
