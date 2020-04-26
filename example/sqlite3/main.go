package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-jwdk/db-connector/config"

	uuid "github.com/satori/go.uuid"

	dbconn "github.com/go-jwdk/db-connector"
	"github.com/go-jwdk/db-connector/sqlite3"
	"github.com/go-jwdk/jobworker"
)

func main() {

	dsn := os.Getenv("SQLITE3_DSN")
	connMaxLifetime := time.Minute
	numMaxRetries := 3

	s := &config.Config{
		DSN:             dsn,
		MaxOpenConns:    3,
		MaxIdleConns:    3,
		ConnMaxLifetime: &connMaxLifetime,
		NumMaxRetries:   &numMaxRetries,
	}

	conn, err := sqlite3.Open(s)
	if err != nil {
		fmt.Println("open conn error:", err)
		return
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Println("close conn error:", err)
		}
	}()

	_, err = conn.CreateQueue(context.Background(), &dbconn.CreateQueueInput{
		Name: "test",
	})

	go func() {
		for {
			_, err := conn.Enqueue(context.Background(), &jobworker.EnqueueInput{
				Queue:   "test",
				Content: "hello: " + uuid.NewV4().String(),
			})
			if err != nil {
				fmt.Println("could not enqueue a job", err)
			}

			time.Sleep(3 * time.Second)
		}
		for {
			_, err := conn.EnqueueBatch(context.Background(), &jobworker.EnqueueBatchInput{
				Queue: "test",
				Entries: []*jobworker.EnqueueBatchEntry{
					{
						ID:      "foo",
						Content: "foo-content",
					},
					{
						ID:      "bar",
						Content: "bar-content",
					},
					{
						ID:      "baz",
						Content: "baz-content",
					},
				},
			})
			if err != nil {
				fmt.Println("could not enqueue a job", err)
			}

			time.Sleep(3 * time.Second)
		}
	}()

	done := make(chan struct{})

	go func() {
		out, err := conn.Subscribe(context.Background(), &jobworker.SubscribeInput{Queue: "test"})
		if err != nil {
			fmt.Println("subscribe error:", err)
		}
		for job := range out.Subscription.Queue() {
			printJob(job)
			_, err := conn.CompleteJob(context.Background(), &jobworker.CompleteJobInput{
				Job: job,
			})
			if err != nil {
				fmt.Println("complete jobs error:", err)
			}
		}
		close(done)
	}()

	<-done

}

func printJob(job *jobworker.Job) {
	fmt.Println("# ----------")
	for k, v := range job.Metadata {
		fmt.Println(k, ":", v)
	}
	fmt.Println("# ----------")
	fmt.Println("Content :", job.Content)
	fmt.Println("# ----------")
	fmt.Println("Queue :", job.QueueName)
	fmt.Println("# ----------")
}
