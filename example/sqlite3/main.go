package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-jwdk/db-connector/sqlite3"
	"github.com/go-jwdk/jobworker"
	uuid "github.com/satori/go.uuid"
)

func main() {

	dsn := os.Getenv("SQLITE3_DSN")
	connMaxLifetime := time.Minute
	numMaxRetries := 3

	s := &sqlite3.Setting{
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
	}()

	done := make(chan struct{})

	go func() {
		out, err := conn.Subscribe(context.Background(), &jobworker.SubscribeInput{Queue: "test"})
		if err != nil {
			fmt.Println("receive jobs error:", err)
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
