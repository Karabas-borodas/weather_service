package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
)

const httpPort = ":3000"

func initJobs(scheduler gocron.Scheduler) ([]gocron.Job, error) {

	j, err := scheduler.NewJob(
		gocron.DurationJob(
			1*time.Second,
		),
		gocron.NewTask(
			func(a string, b int) {
				fmt.Println(" crore RUN ")
			},
			"hello",
			1,
		),
	)
	if err != nil {
		return nil, err
	}
	return []gocron.Job{j}, nil
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	var wg sync.WaitGroup

	var reqCount atomic.Uint64
	// Register cron task once at startup.
	// gocron.Every(1).Second().Do(func() {
	// 	fmt.Println("cron started")
	// })

	// Start scheduler once in background.
	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}
	jobs, err := initJobs(s)
	if err != nil {
		panic(err)
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		fmt.Printf("sturting jobs %s\n", jobs[0].ID())
		s.Start()
	}()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		n := reqCount.Add(1)
		fmt.Printf("request #%d from %s\n", n, r.RemoteAddr)
		fmt.Printf("request #%d from %s\n", n, r.Body)
		_, err := w.Write([]byte(fmt.Sprintf("welcome, request #%d\n", n)))
		if err != nil {
			log.Print(err)
		}
	})

	go func() {
		defer wg.Done()
		fmt.Printf("server started on %s\n", httpPort)
		if err := http.ListenAndServe(httpPort, r); err != nil {
			panic(err)
		}
	}()

	wg.Wait()
}
