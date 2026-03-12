package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Karabas-borodas/weather_service.git/cmd/internal/client/http/geocoding"
	"github.com/Karabas-borodas/weather_service.git/cmd/internal/client/http/open_meteo"
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

	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	geocodingClient := geocoding.NewClient(httpClient)

	var wg sync.WaitGroup

	var reqCount atomic.Uint64

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

	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		city := chi.URLParam(r, "city")
		fmt.Printf("var SITY %s", city)
		geoResp, err := geocodingClient.GetCoordinate(city)
		if err != nil {
			log.Print(err)
		}
		openMeteoClient := openmeteo.NewClient(httpClient)
		if err != nil {
			log.Print(err)
		}
		// resp, err := GetTemperature(resp)

		res, err := openMeteoClient.GetTemperature(geoResp[0].Latitude, geoResp[0].Longitude)
		if err != nil {
			log.Print(err)
		}
		n := reqCount.Add(1)
		// fmt.Printf("request #%d from %s\n", n, r.RemoteAddr)
		fmt.Printf("request #%d from %s\n", n, r.Body)
		row, err := json.Marshal(res.Current.Temperature2M)
		if err != nil {
			log.Print(err)
		}
		_, err = w.Write(row)
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
