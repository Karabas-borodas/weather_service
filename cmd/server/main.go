package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	// "sync/atomic"
	"time"

	"github.com/Karabas-borodas/weather_service.git/internal/client/http/geocoding"
	"github.com/Karabas-borodas/weather_service.git/internal/client/http/open_meteo"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
)

const httpPort = ":3000"
const city = "Moscow"

type Reading struct {
	Timestamp   time.Time
	Temperatuer float64
}
type Storage struct {
	data map[string][]Reading
	mu   sync.RWMutex
}

func initJobs(scheduler gocron.Scheduler, storage *Storage) ([]gocron.Job, error) {

	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	geoResp, err := geocodingClient.GetCoordinate(city)
	if err != nil {
		log.Print(err)
	}
	j, err := scheduler.NewJob(
		gocron.DurationJob(
			1*time.Second,
		),
		gocron.NewTask(
			func() {
				openMeteoClient := openmeteo.NewClient(httpClient)
				if err != nil {
					log.Print(err)
				}
				// resp, err := GetTemperature(resp)

				openMeteoRes, err := openMeteoClient.GetTemperature(geoResp[0].Latitude, geoResp[0].Longitude)
				if err != nil {
					log.Print(err)
					return
				}
				storage.mu.Lock()
				defer storage.mu.Unlock()
				timeStor, err := time.Parse("2006-01-02T15:04", openMeteoRes.Current.Time)
				if err != nil {
					log.Print(err)
					return
				}
				if len(storage.data[city]) == 0 {
					storage.data[city] = append(storage.data[city], Reading{
						Timestamp:   timeStor,
						Temperatuer: openMeteoRes.Current.Temperature2M,
					})
				} else {
					storage.data[city][0] = Reading{
						Timestamp:   timeStor,
						Temperatuer: openMeteoRes.Current.Temperature2M,
					}
				}
			},
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

	storage := Storage{
		data: make(map[string][]Reading),
	}
	var wg sync.WaitGroup

	// var reqCount atomic.Uint64

	// Start scheduler once in background.
	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}
	jobs, err := initJobs(s, &storage)
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
		cityName := chi.URLParam(r, "city")
		fmt.Printf("var SITY %s", cityName)

		storage.mu.RLock()
		defer storage.mu.RUnlock()

		resding, ok := storage.data[cityName]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
			return
		}
		// n := reqCount.Add(1)
		// fmt.Printf("request #%d from %s\n", n, r.RemoteAddr)
		// fmt.Printf("request #%d from %s\n", n, r.Body)
		row, err := json.Marshal(resding)
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
