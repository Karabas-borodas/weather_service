package geocoding

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type client struct {
	httpClient *http.Client
}
type Responce []struct {
	Name      string  `json:"name"`      // Название города
	Country   string  `json:"country"`   // Название страны
	Latitude  float64 `json:"latitude"`  // Географическая широта
	Longitude float64 `json:"longitude"` // Географическая долгота
}

func NewClient(httpClient *http.Client) *client {

	return &client{
		httpClient: httpClient,
	}
}

func (c *client) GetCoordinate(city string) (Responce, error) {

	resp, err := c.httpClient.Get(
		fmt.Sprintf("https://geocoding-api.open-meteo.com/v1/search?name=%s&count=1&language=ru&format=json", city))
	if err != nil {
		return Responce{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Responce{}, fmt.Errorf("status code %d", resp.StatusCode)
	}
	var geoResp struct {
		Results Responce `json:"results"`
	}
	err = json.NewDecoder(resp.Body).Decode(&geoResp)
	if err != nil {
		return Responce{}, err

	}
	return geoResp.Results, nil
}
