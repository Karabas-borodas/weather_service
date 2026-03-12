package openmeteo

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type client struct {
	httpClient *http.Client
}

func NewClient(httpClient *http.Client) *client {

	return &client{
		httpClient: httpClient,
	}
}

type weatherResponse struct {
	Current struct {
		Time          string  `json:"time"`
		Temperature2M float64 `json:"temperature_2m"`
	} `json:"current"`
}

func (c *client) GetTemperature(lat, long float64) (weatherResponse, error) {
	resp, err := c.httpClient.Get(
		fmt.Sprintf("https://api.open-meteo.com/v1/forecast?latitude=%f&longitude=%f&current=temperature_2m", lat, long))
	if err != nil {
		return weatherResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return weatherResponse{}, fmt.Errorf("status code %d", resp.StatusCode)
	}
	var weatherResp weatherResponse
	err = json.NewDecoder(resp.Body).Decode(&weatherResp)
	if err != nil {
		return weatherResponse{}, err

	}
	return weatherResp, err
}
