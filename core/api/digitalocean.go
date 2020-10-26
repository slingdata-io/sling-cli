package api

import (
	"github.com/digitalocean/godo"
	h "github.com/flarco/gutil"
)

// DigitalOceanAPI is DO's api
// https://developers.digitalocean.com/documentation/v2
type DigitalOceanAPI struct {
	BaseAPI
	client *godo.Client
}

// Init initializes
func (api *DigitalOceanAPI) Init() (err error) {
	api.Provider = DigitalOcean
	api.BaseURL = "https://api.digitalocean.com"
	api.Key = api.properties["DIGITALOCEAN_ACCESS_TOKEN"]

	if api.Key == "" {
		err = h.Error("did not provide DIGITALOCEAN_ACCESS_TOKEN")
		return
	}

	api.DefHeaders = map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + api.Key,
	}

	api.client = godo.NewFromToken(api.Key)

	return api.BaseAPI.Init()
}
