package api

import (
	"net/http"
	"strings"

	h "github.com/flarco/g"
)

// GithubAPI is github's api
// https://developer.github.com/v3/
type GithubAPI struct {
	BaseAPI
}

// Init initializes
func (api *GithubAPI) Init() (err error) {
	api.Provider = Github
	api.BaseURL = "https://api.github.com"
	api.Key = api.properties["GITHUB_ACCESS_TOKEN"]

	if api.Key == "" {
		err = h.Error("did not provide GITHUB_ACCESS_TOKEN")
		return
	}

	api.DefHeaders = map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "token " + api.Key,
	}
	api.DefParams = map[string]string{
		"per_page": "100",
	}
	api.FlattenNested = true

	// https://developer.github.com/v3/#pagination
	api.BaseAPI.getNextURL = func(resp *http.Response) (url string) {
		link := resp.Header.Get("Link")
		if link == "" {
			return
		}
		linkArr := strings.Split(link, ",")
		if len(linkArr) == 0 {
			return
		}
		i := -1
		for j := range linkArr {
			if strings.Contains(linkArr[j], `"next"`) {
				i = j
			}
		}
		if i == -1 {
			return
		}

		linkArr = strings.Split(linkArr[i], ";")
		if len(linkArr) == 0 {
			return
		}
		url = strings.ReplaceAll(linkArr[0], ">", "")
		url = strings.TrimSpace(strings.ReplaceAll(url, "<", ""))
		if !strings.Contains(url, "per_page") {
			url = url + "&per_page={per_page}"
		}
		return
	}

	return api.BaseAPI.Init()
}
