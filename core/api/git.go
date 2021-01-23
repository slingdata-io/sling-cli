package api

// GitAPI is for interacting with a Git Repo
// for cloning, fetching
type GitAPI struct {
	BaseAPI
}

// Init initializes
func (api *GitAPI) Init() (err error) {
	api.Provider = Git
	api.BaseURL = api.GetProp("url")
	api.User = api.GetProp("GIT_USER")
	api.Key = api.GetProp("GIT_PASSWORD")

	return api.BaseAPI.Init()
}
