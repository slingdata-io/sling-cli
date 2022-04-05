package sling

type Project struct {
	Config      ProjectConfig
	TaskConfigs map[string]Config
}

func LoadProject(path string) {}

type ProjectConfig struct {
	Project          string                        `json:"project" yaml:"project"`
	TaskPaths        []string                      `json:"task-paths" yaml:"task-paths"`
	Defaults         map[string]interface{}        `json:"defaults" yaml:"defaults"`
	NotificationTags map[string]NotificationConfig `json:"notification_tags" yaml:"notification_tags"`
}

type NotificationConfig struct {
	Name        string   `json:"name"`
	Emails      []string `json:"emails"`
	Slack       bool     `json:"slack"`
	MsTeams     bool     `json:"msteams"`
	WebhookURLs []string `json:"webhook_urls"` // urls
	OnSuccess   bool     `json:"on_success"`
	OnFailure   bool     `json:"on_failure"`
	OnLinger    bool     `json:"on_linger"`
	OnEmpty     bool     `json:"on_empty"`
}
