package env

import "os"

var envVars = []string{
	"PARALLEL", "CONCURRENCY", "USE_BUFFERED_STREAM", "CONCURRENCY_LIMIT",

	"BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN", "ENDPOINT", "REGION", "DEFAULT_REGION",

	"AWS_BUCKET", "AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ENDPOINT", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",

	"COMPRESSION", "FILE_MAX_ROWS", "SAMPLE_SIZE",

	"KEY_FILE", "KEY_BODY", "CRED_API_KEY",

	"GC_BUCKET", "GOOGLE_APPLICATION_CREDENTIALS", "GSHEETS_CRED_FILE",
	"GC_KEY_BODY", "GC_CRED_API_KEY",

	"ACCOUNT", "CONTAINER", "SAS_SVC_URL", "CONN_STR",

	"AZURE_ACCOUNT", "AZURE_KEY", "AZURE_CONTAINER", "AZURE_SAS_SVC_URL",
	"AZURE_CONN_STR",

	"SSH_TUNNEL", "SSH_PRIVATE_KEY", "SSH_PUBLIC_KEY",

	"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM_EMAIL", "SMTP_REPLY_EMAIL",

	"HTTP_USER", "HTTP_PASSWORD", "GSHEET_CLIENT_JSON_BODY",
	"GSHEET_SHEET_NAME", "GSHEET_MODE",

	"DIGITALOCEAN_ACCESS_TOKEN", "GITHUB_ACCESS_TOKEN",
	"SURVEYMONKEY_ACCESS_TOKEN",

	"SEND_ANON_USAGE", "DBIO_HOME",
}

// Vars are the variables we are using
func Vars() (vars map[string]string) {
	vars = map[string]string{}
	// get default from environment
	for _, k := range envVars {
		val := os.Getenv(k)
		if vars[k] == "" && val != "" {
			vars[k] = val
		}
	}

	// default as true
	for _, k := range []string{} {
		if vars[k] == "" {
			vars[k] = "true"
		}
	}

	if vars["SAMPLE_SIZE"] == "" {
		vars["SAMPLE_SIZE"] = "900"
	}

	return
}
