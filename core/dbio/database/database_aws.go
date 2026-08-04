package database

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/flarco/g"
)

// loadAWSCredentialsFromChain loads AWS credentials from the default credential chain
// (environment variables, shared config profiles, IAM roles, etc.) and populates the
// connection properties so they can be used by the database or filesystem clients.
func loadAWSCredentialsFromChain(conn Connection) error {
	g.Debug("Loading AWS credentials from default credential chain")

	ctx := context.Background()
	if conn.Context() != nil && conn.Context().Ctx != nil {
		ctx = conn.Context().Ctx
	}

	configOptions := []func(*config.LoadOptions) error{}
	if profile := conn.GetProp("AWS_PROFILE", "PROFILE"); profile != "" {
		configOptions = append(configOptions, config.WithSharedConfigProfile(profile))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return g.Error(err, "Failed to load AWS configuration from credential chain")
	}

	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return g.Error(err, "Failed to retrieve AWS credentials from credential chain")
	}

	conn.SetProp("AWS_ACCESS_KEY_ID", creds.AccessKeyID)
	conn.SetProp("AWS_SECRET_ACCESS_KEY", creds.SecretAccessKey)
	if creds.SessionToken != "" {
		conn.SetProp("AWS_SESSION_TOKEN", creds.SessionToken)
	}

	// Set region if not already set
	if conn.GetProp("AWS_REGION", "AWS_DEFAULT_REGION", "REGION", "DEFAULT_REGION") == "" && cfg.Region != "" {
		conn.SetProp("AWS_REGION", cfg.Region)
	}

	g.Debug("Successfully loaded AWS credentials from credential chain")
	return nil
}
