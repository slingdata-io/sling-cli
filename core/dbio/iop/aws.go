package iop

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

func MakeAwsConfig(ctx context.Context, props map[string]string) (cfg aws.Config, err error) {

	getProp := func(key ...string) string {
		for _, k := range key {
			if val, ok := props[strings.ToLower(k)]; ok {
				return val
			}
		}
		return ""
	}

	// Get AWS credentials from connection properties
	awsAccessKeyID := getProp("aws_access_key_id", "access_key_id")
	awsSecretAccessKey := getProp("aws_secret_access_key", "secret_access_key")
	awsSessionToken := getProp("aws_session_token", "session_token")
	awsRegion := getProp("aws_region", "region")
	awsProfile := getProp("aws_profile", "profile")

	if awsRegion == "" {
		return cfg, g.Error("AWS region not specified")
	}

	// Configure options based on authentication method
	var configOptions []func(*awsconfig.LoadOptions) error

	// Add region to config options
	configOptions = append(configOptions, awsconfig.WithRegion(awsRegion))

	// Set timeout if provided
	if timeOut := cast.ToInt(getProp("timeout")); timeOut > 0 {
		httpClient := &http.Client{
			Timeout: time.Duration(timeOut) * time.Second,
		}
		configOptions = append(configOptions, awsconfig.WithHTTPClient(httpClient))
	}

	// Set credentials if provided
	if awsAccessKeyID != "" && awsSecretAccessKey != "" {
		credProvider := credentials.NewStaticCredentialsProvider(
			awsAccessKeyID,
			awsSecretAccessKey,
			awsSessionToken,
		)
		configOptions = append(configOptions, awsconfig.WithCredentialsProvider(credProvider))

		// Load config with static credentials
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return cfg, g.Error(err, "Failed to create AWS config with static credentials")
		}
	} else if awsProfile != "" {
		g.Debug("using AWS profile=%s region=%s", awsProfile, awsRegion)

		// Use specified profile from AWS credentials file
		configOptions = append(configOptions, awsconfig.WithSharedConfigProfile(awsProfile))

		// Load config with profile
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return cfg, g.Error(err, "Failed to create AWS config with profile %s", awsProfile)
		}
	} else {
		g.Debug("using default AWS credential chain")
		// Use default credential chain (env vars, IAM role, credential file, etc.)

		// Load config with default credential chain
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return cfg, g.Error(err, "Failed to create AWS config with default credentials")
		}
	}

	return cfg, nil
}
