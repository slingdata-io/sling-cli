package secrets

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func init() {
	Register("awssecrets", newAWSSecrets, "aws_secrets_manager")
	Register("awsssm", newAWSSSM, "aws_ssm")
}

// awsLoader builds one aws.Config per region, from the provider config.
type awsLoader struct {
	cfg ProviderConfig

	mu      sync.Mutex
	configs map[string]aws.Config
}

func newAWSLoader(cfg ProviderConfig) *awsLoader {
	return &awsLoader{cfg: cfg, configs: map[string]aws.Config{}}
}

// region is the ref param, else the region of an ARN path, else the config
// key, else the AWS env vars.
func (l *awsLoader) region(ref Ref) string {
	if r := ref.Params.Get("region"); r != "" {
		return r
	}
	// arn:<partition>:<service>:<region>:<account>:<resource>
	if parts := strings.SplitN(ref.Path, ":", 5); len(parts) == 5 && parts[0] == "arn" && parts[3] != "" {
		return parts[3]
	}
	return l.cfg.Get("region", "AWS_REGION", "AWS_DEFAULT_REGION")
}

// endpoint is a custom service endpoint (for tests or VPC endpoints).
func (l *awsLoader) endpoint() *string {
	if e := l.cfg.Get("endpoint"); e != "" {
		return aws.String(e)
	}
	return nil
}

func (l *awsLoader) load(ctx context.Context, region string) (aws.Config, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c, ok := l.configs[region]; ok {
		return c, nil
	}

	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	if profile := l.cfg.Get("profile"); profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	}
	c, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return c, fmt.Errorf("could not load AWS config: %w", err)
	}
	if c.Region == "" {
		return c, fmt.Errorf("no AWS region. Set `region` on the provider, ?region= on the reference, or AWS_REGION")
	}

	if roleArn := l.cfg.Get("role_arn"); roleArn != "" {
		externalID := l.cfg.Get("external_id")
		provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(c), roleArn, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = "sling-secrets"
			if externalID != "" {
				o.ExternalID = aws.String(externalID)
			}
		})
		c.Credentials = aws.NewCredentialsCache(provider)
	}

	l.configs[region] = c
	return c, nil
}

// awsSecretsProvider reads AWS Secrets Manager.
// `ref+awssecrets://prod/postgres?version_stage=AWSCURRENT`
type awsSecretsProvider struct {
	loader *awsLoader

	mu      sync.Mutex
	clients map[string]*secretsmanager.Client
}

func newAWSSecrets(cfg ProviderConfig) (Provider, error) {
	return &awsSecretsProvider{loader: newAWSLoader(cfg), clients: map[string]*secretsmanager.Client{}}, nil
}

func (p *awsSecretsProvider) client(ctx context.Context, region string) (*secretsmanager.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[region]; ok {
		return c, nil
	}
	awsCfg, err := p.loader.load(ctx, region)
	if err != nil {
		return nil, err
	}
	endpoint := p.loader.endpoint()
	c := secretsmanager.NewFromConfig(awsCfg, func(o *secretsmanager.Options) {
		if endpoint != nil {
			o.BaseEndpoint = endpoint
		}
	})
	p.clients[region] = c
	return c, nil
}

func (p *awsSecretsProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	c, err := p.client(ctx, p.loader.region(ref))
	if err != nil {
		return nil, err
	}
	in := &secretsmanager.GetSecretValueInput{SecretId: aws.String(ref.Path)}
	if v := ref.Params.Get("version_id"); v != "" {
		in.VersionId = aws.String(v)
	}
	if v := ref.Params.Get("version_stage"); v != "" {
		in.VersionStage = aws.String(v)
	}
	out, err := c.GetSecretValue(ctx, in)
	if err != nil {
		return nil, err
	}
	if out.SecretString != nil {
		return []byte(*out.SecretString), nil
	}
	return out.SecretBinary, nil
}

// awsSSMProvider reads AWS SSM Parameter Store, with decryption.
// `ref+awsssm://prod/pg/password` reads the parameter `/prod/pg/password`.
type awsSSMProvider struct {
	loader *awsLoader

	mu      sync.Mutex
	clients map[string]*ssm.Client
}

func newAWSSSM(cfg ProviderConfig) (Provider, error) {
	return &awsSSMProvider{loader: newAWSLoader(cfg), clients: map[string]*ssm.Client{}}, nil
}

func (p *awsSSMProvider) client(ctx context.Context, region string) (*ssm.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[region]; ok {
		return c, nil
	}
	awsCfg, err := p.loader.load(ctx, region)
	if err != nil {
		return nil, err
	}
	endpoint := p.loader.endpoint()
	c := ssm.NewFromConfig(awsCfg, func(o *ssm.Options) {
		if endpoint != nil {
			o.BaseEndpoint = endpoint
		}
	})
	p.clients[region] = c
	return c, nil
}

func (p *awsSSMProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	c, err := p.client(ctx, p.loader.region(ref))
	if err != nil {
		return nil, err
	}
	name := ref.Path
	if !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "arn:") {
		name = "/" + name
	}
	out, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name), WithDecryption: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	if out.Parameter == nil || out.Parameter.Value == nil {
		return nil, fmt.Errorf("parameter %s has no value", name)
	}
	return []byte(*out.Parameter.Value), nil
}
