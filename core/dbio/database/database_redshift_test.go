package database

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestRedshiftConn(t *testing.T) *RedshiftConn {
	t.Helper()
	conn, err := NewConnContext(
		context.Background(),
		"redshift://testuser:testpass@testhost.example.com:5439/testdb",
	)
	if err != nil {
		t.Fatalf("could not create redshift conn: %s", err)
	}
	rs, ok := conn.(*RedshiftConn)
	if !ok {
		t.Fatalf("expected *RedshiftConn, got %T", conn)
	}
	return rs
}

// ensureAWSCredentials should short-circuit when explicit credentials are provided,
// without attempting to load from the AWS credential chain.
func TestRedshiftEnsureAWSCredentialsExplicit(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")

	ok, err := conn.ensureAWSCredentials()
	assert.NoError(t, err)
	assert.True(t, ok)
}

// ensureAWSCredentials should honor USE_ENVIRONMENT=false and not attempt the chain.
func TestRedshiftEnsureAWSCredentialsOptedOut(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("USE_ENVIRONMENT", "false")

	ok, err := conn.ensureAWSCredentials()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestRedshiftMakeCopyCredentialString(t *testing.T) {
	t.Run("static credentials with session token", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
		conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")
		conn.SetProp("AWS_SESSION_TOKEN", "sessiontoken")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t,
			"credentials 'aws_access_key_id=AKIAEXAMPLE;aws_secret_access_key=secretkey;token=sessiontoken'",
			cred,
		)
	})

	t.Run("iam role arn", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/MyRole")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t,
			"iam_role 'arn:aws:iam::123456789012:role/MyRole'",
			cred,
		)
	})

	t.Run("iam role default", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ROLE_ARN", "default")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t, "iam_role default", cred)
	})
}

// getS3Props should include the region and propagate explicit credentials.
func TestRedshiftGetS3Props(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")
	conn.SetProp("AWS_REGION", "eu-west-1")

	props := conn.getS3Props()
	joined := strings.Join(props, " ")

	assert.Contains(t, joined, "ACCESS_KEY_ID=AKIAEXAMPLE")
	assert.Contains(t, joined, "SECRET_ACCESS_KEY=secretkey")
	assert.Contains(t, joined, "REGION=eu-west-1")
}
