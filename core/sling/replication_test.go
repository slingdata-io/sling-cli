package sling

import (
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestReplicationConfig(t *testing.T) {
	yaml := `
source: BIGQUERY
target: AWS_S3
defaults:
	object: s3://{target_bucket}/{source_name}/{stream_schema}/{stream_table}/{run_timestamp}.csv
	mode: full-refresh
  source_options:
    flatten: true
streams:
	public.*:
	`
	yaml = strings.ReplaceAll(yaml, "\t", "  ")
	replication, err := UnmarshalReplication(yaml)
	assert.NoError(t, err)
	if assert.NotEmpty(t, replication.Defaults.SourceOptions) {
		assert.True(t, *replication.Defaults.SourceOptions.Flatten)
		g.PP(replication.Defaults.SourceOptions)
	}

	err = replication.ProcessWildcards()
	assert.NoError(t, err)

	g.PP(replication)
}
