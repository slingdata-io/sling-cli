package sling

import (
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
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

func TestReplicationWildcards(t *testing.T) {

	type test struct {
		name     string
		connName string
		streams  []string
		expected []string
	}

	tests := []test{
		{
			connName: "sftp",
			streams: []string{
				"/_/analytics/sling/*.yaml",
			},
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
				"/_/analytics/sling/clickhouse-hydra.yaml",
				"/_/analytics/sling/clickhouse-motherduck.yaml",
			},
		},
		{
			connName: "sftp",
			streams: []string{
				"/_/analytics/sling/clickhouse-?????.yaml",
			},
			expected: []string{
				"/_/analytics/sling/clickhouse-hydra.yaml",
			},
		},
		{
			connName: "aws_s3",
			streams: []string{
				"sling_test/*",
			},
			expected: []string{
				"sling_test/csv/",
				"sling_test/files/",
			},
		},
		{
			connName: "aws_s3",
			streams: []string{
				"sling_test/**",
			},
			expected: []string{
				"sling_test/csv/part.01.0001.csv",
				"sling_test/csv/part.01.0002.csv",
				"sling_test/csv/part.01.0003.csv",
				"sling_test/csv/part.01.0004.csv",
				"sling_test/csv/part.01.0005.csv",
				"sling_test/csv/part.01.0006.csv",
				"sling_test/csv/part.01.0007.csv",
				"sling_test/csv/part.01.0008.csv",
				"sling_test/csv/part.01.0009.csv",
				"sling_test/csv/part.01.0010.csv",
				"sling_test/csv/part.01.0011.csv",
				"sling_test/files/test1k_s3.csv",
				"sling_test/files/test1k_s3.json",
				"sling_test/files/test1k_s3.parquet",
			},
		},
		{
			connName: "postgres",
			streams: []string{
				"public.test1k_bigquery*",
			},
			expected: []string{
				"\"public\".\"test1k_bigquery_pg\"",
				"\"public\".\"test1k_bigquery_pg_vw\"",
			},
		},
	}

	// Set all the connections
	conns := connection.GetLocalConns()
	connsMap := map[string]connection.Connection{}
	for _, test := range tests {
		if _, ok := connsMap[test.connName]; !ok {
			connsMap[test.connName] = conns.Get(test.connName).Connection
		}
	}

	for _, test := range tests {
		test.name = g.F("%s|%s", test.connName, test.streams)

		t.Run(g.F("%s", test.name), func(t *testing.T) {

			// Set up the ReplicationConfig
			config := ReplicationConfig{
				Source:  test.connName,
				Target:  test.connName,
				Streams: map[string]*ReplicationStreamConfig{},
			}

			// Add streams to config
			for _, streamName := range test.streams {
				config.Streams[streamName] = &ReplicationStreamConfig{}
			}

			// properly generate the config
			config, err := UnmarshalReplication(g.Marshal(config))
			if !assert.NoError(t, err) {
				return
			}

			conn := connsMap[test.connName]
			if conn.Type.IsFile() {
				err := config.ProcessWildcardsFile(conn, test.streams)
				assert.NoError(t, err)
			} else if conn.Type.IsDb() {
				err := config.ProcessWildcardsDatabase(conn, test.streams)
				assert.NoError(t, err)
			} else {
				assert.Fail(t, "Connection type not supported")
			}

			// assert that the streams are correct
			assert.Equal(t, len(test.expected), len(config.Streams))
			for streamName := range config.Streams {
				assert.Contains(t, test.expected, streamName)
			}
		})

	}
}
