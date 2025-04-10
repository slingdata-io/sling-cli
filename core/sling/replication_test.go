package sling

import (
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/spf13/cast"
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
		assert.True(t, cast.ToBool(replication.Defaults.SourceOptions.Flatten))
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
		patterns []string
		expected []string
	}

	tests := []test{
		{
			connName: "sftp",
			patterns: []string{
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
			patterns: []string{
				"/_/analytics/sling/clickhouse-?????.yaml",
			},
			expected: []string{
				"/_/analytics/sling/clickhouse-hydra.yaml",
			},
		},
		{
			connName: "aws_s3",
			patterns: []string{
				"sling_test/*",
			},
			expected: []string{
				"sling_test/csv/",
				"sling_test/files/",
				"sling_test/lineitem_iceberg/",
				"sling_test/delta/",
			},
		},
		{
			connName: "aws_s3",
			patterns: []string{
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
				"sling_test/delta/_delta_log/",
				"sling_test/delta/_delta_log/00000000000000000000.json",
				"sling_test/delta/",
				"sling_test/delta/country=Argentina/",
				"sling_test/delta/country=Argentina/part-00000-8d0390a3-f797-4265-b9c2-da1c941680a3.c000.snappy.parquet",
				"sling_test/delta/country=China/",
				"sling_test/delta/country=China/part-00000-88fba1af-b28d-4303-9c85-9a97be631d40.c000.snappy.parquet",
				"sling_test/delta/country=Germany/",
				"sling_test/delta/country=Germany/part-00000-030076e1-5ec9-47c2-830a-1569f823b6ee.c000.snappy.parquet",
				"sling_test/files/test1k_s3.csv",
				"sling_test/files/test1k_s3.json",
				"sling_test/files/test1k_s3.parquet",
				"sling_test/lineitem_iceberg/",
				"sling_test/lineitem_iceberg/data/",
				"sling_test/lineitem_iceberg/data/00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet",
				"sling_test/lineitem_iceberg/data/00041-414-f3c73457-bbd6-4b92-9c15-17b241171b16-00001.parquet",
				"sling_test/lineitem_iceberg/metadata/",
				"sling_test/lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro",
				"sling_test/lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro",
				"sling_test/lineitem_iceberg/metadata/cf3d0be5-cf70-453d-ad8f-48fdc412e608-m0.avro",
				"sling_test/lineitem_iceberg/metadata/snap-3776207205136740581-1-cf3d0be5-cf70-453d-ad8f-48fdc412e608.avro",
				"sling_test/lineitem_iceberg/metadata/snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro",
				"sling_test/lineitem_iceberg/metadata/v1.metadata.json",
				"sling_test/lineitem_iceberg/metadata/v2.metadata.json",
				"sling_test/lineitem_iceberg/metadata/version-hint.text",
				"sling_test/lineitem_iceberg/README.md",
			},
		},
		{
			connName: "postgres",
			patterns: []string{
				"public.test1k_bigquery*",
			},
			expected: []string{
				"\"public\".\"test1k_bigquery_pg\"",
				"\"public\".\"test1k_bigquery_pg_vw\"",
				"\"public\".\"test1k_bigquery_pg_orig\"",
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
		test.name = g.F("%s|%s", test.connName, test.patterns)

		t.Run(g.F("%s", test.name), func(t *testing.T) {

			// Set up the ReplicationConfig
			config := ReplicationConfig{
				Source:  test.connName,
				Target:  test.connName,
				Streams: map[string]*ReplicationStreamConfig{},
			}

			// Add streams to config
			for _, streamName := range test.patterns {
				config.Streams[streamName] = &ReplicationStreamConfig{}
			}

			// properly generate the config
			config, err := UnmarshalReplication(g.Marshal(config))
			if !assert.NoError(t, err) {
				return
			}

			conn := connsMap[test.connName]
			wildcards := Wildcards{}
			if conn.Type.IsFile() {
				wildcards, err = config.ProcessWildcardsFile(conn, test.patterns)
				assert.NoError(t, err)
			} else if conn.Type.IsDb() {
				wildcards, err = config.ProcessWildcardsDatabase(conn, test.patterns)
				assert.NoError(t, err)
			} else {
				assert.Fail(t, "Connection type not supported")
			}

			streams := []string{}
			for _, wildcard := range wildcards {
				streams = append(streams, wildcard.StreamNames...)
			}

			// assert that the streams are correct
			assert.Equal(t, len(test.expected), len(streams))
			for _, streamName := range streams {
				assert.Contains(t, test.expected, streamName)
			}
		})

	}
}
