package sling

import (
	"regexp"
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

// assertMatchesPattern validates that a value matches a regex pattern
func assertMatchesPattern(t *testing.T, pattern, value, message string) bool {
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		t.Errorf("Invalid regex pattern %s: %v", pattern, err)
		return false
	}
	if !matched {
		t.Errorf("%s: expected pattern %s, got %s", message, pattern, value)
		return false
	}
	return true
}

func TestReplicationCompile(t *testing.T) {
	type expectedTask struct {
		StreamName    string
		StreamSQL     string // Source.Query if SQL specified
		ObjectName    string // Target.Object after variable expansion
		ObjectPattern string // Regex pattern for dynamic values (timestamps, etc.)
	}

	type testCase struct {
		Name          string
		YamlBody      string
		ExpectedTasks []expectedTask
		ShouldError   bool // True if compilation should fail
	}

	tests := []testCase{
		// Test 1: Simple stream variable expansion
		{
			Name: "simple_stream_table_variable",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: public.{stream_table}_copy
streams:
  public.users:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.users",
					ObjectName: `"public"."users_copy"`,
				},
			},
		},

		// Test 2: Schema and table variables
		{
			Name: "stream_schema_and_table",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: backup.{stream_schema}_{stream_table}
streams:
  public.customers:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.customers",
					ObjectName: `"backup"."public_customers"`,
				},
			},
		},

		// Test 3: Case transformation - uppercase
		{
			Name: "case_transformation_upper",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{stream_schema_upper}.{stream_table_upper}"
streams:
  public.orders:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.orders",
					ObjectName: `"PUBLIC"."ORDERS"`,
				},
			},
		},

		// Test 4: Case transformation - lowercase
		{
			Name: "case_transformation_lower",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{stream_schema_lower}.{stream_table_lower}"
streams:
  PUBLIC.PRODUCTS:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "PUBLIC.PRODUCTS",
					ObjectName: `"public"."products"`,
				},
			},
		},

		// Test 5: Multiple streams
		{
			Name: "multiple_streams",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: backup.{stream_table}
streams:
  public.users:
  public.orders:
  public.products:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.users",
					ObjectName: `"backup"."users"`,
				},
				{
					StreamName: "public.orders",
					ObjectName: `"backup"."orders"`,
				},
				{
					StreamName: "public.products",
					ObjectName: `"backup"."products"`,
				},
			},
		},

		// Test 6: SQL query with sql field
		{
			Name: "sql_query",
			YamlBody: `
source: postgres
target: postgres
streams:
  analytics_summary:
    sql: SELECT * FROM public.analytics WHERE year = 2024
    object: public.analytics_2024
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "analytics_summary",
					StreamSQL:  "SELECT * FROM public.analytics WHERE year = 2024",
					ObjectName: `"public"."analytics_2024"`,
				},
			},
		},

		// Test 7: Select columns
		{
			Name: "select_columns",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: public.{stream_table}_subset
streams:
  public.users:
    select: [id, email, created_at]
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.users",
					ObjectName: `"public"."users_subset"`,
				},
			},
		},

		// Test 8: Where clause
		{
			Name: "where_clause",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: public.{stream_table}_filtered
streams:
  public.logs:
    where: "created_at > '2024-01-01'"
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.logs",
					ObjectName: `"public"."logs_filtered"`,
				},
			},
		},

		// Test 9: Nested variable format - source
		{
			Name: "nested_source_variable",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{source.name}_{stream_table}"
streams:
  public.events:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.events",
					ObjectName: `"public"."postgres_events"`,
				},
			},
		},

		// Test 10: Nested variable format - target
		{
			Name: "nested_target_variable",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{target.name}_{ upper(stream_table) }"
streams:
  public.metrics:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.metrics",
					ObjectName: `"public"."postgres_METRICS"`,
				},
			},
		},

		// Test 11: Source and target type variables
		{
			Name: "source_target_type_variables",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{source.type}_{target.type}_{stream_table}"
streams:
  public.data:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.data",
					ObjectName: `"public"."postgres_postgres_data"`,
				},
			},
		},

		// Test 12: Timestamp variables with pattern (simple filename)
		{
			Name: "timestamp_variables",
			YamlBody: `
source: postgres
target: local
defaults:
  mode: full-refresh
  object: "file://./exports/daily_report_{timestamp.YYYY}_{timestamp.MM}_{timestamp.DD}.csv"
streams:
  public.daily_report:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName:    "public.daily_report",
					ObjectPattern: `^file://\./exports/daily_report_\d{4}_\d{2}_\d{2}\.csv$`,
				},
			},
		},

		// Test 13: Timestamp file_name format
		{
			Name: "timestamp_file_name_format",
			YamlBody: `
source: postgres
target: local
defaults:
  mode: full-refresh
  object: "file://./backups/backup_{timestamp.file_name}.csv"
streams:
  public.backup:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName:    "public.backup",
					ObjectPattern: `^file://\./backups/backup_\d{4}_\d{2}_\d{2}_\d{6}\.csv$`,
				},
			},
		},

		// Test 14: Mixed variables in single pattern
		{
			Name: "mixed_variables_pattern",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "public.{source.type}_{stream_table}"
streams:
  public.mixed_test:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.mixed_test",
					ObjectName: `"public"."postgres_mixed_test"`,
				},
			},
		},

		// Test 15: Complex pattern with multiple variables
		{
			Name: "complex_pattern_multiple_vars",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{stream_schema}_{stream_table}_{source.type}"
streams:
  public.complex:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.complex",
					ObjectName: `"public"."public_complex_postgres"`,
				},
			},
		},

		// Test 16: Stream-specific object override
		{
			Name: "stream_specific_object",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: default.{stream_table}
streams:
  public.users:
    object: custom.users_table
  public.orders:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.users",
					ObjectName: `"custom"."users_table"`,
				},
				{
					StreamName: "public.orders",
					ObjectName: `"default"."orders"`,
				},
			},
		},

		// Test 17: Mixed case stream variables
		{
			Name: "mixed_case_variables",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: "{stream_schema_upper}_{stream_table_lower}"
streams:
  Public.MixedCase:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "Public.MixedCase",
					ObjectName: `"public"."PUBLIC_mixedcase"`,
				},
			},
		},

		// Test 18: Multiple streams with different schemas
		{
			Name: "multiple_schemas",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: backup.{stream_schema}_{stream_table}
streams:
  public.table1:
  analytics.table2:
  reporting.table3:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.table1",
					ObjectName: `"backup"."public_table1"`,
				},
				{
					StreamName: "analytics.table2",
					ObjectName: `"backup"."analytics_table2"`,
				},
				{
					StreamName: "reporting.table3",
					ObjectName: `"backup"."reporting_table3"`,
				},
			},
		},

		// Test 19: Stream with SQL and variables. no_exist should be kept intact
		{
			Name: "sql_with_variables",
			YamlBody: `
source: postgres
target: postgres
streams:
  filtered_users:
    sql: SELECT id, name, '{env.DATE}' as date FROM public.users WHERE date = '{DATE}' -- {no_exist}
    object: public.{stream_name}_active
env:
	DATE: '2025-11-01'
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "filtered_users",
					StreamSQL:  "SELECT id, name, '2025-11-01' as date FROM public.users WHERE date = '2025-11-01' -- {no_exist}",
					ObjectName: `"public"."filtered_users_active"`,
				},
			},
		},

		// Test 20: Object variables in target
		{
			Name: "object_variables",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: staging.{stream_table}
streams:
  public.staging_test:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName: "public.staging_test",
					ObjectName: `"staging"."staging_test"`,
				},
			},
		},

		// Test 21: Combination of timestamp and source type
		{
			Name: "timestamp_and_source_type",
			YamlBody: `
source: postgres
target: local
defaults:
  mode: full-refresh
  object: "file://./archives/{source.type}_data_{timestamp.YYYY}{MM}.csv"
streams:
  public.monthly_data:
`,
			ExpectedTasks: []expectedTask{
				{
					StreamName:    "public.monthly_data",
					ObjectPattern: `^file://\./archives/postgres_data_\d{4}\d{2}\.csv$`,
				},
			},
		},

		// NEGATIVE TEST CASES

		// Test 22: Missing source connection
		{
			Name: "error_missing_source",
			YamlBody: `
target: postgres
defaults:
  mode: full-refresh
  object: public.test
streams:
  test_stream:
`,
			ShouldError: true,
		},

		// Test 23: Missing target connection
		{
			Name: "error_missing_target",
			YamlBody: `
source: postgres
defaults:
  mode: full-refresh
  object: public.test
streams:
  test_stream:
`,
			ShouldError: true,
		},

		// Test 24: Missing stream object
		{
			Name: "error_missing_object",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
streams:
  public.users:
`,
			ShouldError: true,
		},

		// Test 25: Invalid YAML syntax
		{
			Name: "error_invalid_yaml",
			YamlBody: `
source: postgres
target: postgres
defaults:
  mode: full-refresh
  object: test
streams:
  public.users
    bad syntax here
`,
			ShouldError: true,
		},

		// Test 26: Non-existent connection
		{
			Name: "error_nonexistent_connection",
			YamlBody: `
source: nonexistent_connection_xyz
target: postgres
defaults:
  mode: full-refresh
  object: public.test
streams:
  test_stream:
`,
			ShouldError: true,
		},
	}

	// Normalize YAML indentation for all tests
	for i := range tests {
		tests[i].YamlBody = strings.ReplaceAll(tests[i].YamlBody, "\t", "  ")
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			// Unmarshal YAML
			replication, err := UnmarshalReplication(tt.YamlBody)
			if tt.ShouldError {
				// For error cases, we expect either unmarshal or compile to fail
				if err != nil {
					return // Test passed - unmarshal failed as expected
				}
			} else {
				if !assert.NoError(t, err, "Failed to unmarshal") {
					return
				}
			}

			// Compile replication
			err = replication.Compile(nil)
			if tt.ShouldError {
				// Expect compilation to fail
				assert.Error(t, err, "Expected compilation to fail")
				return
			}
			g.LogError(err)
			if !assert.NoError(t, err, "Failed to compile") {
				return
			}

			// Validate task count
			if !assert.Equal(t, len(tt.ExpectedTasks), len(replication.Tasks),
				"Task count mismatch") {
				g.Warn("Expected %d tasks, got %d", len(tt.ExpectedTasks), len(replication.Tasks))
				for i, task := range replication.Tasks {
					g.Warn("Task[%d]: StreamName=%s, Object=%s", i, task.StreamName, task.Target.Object)
				}
				return
			}

			// Validate each task
			for i, expected := range tt.ExpectedTasks {
				task := replication.Tasks[i]

				assert.Equal(t, expected.StreamName, task.StreamName,
					"Task[%d] StreamName mismatch", i)

				if expected.StreamSQL != "" {
					assert.Equal(t, expected.StreamSQL, task.Source.Query,
						"Task[%d] SQL mismatch", i)
				}

				// Validate object name - exact match or pattern
				if expected.ObjectPattern != "" {
					assertMatchesPattern(t, expected.ObjectPattern, task.Target.Object,
						g.F("Task[%d] Object pattern mismatch", i))
				} else if expected.ObjectName != "" {
					assert.Equal(t, expected.ObjectName, task.Target.Object,
						"Task[%d] Object mismatch", i)
				}
			}
		})
	}
}
