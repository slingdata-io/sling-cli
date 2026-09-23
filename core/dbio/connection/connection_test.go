package connection

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/microsoft/go-mssqldb/msdsn"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionDiscover(t *testing.T) {

	type test struct {
		name     string
		connName string
		pattern  string
		expected []string
		err      bool
	}

	tests := []test{
		{
			connName: "local",
			pattern:  "../../../tests/files/*.csv",
			expected: []string{
				"../../../tests/files/disc763_explicit_cast.csv",
				"../../../tests/files/test.wide.csv",
				"../../../tests/files/test1.1.csv",
				"../../../tests/files/test1.2.csv",
				"../../../tests/files/test1.csv",
				"../../../tests/files/test1.result.csv",
				"../../../tests/files/test1.skiplines.csv",
				"../../../tests/files/test1.upsert.csv",
				"../../../tests/files/test2.csv",
				"../../../tests/files/test4.csv",
				"../../../tests/files/test5.csv",
				"../../../tests/files/test6.csv",
				"../../../tests/files/test7.csv",
				"../../../tests/files/test8.csv",
				"../../../tests/files/test_wide_columns.csv",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/clickhouse-duckdb.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/",
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
				"/_/analytics/sling/clickhouse-hydra.yaml",
				"/_/analytics/sling/clickhouse-motherduck.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/*.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
				"/_/analytics/sling/clickhouse-hydra.yaml",
				"/_/analytics/sling/clickhouse-motherduck.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/**.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
				"/_/analytics/sling/clickhouse-hydra.yaml",
				"/_/analytics/sling/clickhouse-motherduck.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/*.none",
			expected: []string{},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/none",
			expected: []string{},
			err:      true,
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/*-duckdb.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-duckdb.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/clickhouse-h*.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-hydra.yaml",
			},
		},
		{
			connName: "sftp",
			pattern:  "/_/analytics/sling/clickhouse-?????.yaml",
			expected: []string{
				"/_/analytics/sling/clickhouse-hydra.yaml",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test",
			expected: []string{
				"sling_test/",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/",
			expected: []string{
				"sling_test/csv/",
				"sling_test/files/",
				"sling_test/lineitem_iceberg/",
				"sling_test/delta/",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/*",
			expected: []string{
				"sling_test/csv/",
				"sling_test/files/",
				"sling_test/lineitem_iceberg/",
				"sling_test/delta/",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "does/not/exist",
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/**",
			expected: []string{
				"sling_test/delta/",
				"sling_test/delta/_delta_log/",
				"sling_test/delta/country=Argentina/",
				"sling_test/delta/country=China/",
				"sling_test/delta/country=Germany/",
				"sling_test/lineitem_iceberg/",
				"sling_test/lineitem_iceberg/data/",
				"sling_test/lineitem_iceberg/metadata/",
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
				"sling_test/delta/_delta_log/00000000000000000000.json",
				"sling_test/delta/country=Argentina/part-00000-8d0390a3-f797-4265-b9c2-da1c941680a3.c000.snappy.parquet",
				"sling_test/delta/country=China/part-00000-88fba1af-b28d-4303-9c85-9a97be631d40.c000.snappy.parquet",
				"sling_test/delta/country=Germany/part-00000-030076e1-5ec9-47c2-830a-1569f823b6ee.c000.snappy.parquet",
				"sling_test/files/test1k_s3.csv",
				"sling_test/files/test1k_s3.json",
				"sling_test/files/test1k_s3.parquet",
				"sling_test/lineitem_iceberg/README.md",
				"sling_test/lineitem_iceberg/data/00000-411-0792dcfe-4e25-4ca3-8ada-175286069a47-00001.parquet",
				"sling_test/lineitem_iceberg/data/00041-414-f3c73457-bbd6-4b92-9c15-17b241171b16-00001.parquet",
				"sling_test/lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m0.avro",
				"sling_test/lineitem_iceberg/metadata/10eaca8a-1e1c-421e-ad6d-b232e5ee23d3-m1.avro",
				"sling_test/lineitem_iceberg/metadata/cf3d0be5-cf70-453d-ad8f-48fdc412e608-m0.avro",
				"sling_test/lineitem_iceberg/metadata/snap-3776207205136740581-1-cf3d0be5-cf70-453d-ad8f-48fdc412e608.avro",
				"sling_test/lineitem_iceberg/metadata/snap-7635660646343998149-1-10eaca8a-1e1c-421e-ad6d-b232e5ee23d3.avro",
				"sling_test/lineitem_iceberg/metadata/v1.metadata.json",
				"sling_test/lineitem_iceberg/metadata/v2.metadata.json",
				"sling_test/lineitem_iceberg/metadata/version-hint.text",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/csv/part.01.0001.csv",
			expected: []string{
				"sling_test/csv/part.01.0001.csv",
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/files/test1k_s3.????",
			expected: []string{
				"sling_test/files/test1k_s3.json",
			},
		},
		{
			connName: "postgres",
			pattern:  "public.test1k_bigquery*",
			expected: []string{
				"\"public\".\"test1k_bigquery_pg\"",
				"\"public\".\"test1k_bigquery_pg_vw\"",
				"\"public\".\"test1k_bigquery_pg_orig\"",
			},
		},
		{
			connName: "postgres",
			pattern:  "public.test1k_bigquery_??",
			expected: []string{
				"\"public\".\"test1k_bigquery_pg\"",
			},
		},
	}

	// Set all the connections
	conns := GetLocalConns()
	connsMap := map[string]Connection{}
	for _, test := range tests {
		if _, ok := connsMap[test.connName]; !ok {
			conn := conns.Get(test.connName).Connection
			if !assert.NotEmpty(t, conn.Name) {
				return
			}
			connsMap[test.connName] = conn
		}
	}

	for _, test := range tests {
		if test.name == "" {
			test.name = g.F("%s|%s", test.connName, test.pattern)
		}

		t.Run(g.F("%s", test.name), func(t *testing.T) {

			conn := connsMap[test.connName]
			results := []string{}

			opts := &DiscoverOptions{Pattern: test.pattern}
			_, files, schemata, endpoints, err := conn.Discover(opts)
			if test.err {
				assert.Error(t, err)
				return
			} else if !assert.NoError(t, err) {
				return
			}
			for _, file := range files {
				results = append(results, file.Path())
			}
			for _, table := range schemata.Tables() {
				results = append(results, table.FullName())
			}
			for _, endpoint := range endpoints {
				results = append(results, endpoint.Name)
			}

			// assert that the streams are correct
			for _, result := range results {
				assert.Contains(t, test.expected, result)
			}
			for _, expected := range test.expected {
				assert.Contains(t, results, expected)
			}
			assert.Equal(t, len(test.expected), len(results))
		})
	}
}

func TestQueryURL(t *testing.T) {
	password := "<JuIQ){cXpV{<)nB+4DrNX;LC+0dx;+Vl4hk^!{M(+R.66Y<}"
	// wrong := "%3CJuIQ%29%7BcXpV%7B%3C%29nB+4DrNX;LC+0dx;+Vl4hk%5E%21%7BM%28+R.66Y%3C%7D"
	// correct := "%3CJuIQ%29%7BcXpV%7B%3C%29nB%2B4DrNX%3BLC%2B0dx%3B%2BVl4hk%5E%21%7BM%28%2BR.66Y%3C%7D"
	// correct := "%3CJuIQ%29%7BcXpV%7B%3C%29nB%2B4DrNX%3BLC%2B0dx%3B%2BVl4hk%5E%21%7BM%28%2BR.66Y%3C%7D"
	// println(url.QueryEscape(password))
	_ = password
}

// GitHub #780: named-instance URL building.
// The driver calls SQL Browser (UDP 1434) only when instance is set and port is 0.
// If a port is set, the driver uses that port and ignores the instance name.
func TestSQLServerNamedInstance(t *testing.T) {
	type want struct {
		host         string
		port         uint64
		instance     string
		database     string
		callBrowser  bool
		dataHost     string
		dataInstance string
		urlHas       []string
		urlNotHas    []string
	}

	base := map[string]any{
		"type":     "sqlserver",
		"user":     "the_user",
		"password": "secret",
		"database": "dbname",
	}

	with := func(extra map[string]any) map[string]any {
		data := map[string]any{}
		for k, v := range base {
			data[k] = v
		}
		for k, v := range extra {
			data[k] = v
		}
		return data
	}

	cases := []struct {
		name string
		data map[string]any
		want want
	}{
		{
			name: "instance only does not inject default port",
			data: with(map[string]any{
				"host":     "THEHOST",
				"instance": "Instance",
			}),
			want: want{
				host:         "THEHOST",
				port:         0,
				instance:     "Instance",
				database:     "dbname",
				callBrowser:  true,
				dataHost:     "THEHOST",
				dataInstance: "Instance",
				urlHas:       []string{"@THEHOST/Instance"},
				urlNotHas:    []string{":1433"},
			},
		},
		{
			name: "port only connects to that port",
			data: with(map[string]any{
				"host": "THEHOST",
				"port": 1433,
			}),
			want: want{
				host:        "THEHOST",
				port:        1433,
				instance:    "",
				database:    "dbname",
				callBrowser: false,
				dataHost:    "THEHOST",
				urlHas:      []string{"@THEHOST:1433"},
			},
		},
		{
			name: "port and instance keep both in the url",
			data: with(map[string]any{
				"host":     "THEHOST",
				"port":     1433,
				"instance": "Instance",
			}),
			want: want{
				host:         "THEHOST",
				port:         1433,
				instance:     "Instance",
				database:     "dbname",
				callBrowser:  false,
				dataHost:     "THEHOST",
				dataInstance: "Instance",
				urlHas:       []string{"@THEHOST:1433/Instance"},
			},
		},
		{
			name: "host slash instance without port",
			data: with(map[string]any{
				"host": "THEHOST/Instance",
			}),
			want: want{
				host:         "THEHOST",
				port:         0,
				instance:     "Instance",
				database:     "dbname",
				callBrowser:  true,
				dataHost:     "THEHOST",
				dataInstance: "Instance",
				urlHas:       []string{"@THEHOST/Instance"},
				urlNotHas:    []string{"Instance:1433", "@THEHOST/Instance:1433"},
			},
		},
		{
			name: "host backslash instance without port",
			data: with(map[string]any{
				"host": `THEHOST\Instance`,
			}),
			want: want{
				host:         "THEHOST",
				port:         0,
				instance:     "Instance",
				database:     "dbname",
				callBrowser:  true,
				dataHost:     "THEHOST",
				dataInstance: "Instance",
				urlHas:       []string{"@THEHOST/Instance"},
				urlNotHas:    []string{`THEHOST\Instance`, "Instance:1433"},
			},
		},
		{
			name: "host slash instance with explicit port",
			data: with(map[string]any{
				"host": "THEHOST/Instance",
				"port": 51433,
			}),
			want: want{
				host:         "THEHOST",
				port:         51433,
				instance:     "Instance",
				database:     "dbname",
				callBrowser:  false,
				dataHost:     "THEHOST",
				dataInstance: "Instance",
				urlHas:       []string{"@THEHOST:51433/Instance"},
				urlNotHas:    []string{"Instance:51433"},
			},
		},
		{
			name: "url with port and instance keeps both",
			data: map[string]any{
				"url": "sqlserver://myuser:mypass@host.ip:51433/my_instance?database=dbname",
			},
			want: want{
				host:         "host.ip",
				port:         51433,
				instance:     "my_instance",
				database:     "dbname",
				callBrowser:  false,
				dataHost:     "host.ip",
				dataInstance: "my_instance",
				urlHas:       []string{"@host.ip:51433/my_instance"},
			},
		},
		{
			name: "url with instance only does not inject default port",
			data: map[string]any{
				"url": "sqlserver://myuser:mypass@host.ip/my_instance?database=dbname",
			},
			want: want{
				host:         "host.ip",
				port:         0,
				instance:     "my_instance",
				database:     "dbname",
				callBrowser:  true,
				dataHost:     "host.ip",
				dataInstance: "my_instance",
				urlHas:       []string{"@host.ip/my_instance"},
				urlNotHas:    []string{":1433"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewConnection("TEST", dbio.TypeDbSQLServer, tc.data)
			require.NoError(t, err)

			gotURL := c.URL()
			for _, s := range tc.want.urlHas {
				assert.Contains(t, gotURL, s, "url=%s", gotURL)
			}
			for _, s := range tc.want.urlNotHas {
				assert.NotContains(t, gotURL, s, "url=%s", gotURL)
			}

			if tc.want.dataHost != "" {
				assert.Equal(t, tc.want.dataHost, c.Data["host"])
			}
			if tc.want.dataInstance != "" {
				assert.Equal(t, tc.want.dataInstance, c.Data["instance"])
			}

			cfg, err := msdsn.Parse(gotURL)
			require.NoError(t, err)
			assert.Equal(t, tc.want.host, cfg.Host)
			assert.Equal(t, tc.want.port, cfg.Port)
			assert.Equal(t, tc.want.instance, cfg.Instance)
			assert.Equal(t, tc.want.database, cfg.Database)
			assert.Equal(t, tc.want.callBrowser, len(cfg.Instance) > 0 && cfg.Port == 0)

			assert.False(t, strings.Contains(cfg.Instance, ":"), "instance must not include a port: %q", cfg.Instance)
		})
	}
}

// A LanceDB namespace root can itself be an object store URI, so everything
// after the `lancedb://` scheme is the path.
func TestLanceDBConnectionURL(t *testing.T) {
	cases := []struct{ url, path string }{
		{"lancedb:///data/lancedb", "/data/lancedb"},
		{"lancedb://./data/lancedb", "./data/lancedb"},
		{"lancedb://relative/dir", "relative/dir"},
		{"lancedb://s3://my-bucket/prefix", "s3://my-bucket/prefix"},
		{"lancedb:///data/lancedb?schema=main", "/data/lancedb"},
	}

	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			c, err := NewConnectionFromURL("LANCEDB_TEST", tc.url)
			require.NoError(t, err)
			assert.Equal(t, dbio.TypeDbLanceDB, c.Type)
			assert.Equal(t, tc.path, c.DataS(true)["path"])
		})
	}

	// the `path` property round-trips through the connection URL
	c, err := NewConnection("LANCEDB_TEST", dbio.TypeDbLanceDB, map[string]any{"path": "s3://my-bucket/prefix"})
	require.NoError(t, err)
	assert.Equal(t, "lancedb://s3://my-bucket/prefix", c.URL())

	reparsed, err := NewConnectionFromURL("LANCEDB_TEST", c.URL())
	require.NoError(t, err)
	assert.Equal(t, dbio.TypeDbLanceDB, reparsed.Type)
	assert.Equal(t, "s3://my-bucket/prefix", reparsed.DataS(true)["path"])
}

// DynamoDB connections carry the region in the URL host (`dynamodb://us-east-1`)
// and take credentials from the AWS credential chain when they are not set.
func TestDynamoDBConnectionURL(t *testing.T) {
	c, err := NewConnectionFromURL("DDB", "dynamodb://eu-west-1")
	require.NoError(t, err)
	assert.Equal(t, dbio.TypeDbDynamoDB, c.Type)
	assert.Equal(t, "eu-west-1", c.DataS(true)["aws_region"])
	assert.Equal(t, "default", c.DataS(true)["schema"]) // DynamoDB has no schemas

	// properties map onto the AWS SDK options and round-trip through the URL
	c, err = NewConnection("DDB", dbio.TypeDbDynamoDB, map[string]any{
		"region":            "us-east-2",
		"access_key_id":     "AKIA_TEST",
		"secret_access_key": "SECRET_TEST",
		"session_token":     "TOKEN_TEST",
		"endpoint":          "http://localhost:8000",
	})
	require.NoError(t, err)
	assert.Equal(t, "us-east-2", c.DataS(true)["aws_region"])
	assert.Equal(t, "AKIA_TEST", c.DataS(true)["aws_access_key_id"])
	assert.Equal(t, "SECRET_TEST", c.DataS(true)["aws_secret_access_key"])
	assert.Equal(t, "TOKEN_TEST", c.DataS(true)["aws_session_token"])
	assert.Equal(t, "http://localhost:8000", c.DataS(true)["endpoint"])
	assert.Equal(t, "dynamodb://us-east-2", c.URL())

	reparsed, err := NewConnectionFromURL("DDB", c.URL())
	require.NoError(t, err)
	assert.Equal(t, dbio.TypeDbDynamoDB, reparsed.Type)
	assert.Equal(t, "us-east-2", reparsed.DataS(true)["aws_region"])

	// a region from the environment is used when the URL and props have none
	t.Setenv("AWS_REGION", "ap-southeast-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	c, err = NewConnection("DDB", dbio.TypeDbDynamoDB, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "ap-southeast-1", c.DataS(true)["aws_region"])
}

const testOptionsSpecYAML = `
name: "Test Options API"
defaults:
  state:
    page: 1
endpoints:
  items:
    request:
      url: "%s/items"
      parameters:
        page: "{state.page}"
    response:
      records:
        jmespath: "data[]"
    pagination:
      stop_condition: "false"
      next_state:
        page: "state.page + 1"
`

func testOptionsEntries(t *testing.T, serverURL string) ConnEntries {
	t.Helper()

	specPath := filepath.Join(t.TempDir(), "spec.yaml")
	require.NoError(t, os.WriteFile(specPath, []byte(fmt.Sprintf(testOptionsSpecYAML, serverURL)), 0644))

	conn, err := NewConnection("TEST_OPTIONS_API", dbio.TypeApi, map[string]any{
		"type": "api",
		"spec": "file://" + specPath,
	})
	require.NoError(t, err)

	return ConnEntries{{Name: "TEST_OPTIONS_API", Connection: conn}}
}

func TestConnEntriesTestWithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"id":1},{"id":2}]}`)
	}))
	defer server.Close()

	var events []api.SpecEvent
	levelBefore := g.GetLogLevel()
	ok, err := testOptionsEntries(t, server.URL).TestWithOptions(context.Background(), "TEST_OPTIONS_API", TestOptions{
		Endpoints:   []string{"items"},
		Limit:       10,
		MaxRequests: 2,
		Trace:       true,
		OnEvent:     func(e api.SpecEvent) { events = append(events, e) },
	})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, levelBefore, g.GetLogLevel(), "trace log level not restored")

	types := []string{}
	for _, e := range events {
		types = append(types, e.Type)
	}
	assert.Equal(t, []string{
		api.SpecEventTypeEndpointStart,
		api.SpecEventTypeRequestComplete, api.SpecEventTypeRecords,
		api.SpecEventTypeRequestComplete, api.SpecEventTypeRecords,
		api.SpecEventTypeEndpointDone,
	}, types, "unexpected event sequence: %#v", types)

	completes, records := 0, 0
	for _, e := range events {
		switch e.Type {
		case api.SpecEventTypeRequestComplete:
			completes++
			assert.Equal(t, "items", e.Endpoint)
			assert.Equal(t, completes, e.RequestIndex)
			assert.NotNil(t, e.Request)
			assert.NotNil(t, e.Response)
			assert.NotNil(t, e.StateBefore, "state_before missing")
			assert.NotNil(t, e.StateAfter, "state_after missing")
			assert.EqualValues(t, 2, e.Response["record_count"])
			assert.EqualValues(t, 200, e.Response["status"])
			// legacy wire fields
			assert.NotEmpty(t, e.ReqID)
			assert.NotZero(t, e.Timestamp)
			assert.NotEmpty(t, e.IterID)
			assert.Equal(t, len(`{"data":[{"id":1},{"id":2}]}`), e.SizeBytes)
		case api.SpecEventTypeEndpointDone:
			assert.Equal(t, 4, e.RecordCount)
		case api.SpecEventTypeRecords:
			records++
			assert.Len(t, e.Records, 2)
		}
	}
	assert.Equal(t, 2, completes, "max_requests not honored")
	assert.Equal(t, 2, records, "one records event per request expected")
}

func TestConnEntriesTestErrorEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := server.URL
	server.Close() // no listener: requests fail to connect

	var events []api.SpecEvent
	ok, err := testOptionsEntries(t, deadURL).TestWithOptions(context.Background(), "TEST_OPTIONS_API", TestOptions{
		Endpoints: []string{"items"},
		OnEvent:   func(e api.SpecEvent) { events = append(events, e) },
	})
	require.Error(t, err)
	assert.False(t, ok)
	require.NotEmpty(t, events)
	last := events[len(events)-1]
	assert.Equal(t, api.SpecEventTypeError, last.Type, "events: %#v", events)
	assert.Equal(t, "items", last.Endpoint)
	assert.NotEmpty(t, last.Error)
}

func TestConnEntriesTestFromEnv(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"id":1}]}`)
	}))
	defer server.Close()

	t.Setenv("SLING_TEST_ENDPOINTS", "items")
	t.Setenv("SLING_TEST_ENDPOINT_LIMIT", "5")
	t.Setenv("SLING_TEST_ENDPOINT_MAX_REQUESTS", "1")

	ok, err := testOptionsEntries(t, server.URL).Test("TEST_OPTIONS_API")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestConnEntriesTestSpecFileOverlay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"id":1}]}`)
	}))
	defer server.Close()

	// the connection's own spec has no "items" endpoint
	connSpec := filepath.Join(t.TempDir(), "conn.yaml")
	require.NoError(t, os.WriteFile(connSpec, []byte(fmt.Sprintf(`
name: "Conn Spec"
endpoints:
  others:
    request:
      url: "%s/others"
    response:
      records:
        jmespath: "data[]"
`, server.URL)), 0644))

	conn, err := NewConnection("TEST_OPTIONS_API", dbio.TypeApi, map[string]any{
		"type": "api",
		"spec": "file://" + connSpec,
	})
	require.NoError(t, err)
	entries := ConnEntries{{Name: "TEST_OPTIONS_API", Connection: conn}}

	// the draft spec replaces the connection's spec for this test only
	draftSpec := filepath.Join(t.TempDir(), "draft.yaml")
	require.NoError(t, os.WriteFile(draftSpec, []byte(fmt.Sprintf(testOptionsSpecYAML, server.URL)), 0644))

	var events []api.SpecEvent
	ok, err := entries.TestWithOptions(context.Background(), "TEST_OPTIONS_API", TestOptions{
		SpecFile:  draftSpec,
		Endpoints: []string{"items"},
		OnEvent:   func(e api.SpecEvent) { events = append(events, e) },
	})
	require.NoError(t, err)
	assert.True(t, ok)
	require.NotEmpty(t, events)
	assert.Equal(t, "items", events[0].Endpoint)

	// the connection entry itself is untouched
	assert.Equal(t, "file://"+connSpec, entries.Get("TEST_OPTIONS_API").Connection.Data["spec"])

	// a missing spec file is reported
	_, err = entries.TestWithOptions(context.Background(), "TEST_OPTIONS_API", TestOptions{
		SpecFile: filepath.Join(t.TempDir(), "nope.yaml"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec file not found")
}

func TestConnEntriesTestCancel(t *testing.T) {
	blocked := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(blocked) })
		<-r.Context().Done() // returns when the client aborts the request
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := testOptionsEntries(t, server.URL).TestWithOptions(ctx, "TEST_OPTIONS_API", TestOptions{
			Endpoints:   []string{"items"},
			MaxRequests: 2,
		})
		errCh <- err
	}()

	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("request never reached the server")
	}
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("test did not stop on cancel")
	}
}
