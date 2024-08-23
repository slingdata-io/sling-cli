package connection

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestConnection(t *testing.T) {
	m := g.M(
		"url", os.Getenv("POSTGRES_URL"),
	)
	c1, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c1

	m = g.M(
		"username", "postgres",
		"password", "postgres",
		"host", "bionic.larco.us",
		"port", 55432,
		"database", "postgres",
		"sslmode", "disable",
	)
	c2, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c2

	_, err = NewConnection("Db", "someother", m)
	assert.NoError(t, err)

	_, err = NewConnectionFromURL("Db", os.Getenv("POSTGRES_URL"))
	assert.NoError(t, err)
}

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
			},
		},
		{
			connName: "aws_s3",
			pattern:  "sling_test/*",
			expected: []string{
				"sling_test/csv/",
				"sling_test/files/",
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
			_, files, schemata, err := conn.Discover(opts)
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

			// assert that the streams are correct
			assert.Equal(t, len(test.expected), len(results))
			for _, result := range results {
				assert.Contains(t, test.expected, result)
			}
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
