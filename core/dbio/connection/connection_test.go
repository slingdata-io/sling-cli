package connection

import (
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
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
			pattern:  "../../../cmd/sling/tests/files/*.csv",
			expected: []string{
				"../../../cmd/sling/tests/files/test.wide.csv",
				"../../../cmd/sling/tests/files/test1.1.csv",
				"../../../cmd/sling/tests/files/test1.2.csv",
				"../../../cmd/sling/tests/files/test1.csv",
				"../../../cmd/sling/tests/files/test1.result.csv",
				"../../../cmd/sling/tests/files/test1.upsert.csv",
				"../../../cmd/sling/tests/files/test2.csv",
				"../../../cmd/sling/tests/files/test4.csv",
				"../../../cmd/sling/tests/files/test5.csv",
				"../../../cmd/sling/tests/files/test6.csv",
				"../../../cmd/sling/tests/files/test7.csv",
				"../../../cmd/sling/tests/files/test8.csv",
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
