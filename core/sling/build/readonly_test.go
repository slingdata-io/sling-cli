package build

import (
	"sync"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateReadOnlyQuery(t *testing.T) {
	allowed := []string{
		"select 1",
		"SELECT 1",
		"  select 1  ",
		"-- leading comment\nselect 1",
		"/* block */ select 1",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"explain select 1",
		"EXPLAIN ANALYZE SELECT 1",
		"show tables",
		"describe foo",
		"desc foo",
		"select * from t where name = 'delete'",
		"select 'insert into t'",
		"select 1;",
		`SELECT * FROM "delete"`,
		"select /* insert */ 1",
		"select $$delete from t$$",
		"select $tag$insert into t$tag$",
		"select * from t where x = 'foo; drop table t'",
	}
	for _, q := range allowed {
		t.Run("allow/"+q, func(t *testing.T) {
			assert.NoError(t, ValidateReadOnlyQuery(q, dbio.TypeDbPostgres))
		})
	}

	denied := []string{
		"insert into t values (1)",
		"INSERT INTO t VALUES (1)",
		"update t set x=1",
		"delete from t",
		"drop table t",
		"alter table t add column x int",
		"create table t (x int)",
		"truncate t",
		"merge into t using s on (1=1)",
		"grant select on t to u",
		"WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
		"select 1; drop table t",
		"SELECT * INTO t2 FROM t",
		"select 1; select 2",
		"call my_proc()",
		"exec my_proc",
		"execute my_proc",
		"set search_path to public",
		"use db",
		"copy t from stdin",
		"vacuum t",
		"replace into t values (1)",
		"",
		"   ",
		"-- only comment",
		"/* only comment */",
	}
	for _, q := range denied {
		t.Run("deny/"+q, func(t *testing.T) {
			err := ValidateReadOnlyQuery(q, dbio.TypeDbPostgres)
			require.Error(t, err)
			assert.Contains(t, err.Error(), readOnlyErrPrefix)
		})
	}
}

func TestValidateReadOnlyQueryParser(t *testing.T) {
	allowed := []struct {
		sql    string
		dbType dbio.Type
	}{
		{"values (1)", dbio.TypeDbPostgres},
		{"select * from copy", dbio.TypeDbPostgres},
		{"select `update` from t", dbio.TypeDbMySQL},
		{"explain select 1", dbio.TypeDbPostgres},
	}
	for _, tc := range allowed {
		t.Run("allow/"+tc.sql, func(t *testing.T) {
			assert.NoError(t, ValidateReadOnlyQuery(tc.sql, tc.dbType))
		})
	}

	denied := []struct {
		sql    string
		dbType dbio.Type
		reason string
	}{
		{"with x as (insert into t values (1) returning *) select * from x", dbio.TypeDbPostgres, "insert"},
		{"with x as (update t set x=1 returning *) select * from x", dbio.TypeDbPostgres, "update"},
		{"with x as (delete from t returning *) select * from x", dbio.TypeDbPostgres, "delete"},
		{"create table t2 as select 1", dbio.TypeDbPostgres, "create_table"},
		{"select * from t for update", dbio.TypeDbPostgres, "update"},
		{"select * into #tmp from t", dbio.TypeDbSQLServer, "into"},
		{"pragma memory_limit='10GB'", dbio.TypeDbDuckDb, "pragma"},
	}
	for _, tc := range denied {
		t.Run("deny/"+tc.sql, func(t *testing.T) {
			err := ValidateReadOnlyQuery(tc.sql, tc.dbType)
			require.Error(t, err)
			assert.Contains(t, err.Error(), readOnlyErrPrefix)
			assert.Contains(t, err.Error(), tc.reason)
		})
	}
}

func TestValidateReadOnlyQueryFallback(t *testing.T) {
	// Garbage SQL fails to parse; keyword guard denies (first token not allowed).
	err := ValidateReadOnlyQuery("selct 1 frm dual", dbio.TypeDbPostgres)
	require.Error(t, err)
	assert.Contains(t, err.Error(), readOnlyErrPrefix)

	// Dialect-specific EXPLAIN form may fail to parse; keyword guard allows EXPLAIN.
	assert.NoError(t, ValidateReadOnlyQuery("explain (format json) select 1", dbio.TypeDbPostgres))
}

func TestValidateReadOnlyQueryParallel(t *testing.T) {
	queries := []string{
		"select 1",
		"show tables",
		"explain select 1",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"select * from copy",
	}
	const n = 50
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := queries[i%len(queries)]
			if err := ValidateReadOnlyQuery(q, dbio.TypeDbPostgres); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("unexpected error: %v", err)
	}
}
