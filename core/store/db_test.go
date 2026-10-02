package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/stretchr/testify/require"
)

// testDB points the store at a temp sling home and re-initializes it. The
// store database is process state, so the tests that use it must not run in
// parallel.
func testDB(t *testing.T) {
	t.Helper()
	t.Setenv("SLING_HOME_DIR", t.TempDir())
	env.LoadHomeDir()
	Db = nil
	Conn = nil
	InitDB()
	if Db == nil {
		t.Fatal("the store database did not initialize")
	}
	t.Cleanup(func() {
		Db = nil
		Conn = nil
	})
}

// GetQueryHistoryFiltered filters in SQL before Count and the page window.
func TestGetQueryHistoryFiltered(t *testing.T) {
	testDB(t)

	key := "project-a"
	now := time.Now()
	rows := []QueryHistory{
		{WorkspaceKey: &key, Connection: "MY_PG", Query: "select 1 from users", Status: "success", RowCount: 3, CreatedAt: now.Add(-3 * time.Hour)},
		{WorkspaceKey: &key, Connection: "my_pg", Query: "UPDATE users SET a = 1", Status: "error", ErrorMessage: "syntax error", CreatedAt: now.Add(-2 * time.Hour)},
		{WorkspaceKey: &key, Connection: "OTHER", Query: "select 2 from orders", Status: "success", CreatedAt: now.Add(-time.Hour)},
		// A row of another workspace never matches.
		{WorkspaceKey: &key, Connection: "MY_PG", Query: "select 9", Status: "success"},
	}
	rows[3].WorkspaceKey = nil
	for i := range rows {
		require.NoError(t, SaveQueryHistory(&rows[i]))
	}

	// Without filters: the rows of the workspace, newest first, and Total is
	// the count of the workspace, not the page.
	entries, total, err := GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Limit: 2})
	require.NoError(t, err)
	require.EqualValues(t, 3, total)
	require.Len(t, entries, 2)
	require.Equal(t, "select 2 from orders", entries[0].Query)
	require.Equal(t, "UPDATE users SET a = 1", entries[1].Query)

	// The connection matches case-insensitively.
	_, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Connection: "my_pg"})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)

	// The search is a case-insensitive substring of the query.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Search: "USERS"})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	require.Len(t, entries, 2)

	// The status filter narrows on its own.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Status: "error"})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, "UPDATE users SET a = 1", entries[0].Query)

	// Filters combine, and Total counts after every one of them.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{
		WorkspaceKey: &key,
		Connection:   "MY_PG",
		Search:       "select",
		Status:       "success",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, entries, 1)
	require.Equal(t, "select 1 from users", entries[0].Query)

	// Offset pages past the first rows.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Limit: 1, Offset: 1})
	require.NoError(t, err)
	require.EqualValues(t, 3, total)
	require.Len(t, entries, 1)
	require.Equal(t, "UPDATE users SET a = 1", entries[0].Query)
}

// GetQueryHistory stays a thin wrapper over the filtered query.
func TestGetQueryHistoryWrapsFiltered(t *testing.T) {
	testDB(t)

	key := "project-b"
	for i := 0; i < 3; i++ {
		require.NoError(t, SaveQueryHistory(&QueryHistory{
			WorkspaceKey: &key,
			Connection:   "MY_PG",
			Query:        fmt.Sprintf("select %d", i),
			Status:       "success",
			CreatedAt:    time.Now().Add(-time.Duration(i) * time.Hour),
		}))
	}

	entries, total, err := GetQueryHistory(&key, 2, 0)
	require.NoError(t, err)
	require.EqualValues(t, 3, total)
	require.Len(t, entries, 2)

	// A nil workspace key matches the rows without one.
	_, total, err = GetQueryHistory(nil, 10, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, total)
}

// Search terms AND together, in any order and case: every term must hit the
// query somewhere (plan Part 7).
func TestGetQueryHistorySearchTermsAndInAnyOrder(t *testing.T) {
	testDB(t)

	key := "project-c"
	now := time.Now()
	rows := []QueryHistory{
		{WorkspaceKey: &key, Connection: "MY_PG", Query: "select 1 from users", Status: "success", CreatedAt: now.Add(-2 * time.Hour)},
		{WorkspaceKey: &key, Connection: "MY_PG", Query: "select 2 from orders", Status: "success", CreatedAt: now.Add(-time.Hour)},
		{WorkspaceKey: &key, Connection: "MY_PG", Query: "UPDATE users SET a = 1", Status: "success", CreatedAt: now},
	}
	for i := range rows {
		require.NoError(t, SaveQueryHistory(&rows[i]))
	}

	// "users select" keeps only the rows with both terms, in any position.
	entries, total, err := GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Search: "users select"})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, entries, 1)
	require.Equal(t, "select 1 from users", entries[0].Query)

	// The reverse order matches the same row.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Search: "SELECT Users"})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, "select 1 from users", entries[0].Query)

	// Terms may sit far apart: "set update" hits the UPDATE row.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Search: "set update"})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Equal(t, "UPDATE users SET a = 1", entries[0].Query)

	// A term that misses one row drops only that row.
	entries, total, err = GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: &key, Search: "select"})
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
}
