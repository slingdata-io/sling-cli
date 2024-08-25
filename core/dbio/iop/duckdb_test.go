package iop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuckDb(t *testing.T) {
	duck := NewDuckDb(context.Background())

	t.Run("ExecMultiContext", func(t *testing.T) {
		result, err := duck.ExecMultiContext(
			context.Background(),
			"create table test (id int, name varchar)",
			"insert into test (id, name) values (1, 'John')",
			"insert into test (id, name) values (2, 'Jane')",
		)

		if assert.NoError(t, err) {
			rows, err := result.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(2), rows)
		}
	})

	t.Run("ExecContext with erroneous query", func(t *testing.T) {
		_, err := duck.Exec("SELECT * FROM non_existent_table")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non_existent_table")
	})

	t.Run("Export", func(t *testing.T) {

		duck := NewDuckDb(context.Background(), "path=/tmp/test.db")

		// Create a test table and insert some data
		_, err := duck.ExecMultiContext(
			context.Background(),
			"CREATE or replace TABLE export_test (id INT, name VARCHAR, age INT)",
			"INSERT INTO export_test VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 35)",
		)
		assert.NoError(t, err)

		// Test the Export function
		ds, err := duck.Export(
			context.Background(),
			"SELECT * FROM export_test ORDER BY id",
		)
		assert.NoError(t, err)
		assert.NotNil(t, ds)

		// Verify the exported data
		data, err := ds.Collect(0)
		records := data.Records()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(records))

		// Check the content of the first record
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, int64(30), records[0]["age"])

		// Check the content of the last record
		assert.Equal(t, int64(3), records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
		assert.Equal(t, int64(35), records[2]["age"])

		// Clean up: drop the test table
		_, err = duck.Exec("DROP TABLE export_test")
		assert.NoError(t, err)
	})
}
