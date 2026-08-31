package sling

import (
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarkdownLines(t *testing.T) {
	lines := MarkdownLines{
		{"Connection", "MY_PG"},
		{"Count", "2"},
	}
	assert.Equal(t, "Connection: MY_PG  |  Count: 2", lines.Line())
	assert.Equal(t, "Connection: MY_PG\n\nCount: 2", lines.Text())
}

func TestDatasetToCompact(t *testing.T) {
	data := iop.NewDataset(iop.NewColumnsFromFields("database", "schema", "name"))
	data.Rows = append(data.Rows, []any{"mydb", "public", "users"})
	data.Rows = append(data.Rows, []any{"mydb", "public", "orders"})

	out := DatasetToCompact(data)
	require.Len(t, out, 2)
	assert.Equal(t, "Column Types", out[0].Key)
	assert.Equal(t, "string, string, string", out[0].Value)
	assert.Equal(t, "Pipe-Delimited Data", out[1].Key)
	assert.Contains(t, out[1].Value, "database|schema|name")
	assert.Contains(t, out[1].Value, "mydb|public|users")
	assert.True(t, strings.Contains(out[1].Value, "```"))
}

func TestCompactText(t *testing.T) {
	data := iop.NewDataset(iop.NewColumnsFromFields("name"))
	data.Rows = append(data.Rows, []any{"users"})
	text := CompactText(MarkdownLines{{"Type", "tables"}, {"Count", "1"}}, data)
	assert.Contains(t, text, "Type: tables  |  Count: 1")
	assert.Contains(t, text, "Column Types: string")
	assert.Contains(t, text, "name")
}
