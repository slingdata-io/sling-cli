package iop

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONFlattenedSelectOrder(t *testing.T) {
	ds := NewDatastream(nil)
	ds.Sp.Config.Flatten = 0
	ds.Sp.Config.Select = []string{"customer__id", "created_at"}

	require.NoError(t, ds.ConsumeJsonReader(strings.NewReader(
		`[{"created_at":"2026-01-01","customer":{"id":1}}]`,
	)))
	data, err := ds.Collect(0)
	require.NoError(t, err)
	require.Equal(t, []string{"customer__id", "created_at"}, data.Columns.Names())
	require.Len(t, data.Rows, 1)
}
