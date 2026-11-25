package iop

import (
	"bytes"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestAvroScanEnd(t *testing.T) {
	var buf bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &buf,
		Schema: `{"type":"record","name":"row","fields":[{"name":"id","type":"int"}]}`,
	})
	require.NoError(t, err)
	// Each Append writes a separate block.
	for id := int32(1); id <= 2; id++ {
		require.NoError(t, writer.Append([]any{map[string]any{"id": id}}))
	}

	for _, tc := range []struct {
		name      string
		size      int
		wantRows  int
		truncated bool
	}{
		{name: "complete", size: buf.Len(), wantRows: 2},
		{name: "truncated", size: buf.Len() - 1, wantRows: 1, truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			avro, err := NewAvroStream(bytes.NewReader(buf.Bytes()[:tc.size]), nil)
			require.NoError(t, err)
			ds := NewDatastream(avro.Columns())
			defer ds.Close()
			it := ds.NewIterator(ds.Columns, avro.nextFunc)

			rows := 0
			for avro.nextFunc(it) {
				rows++
			}

			require.Equal(t, tc.wantRows, rows)
			if tc.truncated {
				require.ErrorContains(t, ds.Err(), "unexpected EOF")
			} else {
				require.NoError(t, ds.Err())
			}
		})
	}
}
