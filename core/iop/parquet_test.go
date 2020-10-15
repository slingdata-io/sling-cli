package iop

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParquet(t *testing.T) {
	return

	csvPath := "test/test1.1.csv"
	pqPath := "test/test1.1.parquet"

	csv1 := CSV{Path: csvPath}
	err := csv1.InferSchema()
	assert.NoError(t, err)

	// Parquet
	ds, err := csv1.ReadStream()
	assert.NoError(t, err)

	pq1 := Parquet{Path: pqPath}
	err = pq1.WriteStream(ds)
	assert.NoError(t, err)

	err = os.Remove(pqPath)

}
