package iop

import (
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/stretchr/testify/assert"
)

func TestParquetRead1(t *testing.T) {
	file, err := os.Open("/var/folders/49/1zc24t595j79t5mw7_t9gtxr0000gn/T/3322575208")
	g.LogFatal(err)
	reader := parquet.NewReader(file)
	g.P(reader.Schema())
	row := map[string]any{}
	err = reader.Read(&row)
	g.LogFatal(err)
	g.P(row)
}

func TestParquetWrite1(t *testing.T) {
	// parquet.Node
	cols := NewColumns(
		Column{Name: "col_string", Type: StringType},
		Column{Name: "col_int", Type: IntegerType},
		Column{Name: "col_bool", Type: BoolType},
		Column{Name: "col_float", Type: FloatType},
		Column{Name: "col_time", Type: TimestampType},
	)

	row := []parquet.Value{
		parquet.ValueOf("hello"),
		parquet.ValueOf(5),
		parquet.ValueOf(true),
		parquet.ValueOf(5.5),
		parquet.ValueOf(time.Now()),
	}

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)
	config.Schema = parquet.NewSchema("test", NewRecNode(cols, false))
	// config.Compression = &snappy.Codec{}

	file, err := os.CreateTemp(env.GetTempFolder(), "")
	g.Info(file.Name())
	g.LogFatal(err)

	writer := parquet.NewWriter(file, config)
	// writer2 := parquet.NewGenericWriter[any](file)
	// writer2.SetKeyValueMetadata()
	for _, val := range row {
		g.P(val)
	}
	_, err = writer.WriteRows([]parquet.Row{row, row, row})
	g.LogFatal(err)
	err = writer.Close()
	g.LogFatal(err)

	stat, _ := file.Stat()
	g.Info("size: %d", stat.Size())

	err = file.Close()
	g.LogFatal(err)

}

func TestParquetWrite2(t *testing.T) {
	// parquet.Node
	// parquet.NewSchema("test", node)

	file, err := os.CreateTemp(env.GetTempFolder(), "")
	g.Info(file.Name())
	g.LogFatal(err)

	row := []parquet.Value{
		parquet.ValueOf("hello"),
		parquet.ValueOf(5),
		// parquet.ValueOf(true),
		// parquet.ValueOf(5.5),
		// parquet.ValueOf(time.Now()),
	}

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)
	config.Schema = parquet.SchemaOf(struct {
		Name string
		Age  int
	}{})
	g.P(config.Schema)

	// schema := parquet.SchemaOf(nil)

	writer := parquet.NewWriter(file, config)
	// writer2 := parquet.NewGenericWriter[any](file)
	// writer2.SetKeyValueMetadata()
	for _, val := range row {
		g.P(val)
	}
	_, err = writer.WriteRows([]parquet.Row{row, row, row})
	g.LogFatal(err)
	err = writer.Close()
	g.LogFatal(err)

	err = file.Close()
	g.LogFatal(err)

}

func TestParquetWrite3(t *testing.T) {
	// parquet.Node
	// parquet.NewSchema("test", node)

	file, err := os.CreateTemp(env.GetTempFolder(), "*.parquet")
	g.Info(file.Name())
	g.LogFatal(err)

	type Row struct {
		Col1 *int `parquet:",,optional"`
		Col2 int
		Col3 *time.Time `parquet:",,optional"`
	}
	w := parquet.NewGenericWriter[Row](file)

	now := time.Now()
	rows := []Row{
		{Col1: g.Int(1), Col2: 0, Col3: &now},
		{Col1: nil, Col2: 1, Col3: nil},
		{Col1: g.Int(10), Col2: 4, Col3: nil},
	}

	_, err = w.Write(rows)
	g.LogFatal(err)

	err = w.Close()
	g.LogFatal(err)

	err = file.Close()
	g.LogFatal(err)

}

func BenchmarkParquetWrite3(b *testing.B) {
	// parquet.Node
	// parquet.NewSchema("test", node)

	file, err := os.CreateTemp(env.GetTempFolder(), "*.parquet")
	g.Info(file.Name())
	g.LogFatal(err)

	type Row struct {
		Col1 *int `parquet:",,optional"`
		Col2 int
		Col3 *time.Time `parquet:",,optional"`
	}
	w := parquet.NewGenericWriter[Row](file)

	now := time.Now()
	rows := []Row{
		{Col1: g.Int(1), Col2: 0, Col3: &now},
		{Col1: nil, Col2: 1, Col3: nil},
		{Col1: g.Int(10), Col2: 4, Col3: nil},
	}

	for n := 0; n < b.N; n++ {
		_, err = w.Write(rows)
		g.LogFatal(err)
	}

	err = w.Close()
	g.LogFatal(err)

	err = file.Close()
	g.LogFatal(err)

}

func TestParquetWrite4(t *testing.T) {

	file, err := os.CreateTemp(env.GetTempFolder(), "*.parquet")
	g.Info(file.Name())
	g.LogFatal(err)

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)

	columns := NewColumns(
		Column{Name: "col_int1", Type: IntegerType},
		Column{Name: "col_int2", Type: IntegerType},
		Column{Name: "col_time", Type: TimestampType},
	)
	config.Schema = getParquetSchema(columns, true)
	config.Compression = &parquet.Snappy

	w := parquet.NewGenericWriter[map[string]any](file, config)

	now := time.Now()
	rows := []map[string]any{
		{"col_int1": 1, "col_int2": 0, "col_time": now},
		{"col_int1": nil, "col_int2": 1, "col_time": nil},
	}
	_, err = w.Write(rows)
	g.LogFatal(err)

	err = w.Close()
	g.LogFatal(err)

	err = file.Close()
	g.LogFatal(err)

}

// 4 times slower than BenchmarkParquetWrite3 when using map, but fast enough
func BenchmarkParquetWrite4(b *testing.B) {

	file, err := os.CreateTemp(env.GetTempFolder(), "*.parquet")
	g.Info(file.Name())
	g.LogFatal(err)

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)

	columns := NewColumns(
		Column{Name: "col_int1", Type: IntegerType},
		Column{Name: "col_int2", Type: IntegerType},
		Column{Name: "col_time", Type: TimestampType},
	)
	config.Schema = getParquetSchema(columns, true)
	config.Compression = &parquet.Snappy

	w := parquet.NewGenericWriter[map[string]any](file, config)

	now := time.Now()
	rows := []map[string]any{
		{"col_int1": 1, "col_int2": 0, "col_time": now},
		{"col_int1": nil, "col_int2": 1, "col_time": nil},
	}

	for n := 0; n < b.N; n++ {
		_, err = w.Write(rows)
		g.LogFatal(err)
	}

	err = w.Close()
	g.LogFatal(err)

	err = file.Close()
	g.LogFatal(err)
}

func TestParquet(t *testing.T) {
	file, err := os.Open("/tmp/test.parquet")
	g.LogFatal(err)

	stat, err := file.Stat()
	g.LogFatal(err)

	pfile, err := parquet.OpenFile(file, stat.Size())
	g.LogFatal(err)

	g.Info("NumRows = %d", pfile.NumRows())
	g.Info("len(RowGroups) = %d", len(pfile.RowGroups()))
	g.P(pfile.Schema())
}

func TestParquetDecimal(t *testing.T) {
	filePath := "/tmp/test.parquet"

	file, err := os.Create(filePath)
	g.Info(file.Name())
	g.LogFatal(err)

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)

	columns := NewColumns(
		Columns{
			{Name: "col_big_int", Type: BigIntType},
			{Name: "col_decimal_type", Type: DecimalType},
			// {Name: "col_float_type", Type: FloatType},
		}...,
	)

	config.Schema = getParquetSchema(columns, false)

	fw := parquet.NewWriter(file, config)

	correctIntValues := []int64{}
	correctDecValues := []string{}
	for i := 0; i < 5; i++ {
		intVal := g.RandInt64(1000000)
		decVal := g.F("%d.%d", g.RandInt64(1000000), g.RandInt64(1000000000))
		if i == 0 {
			decVal = "-1.2"
		}

		rec := []parquet.Value{
			parquet.ValueOf(intVal), // big_int_type
			parquet.ValueOf(decVal), // decimal_type
			// parquet.ValueOf(decVal), // float_type
		}
		_, err = fw.WriteRows([]parquet.Row{rec})

		correctIntValues = append(correctIntValues, intVal)
		correctDecValues = append(correctDecValues, decVal)
	}
	g.LogFatal(err)

	fw.Close()
	file.Close()

	g.Info("correctIntValues => %s", g.Marshal(correctIntValues))
	g.Info("correctDecValues => %s", g.Marshal(correctDecValues))
	// check file with core/dbio/scripts/check_parquet.py

}

func TestParquetDuckDb(t *testing.T) {
	// Initialize ParquetDuckDb
	testFile := "test/test1.parquet"
	p, err := NewParquetReaderDuckDb(testFile)
	assert.NoError(t, err)
	if !assert.NotNil(t, p) {
		return
	}

	// Test Columns method
	columns, err := p.Columns()
	assert.NoError(t, err)
	assert.NotEmpty(t, columns)

	// Check if columns are correctly parsed
	expectedColumns := []struct {
		name     string
		dataType ColumnType
	}{
		{"id", BigIntType},
		{"first_name", StringType},
		{"last_name", StringType},
		{"email", StringType},
		{"target", BoolType},
		{"create_dt", TimestampzType},
		{"date", TimestampzType},
		{"rating", DecimalType},
		{"code", DecimalType},
	}

	if assert.Equal(t, len(expectedColumns), len(columns), "Number of columns should match") {

		for i, expected := range expectedColumns {
			assert.Equal(t, expected.name, columns[i].Name, "Column name should match")
			assert.Equal(t, expected.dataType, columns[i].Type, "Column type should match for: %s", columns[i].Name)
		}
	}
	// Test Close method
	err = p.Close()
	assert.NoError(t, err)
}
