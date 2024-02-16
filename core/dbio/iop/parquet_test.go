package iop

import (
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	parquet "github.com/parquet-go/parquet-go"
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
	config.Schema = parquet.NewSchema("test", NewRecNode(cols))
	// config.Compression = &snappy.Codec{}

	file, err := os.CreateTemp(os.TempDir(), "")
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

	file, err := os.CreateTemp(os.TempDir(), "")
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

	config.Schema = getParquetSchema(columns)

	fw := parquet.NewWriter(file, config)

	correctIntValues := []int64{}
	correctDecValues := []string{}
	for i := 0; i < 5; i++ {
		intVal := g.RandInt64(1000000)
		correctIntValues = append(correctIntValues, intVal)
		decVal := g.F("%d.%d", g.RandInt64(1000000), g.RandInt64(1000000))
		correctDecValues = append(correctDecValues, decVal)

		rec := []parquet.Value{
			parquet.ValueOf(intVal), // big_int_type
			parquet.ValueOf(decVal), // decimal_type
			// parquet.ValueOf(decVal), // float_type
		}
		_, err = fw.WriteRows([]parquet.Row{rec})
	}
	g.LogFatal(err)

	fw.Close()
	file.Close()

	g.Info("correctIntValues => %s", g.Marshal(correctIntValues))
	g.Info("correctDecValues => %s", g.Marshal(correctDecValues))
	// check file with core/dbio/scripts/check_parquet.py

}
