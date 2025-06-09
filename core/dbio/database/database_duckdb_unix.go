//go:build linux || darwin

package database

import (
	"bufio"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	switch conn.GetProp("copy_method") {
	case "named_pipes":
		return conn.importViaNamedPipe(tableFName, df)
	case "csv_files":
		return conn.importViaTempCSVs(tableFName, df)
	case "csv_http":
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	case "arrow_http":
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeArrow)
	default:
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	}
}

func (conn *DuckDbConn) importViaNamedPipe(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// Create a named pipe
	folderPath := path.Join(env.GetTempFolder(), "duckdb", "import", env.CleanTableName(tableFName), g.NowFileStr())
	if err = os.MkdirAll(folderPath, 0755); err != nil {
		return 0, g.Error(err, "could not create temp folder: %s", folderPath)
	}

	pipePath := path.Join(folderPath, "duckdb_pipe")
	if err = syscall.Mkfifo(pipePath, 0666); err != nil {
		return 0, g.Error(err, "could not create named pipe")
	}
	defer os.Remove(pipePath)

	importContext := g.NewContext(conn.context.Ctx)

	importContext.Wg.Write.Add()
	readyChn := make(chan bool)
	go func() {
		defer importContext.Wg.Write.Done()

		config := iop.DefaultStreamConfig()
		config.Header = true
		config.Delimiter = ","
		config.Escape = `"`
		config.Quote = `"`
		config.NullAs = `\N`
		config.DatetimeFormat = conn.Type.GetTemplateValue("variable.timestampz_layout")

		readyChn <- true

		pipeFile, err := os.OpenFile(pipePath, os.O_WRONLY, os.ModeNamedPipe)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not open named pipe for writing"))
			return
		}
		defer pipeFile.Close()
		bufWriter := bufio.NewWriter(pipeFile)

		tbw := int64(0)
		for ds := range df.StreamCh {
			for batchR := range ds.NewCsvReaderChnl(config) {
				bw, err := io.Copy(bufWriter, batchR.Reader)
				if err != nil {
					err = g.Error(err, "Error writing from reader")
					df.Context.CaptureErr(err)
					return
				}
				tbw += bw
			}
		}

		g.Debug("wrote %d bytes via named pipe", tbw)
	}()

	columnNames := lo.Map(df.Columns.Names(), func(col string, i int) string {
		return `"` + col + `"`
	})

	sqlLines := []string{
		g.F(`insert into %s (%s) select * from read_csv('%s', delim=',', auto_detect=False, header=True, columns=%s, max_line_size=2000000, parallel=false, quote='"', escape='"', nullstr='\N', auto_detect=false);`, table.FDQN(), strings.Join(columnNames, ", "), pipePath, conn.generateCsvColumns(df.Columns)),
	}

	sql := strings.Join(sqlLines, ";\n")

	<-readyChn // wait for writer to be ready
	result, err := conn.duck.ExecContext(conn.Context().Ctx, sql)
	if err != nil {
		return 0, g.Error(err, "could not insert into %s", tableFName)
	}

	importContext.Wg.Write.Wait()

	if result != nil {
		inserted, _ := result.RowsAffected()
		g.Debug("inserted %d rows", inserted)
	}

	return df.Count(), nil
}
