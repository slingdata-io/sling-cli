package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/valentin-kaiser/go-dbase/dbase"
)

// DbaseConn reads dBase / FoxPro tables (`.dbf` files).
//
// The connection root is a `.dbf` file (a single table) or a directory holding
// `.dbf` files, where every file is a table of the `main` schema. dBase has no
// query engine, so the connector is read-only and answers the select statements
// sling generates for a table read: a field list, plus `limit` / `offset`.
// Filters and custom SQL are rejected with an explicit error.
//
// Values are read through the reader library, with the raw record inspected
// where the library cannot report the value as stored:
//
//   - a blank number, date or logical is stored as spaces (a blank datetime as
//     NULs), which the library reports as a zero value; such a field is NULL
//   - a variable length varchar / varbinary field stores the length of its
//     value in the last byte of the field, marked in the record's null flag
//     field; the library resolves those markers from the first varchar field
//     of the table for every field, so they are resolved here instead
type DbaseConn struct {
	BaseConn

	URL  string
	Path string // root: a .dbf file, or a directory of .dbf files
}

const dbaseDefaultSchema = "main"

// dbfTextNullTypes are the dBase types whose blank value is stored as text
// (spaces) or NULs, and which the reader library turns into a zero value.
var dbfTextNullTypes = []dbase.DataType{
	dbase.Numeric, dbase.Float, dbase.Date, dbase.DateTime, dbase.Logical,
}

// Init initiates the object
func (conn *DbaseConn) Init() error {
	conn.Path = strings.TrimSpace(conn.GetProp("path"))
	if conn.Path == "" {
		conn.Path = strings.TrimSpace(conn.GetProp("instance"))
	}
	if conn.Path == "" {
		conn.Path = DbasePathFromURL(conn.URL)
	}
	if conn.Path == "" {
		return g.Error("did not provide 'path' for dBase connection (a .dbf file or a directory of .dbf files)")
	}
	conn.SetProp("path", conn.Path)

	if conn.GetProp("schema") == "" {
		conn.SetProp("schema", dbaseDefaultSchema)
	}

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDBase

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// DbasePathFromURL returns the path of a dBase connection URL. The whole part
// after the scheme is the path, so that both `dbase:///data/tables` and
// `dbase://./tables` resolve as written.
func DbasePathFromURL(connURL string) string {
	path := strings.TrimSpace(connURL)
	for _, scheme := range []string{"dbase://", "dbf://"} {
		if strings.HasPrefix(strings.ToLower(path), scheme) {
			path = path[len(scheme):]
			if unescaped, err := url.PathUnescape(path); err == nil {
				path = unescaped
			}
			return path
		}
	}
	return path
}

// Connect validates the connection root
func (conn *DbaseConn) Connect(timeOut ...int) (err error) {
	if _, err := os.Stat(conn.Path); err != nil {
		return g.Error(err, "could not access dBase path: %s", conn.Path)
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))
	return nil
}

// GetURL returns the processed URL
func (conn *DbaseConn) GetURL(newURL ...string) string {
	if len(newURL) > 0 {
		return newURL[0]
	}
	return conn.BaseConn.URL
}

// ExecContext is not supported: a dBase file cannot be written by sling
func (conn *DbaseConn) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	return nil, g.Error("dBase connections are read-only, cannot execute: %s", strings.TrimSpace(query))
}

// tableFiles maps every table of the connection to its file path, keyed by the
// table name (the file name without its extension)
func (conn *DbaseConn) tableFiles() (files map[string]string, err error) {
	files = map[string]string{}

	info, err := os.Stat(conn.Path)
	if err != nil {
		return nil, g.Error(err, "could not access dBase path: %s", conn.Path)
	}

	if !info.IsDir() {
		name := strings.TrimSuffix(filepath.Base(conn.Path), filepath.Ext(conn.Path))
		files[name] = conn.Path
		return files, nil
	}

	entries, err := os.ReadDir(conn.Path)
	if err != nil {
		return nil, g.Error(err, "could not read dBase directory: %s", conn.Path)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".dbf") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		files[name] = filepath.Join(conn.Path, entry.Name())
	}

	return files, nil
}

// tableFile resolves a (possibly qualified) table name to its file path. Table
// names are matched without case, as the file system of the data source may be
// case sensitive.
func (conn *DbaseConn) tableFile(tableName string) (filePath string, err error) {
	name := dbaseTableName(tableName)

	files, err := conn.tableFiles()
	if err != nil {
		return "", err
	}

	if filePath, ok := files[name]; ok {
		return filePath, nil
	}

	for fileName, path := range files {
		if strings.EqualFold(fileName, name) {
			return path, nil
		}
	}

	available := sortedTableNames(files)
	if len(available) == 0 {
		return "", g.Error("dbf table not found: %s (no .dbf files under %s)", tableName, conn.Path)
	}
	return "", g.Error("dbf table not found: %s (available: %s)", tableName, strings.Join(available, ", "))
}

// dbaseTableName returns the table name of a table reference, which may be
// quoted and qualified (`"main"."customers"`, `main.customers` or `customers`)
func dbaseTableName(tableName string) string {
	parts := strings.Split(strings.TrimSpace(tableName), ".")
	return strings.Trim(strings.TrimSpace(parts[len(parts)-1]), `"`)
}

// openTable opens the table file for reading
func (conn *DbaseConn) openTable(tableName string) (file *dbase.File, filePath string, err error) {
	filePath, err = conn.tableFile(tableName)
	if err != nil {
		return nil, "", err
	}

	cfg := &dbase.Config{
		Filename: filePath,
		ReadOnly: true,
		// dBase III/IV and FoxBase files are common and are not on the reader
		// library's tested list; the header and column definitions are still
		// validated when the table is opened.
		Untested:   true,
		TrimSpaces: conn.trimSpaces(),
	}

	if codePage := strings.TrimSpace(conn.GetProp("code_page")); codePage != "" {
		mark, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(codePage), "0x"), lo.Ternary(strings.HasPrefix(strings.ToLower(codePage), "0x"), 16, 10), 8)
		if err != nil {
			return nil, "", g.Error(err, "invalid code_page: %s (expected a code page mark, e.g. 0x03)", codePage)
		}
		cfg.Converter = dbase.ConverterFromCodePage(byte(mark))
	}

	file, err = dbase.OpenTable(cfg)
	if err != nil {
		return nil, "", g.Error(err, "could not open dbf file: %s", filePath)
	}
	return file, filePath, nil
}

// checkDbfMemo returns an error when a table has a memo field whose memo file
// cannot be read. The reader only resolves a FoxPro `.fpt` file, so a dBase
// III/IV table with a `.dbt` memo would otherwise fail row by row with an
// opaque error.
func checkDbfMemo(file *dbase.File, filePath string) error {
	hasMemo := false
	for _, column := range file.Columns() {
		if dbase.DataType(column.DataType) == dbase.Memo {
			hasMemo = true
			break
		}
	}

	if !hasMemo {
		return nil
	}

	if _, related := file.GetHandle(); related != nil {
		return nil
	}

	memoFile := strings.TrimSuffix(filePath, filepath.Ext(filePath)) + ".dbt"
	if _, err := os.Stat(memoFile); err != nil {
		memoFile = strings.TrimSuffix(filePath, filepath.Ext(filePath)) + ".DBT"
	}
	if _, err := os.Stat(memoFile); err == nil {
		return g.Error("dbf table %s has a memo field, but its memo file is not a FoxPro `.fpt` file: %s is not supported by the reader", filepath.Base(filePath), memoFile)
	}

	return g.Error("dbf table %s has a memo field, but no readable memo file (.fpt) was found for it", filepath.Base(filePath))
}

// trimSpaces returns whether string values should be trimmed (default true)
func (conn *DbaseConn) trimSpaces() bool {
	if val := conn.GetProp("trim_spaces"); val != "" {
		return cast.ToBool(val)
	}
	return true
}

// makeColumns converts the table's column definitions into sling columns. The
// general type comes from the connection template's `native_type_map`, so it
// can be overridden like any other connection's type mapping.
func (conn *DbaseConn) makeColumns(file *dbase.File, tableName string) iop.Columns {
	columns := make(iop.Columns, 0, len(file.Columns()))
	names := map[string]bool{}
	for i, column := range file.Columns() {
		dataType := dbase.DataType(column.DataType)

		col := iop.Column{
			Position: i + 1,
			Name:     uniqueDbfColumnName(names, column.Name()),
			Type:     iop.NativeTypeToGeneral(column.Name(), dbfTypeName(dataType), conn.GetType()),
			DbType:   dbfNativeType(column),
			Sourced:  true,
			Table:    tableName,
			Schema:   conn.GetProp("schema"),
			Database: conn.GetProp("schema"),
		}

		// a dBase numeric is an exact decimal, and the template maps the bare
		// `numeric` name to a whole number
		if dataType == dbase.Numeric {
			col.DbPrecision = int(column.Length)
			col.DbScale = int(column.Decimals)
			if column.Decimals > 0 {
				col.Type = iop.DecimalType
			}
		}

		columns = append(columns, col)
	}
	return columns
}

// uniqueDbfColumnName returns a name that is not yet used, appending a number to
// repeated ones (`Point_ID`, `Point_ID1`, ...). A dBase table can hold columns
// with the same name, which sling cannot tell apart, so they are made unique as
// the file readers do with repeated headers.
func uniqueDbfColumnName(used map[string]bool, name string) string {
	for i := 0; ; i++ {
		candidate := lo.Ternary(i == 0, name, g.F("%s%d", name, i))
		if !used[strings.ToLower(candidate)] {
			used[strings.ToLower(candidate)] = true
			return candidate
		}
	}
}

// dbfTypeName returns the dBase type name of a column
func dbfTypeName(dataType dbase.DataType) string {
	switch dataType {
	case dbase.Character:
		return "character"
	case dbase.Varchar:
		return "varchar"
	case dbase.Memo:
		return "memo"
	case dbase.Numeric:
		return "numeric"
	case dbase.Float:
		return "float"
	case dbase.Currency:
		return "currency"
	case dbase.Double:
		return "double"
	case dbase.Integer:
		return "integer"
	case dbase.Date:
		return "date"
	case dbase.DateTime:
		return "datetime"
	case dbase.Logical:
		return "logical"
	case dbase.Blob:
		return "blob"
	case dbase.General:
		return "general"
	case dbase.Picture:
		return "picture"
	case dbase.Varbinary:
		return "varbinary"
	}
	return "unknown"
}

// dbfNativeType returns the dBase type of a column with its length, as stored
// in the table definition (e.g. `character(20)`, `numeric(16,6)`)
func dbfNativeType(column *dbase.Column) string {
	switch dbase.DataType(column.DataType) {
	case dbase.Numeric, dbase.Float:
		if column.Decimals > 0 {
			return g.F("%s(%d,%d)", dbfTypeName(dbase.DataType(column.DataType)), column.Length, column.Decimals)
		}
	case dbase.Character, dbase.Varchar, dbase.Memo, dbase.Blob, dbase.General, dbase.Picture, dbase.Varbinary:
		return g.F("%s(%d)", dbfTypeName(dbase.DataType(column.DataType)), column.Length)
	}
	return dbfTypeName(dbase.DataType(column.DataType))
}

// GetTableColumns returns the columns of a table, read from the table header
func (conn *DbaseConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	file, _, err := conn.openTable(table.Name)
	if err != nil {
		return columns, err
	}
	defer file.Close()

	allColumns := conn.makeColumns(file, table.Name)
	if len(fields) == 0 {
		return allColumns, nil
	}

	columns = make(iop.Columns, 0, len(fields))
	for _, field := range fields {
		col := allColumns.GetColumn(field)
		if col == nil {
			return nil, g.Error("provided field '%s' not found in table %s", field, table.FullName())
		}
		col.Position = len(columns) + 1
		columns = append(columns, *col)
	}

	if len(columns) == 0 {
		return columns, g.Error("did not find any columns for %s", table.FullName())
	}
	return columns, nil
}

// GetSQLColumns returns the columns of a statement, resolved from the table
// definition rather than by executing the statement
func (conn *DbaseConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	if !table.IsQuery() {
		return conn.GetTableColumns(&table)
	}

	sel, err := parseDbaseSelect(table.SQL)
	if err != nil {
		return columns, err
	}

	file, _, err := conn.openTable(sel.table)
	if err != nil {
		return columns, err
	}
	defer file.Close()

	columns, _, err = selectDbaseColumns(conn.makeColumns(file, dbaseTableName(sel.table)), sel.fields)
	return columns, err
}

// GetCount returns the number of records of a table, excluding deleted ones
func (conn *DbaseConn) GetCount(tableFName string) (int64, error) {
	file, _, err := conn.openTable(tableFName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// the record count in the header includes deleted records, which are not
	// part of the table
	var count int64
	for !file.EOF() {
		deleted, err := file.Deleted()
		if err != nil {
			return count, g.Error(err, "could not read deleted flag of record %d", file.Pointer())
		} else if !deleted {
			count++
		}
		file.Skip(1)
	}

	return count, nil
}

// TableExists returns whether the table file exists
func (conn *DbaseConn) TableExists(table Table) (exists bool, err error) {
	_, err = conn.tableFile(table.Name)
	if err != nil {
		if strings.Contains(err.Error(), "table not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetSchemas returns the schemas of the connection
func (conn *DbaseConn) GetSchemas() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]interface{}{conn.GetProp("schema")})
	return data, nil
}

// GetTables returns the tables of a schema
func (conn *DbaseConn) GetTables(schema string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "is_view"))

	files, err := conn.tableFiles()
	if err != nil {
		return data, err
	}

	schema = lo.Ternary(schema == "", conn.GetProp("schema"), schema)
	for _, name := range sortedTableNames(files) {
		data.Append([]interface{}{schema, name, false})
	}
	return data, nil
}

// GetViews returns no views: dBase tables have no views
func (conn *DbaseConn) GetViews(schema string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "is_view"))
	return data, nil
}

// GetPrimaryKeys returns no keys: dBase tables expose no key metadata
func (conn *DbaseConn) GetPrimaryKeys(tableFName string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("pk_name", "position", "column_name"))
	return data, nil
}

// GetIndexes returns no indexes: index files (.cdx/.idx) are not readable
func (conn *DbaseConn) GetIndexes(tableFName string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("table_name", "column_name"))
	return data, nil
}

// CurrentDatabase returns the database name
func (conn *DbaseConn) CurrentDatabase() (dbName string, err error) {
	return conn.GetProp("schema"), nil
}

// CurrentSchema returns the schema name
func (conn *DbaseConn) CurrentSchema() (schemaName string, err error) {
	return conn.GetProp("schema"), nil
}

// GetDatabases returns the databases of the connection
func (conn *DbaseConn) GetDatabases() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("name"))
	data.Append([]interface{}{conn.GetProp("schema")})
	return data, nil
}

// GetSchemata obtains the schemata of the connection
func (conn *DbaseConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	files, err := conn.tableFiles()
	if err != nil {
		return schemata, err
	}

	schemaName = lo.Ternary(schemaName == "", conn.GetProp("schema"), schemaName)
	schema := Schema{
		Name:   schemaName,
		Tables: map[string]Table{},
	}

	filters := lo.Filter(tableNames, func(name string, _ int) bool { return strings.TrimSpace(name) != "" })

	for _, tableName := range sortedTableNames(files) {
		if len(filters) > 0 && !g.IsMatched(filters, tableName) {
			continue
		}

		table := Table{
			Name:     tableName,
			Schema:   schemaName,
			Database: schemaName,
			Dialect:  conn.GetType(),
		}

		if level == SchemataLevelColumn {
			columns, err := conn.GetTableColumns(&table)
			if err != nil {
				return schemata, err
			}
			table.Columns = columns
		}

		schema.Tables[strings.ToLower(tableName)] = table
	}

	schemata.Databases[strings.ToLower(schemaName)] = Database{
		Name:    schemaName,
		Schemas: map[string]Schema{strings.ToLower(schemaName): schema},
	}

	return schemata, nil
}

// StreamRowsContext streams the rows of a statement. Only the select form sling
// generates for a table read is supported: a field list, plus limit / offset.
func (conn *DbaseConn) StreamRowsContext(ctx context.Context, query string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(options)

	sel, err := parseDbaseSelect(query)
	if err != nil {
		return ds, err
	}

	if limit := cast.ToInt(opts["limit"]); limit > 0 && (sel.limit == 0 || limit < sel.limit) {
		sel.limit = limit
	}

	file, filePath, err := conn.openTable(sel.table)
	if err != nil {
		return ds, err
	}

	if err := checkDbfMemo(file, filePath); err != nil {
		file.Close()
		return ds, err
	}

	columns, colIndexes, err := selectDbaseColumns(conn.makeColumns(file, dbaseTableName(sel.table)), sel.fields)
	if err != nil {
		file.Close()
		return ds, err
	}

	queryContext := g.NewContext(ctx)
	nextFunc, closeFunc := conn.newRowIterator(file, filePath, len(columns), colIndexes, sel)

	ds = iop.NewDatastreamIt(queryContext.Ctx, columns, nextFunc)
	ds.Defer(closeFunc)
	ds.NoDebug = strings.Contains(query, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()
	ds.Metadata.StreamURL.Value = filePath
	conn.LogSQL(query)
	if !ds.NoDebug {
		// don't set metadata for internal queries
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}
	return ds, nil
}

// newRowIterator returns the row provider for a table read, along with the
// cleanup function that releases the table file
func (conn *DbaseConn) newRowIterator(file *dbase.File, filePath string, colCount int, colIndexes []int, sel dbaseSelect) (nextFunc func(it *iop.Iterator) bool, closeFunc func()) {
	fields := file.Columns()
	offsets := dbfFieldOffsets(fields)
	nullFlags := newDbfNullFlags(filePath, fields, conn.trimSpaces())
	header := file.Header()

	// a blank number / date / logical can only be told from a real zero value
	// by looking at the raw record, as can the variable length of a varchar.
	// The raw record is read through the same file handle the reader library
	// uses: os.File.ReadAt does not move that handle's offset.
	var rawFile *os.File
	if handle, _ := file.GetHandle(); handle != nil {
		rawFile, _ = handle.(*os.File)
	}

	rawBuf := make([]byte, header.RowLength)
	limit := sel.limit
	offset := sel.offset
	noRows := sel.noRows

	nextFunc = func(it *iop.Iterator) bool {
		for !file.EOF() {
			record, err := file.Next()
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "could not read row %d of %s", file.Pointer(), file.TableName()))
				return false
			} else if record.Deleted {
				continue // deleted records are not part of the table
			} else if offset > 0 {
				offset--
				continue
			} else if noRows || (limit > 0 && it.Counter >= uint64(limit)) {
				return false
			}

			raw := readDbfRecord(rawFile, rawBuf, header, record.Position)
			recordFields := record.Fields()

			row := make([]any, colCount)
			for i, colIndex := range colIndexes {
				if colIndex >= len(recordFields) {
					continue
				}
				field := recordFields[colIndex]
				if value, ok := nullFlags.variableValue(raw, field, offsets[colIndex], colIndex); ok {
					row[i] = value
				} else {
					row[i] = dbfValue(field, raw, offsets[colIndex])
				}
			}
			it.Row = row
			return true
		}
		return false
	}

	closeFunc = func() { file.Close() }

	return nextFunc, closeFunc
}

// readDbfRecord returns the raw bytes of a record, or nil when they are not
// available. The buffer is reused across rows.
func readDbfRecord(rawFile *os.File, buf []byte, header *dbase.Header, position uint32) []byte {
	if rawFile == nil || len(buf) == 0 {
		return nil
	}

	offset := int64(header.FirstRow) + int64(position)*int64(header.RowLength)
	n, err := rawFile.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil
	} else if n != len(buf) {
		return nil
	}
	return buf
}

// dbfFieldBits are the `_NullFlags` bit positions of a varchar / varbinary
// field: the variable length marker and the null marker
type dbfFieldBits struct {
	varlen int // -1 when the field holds no variable length marker
	null   int // -1 when the field holds no null marker
}

// dbfNullFlags resolves the `_NullFlags` bits of a table's records. The reader
// resolves those bits from the first varchar / varbinary field of the table for
// every field, so a table holding more than one of them reads wrong
// variable-length markers; the bits are resolved here instead, from the flag
// bytes of the record.
type dbfNullFlags struct {
	offset     int            // byte offset of the `_NullFlags` field within a record
	length     int            // length of the `_NullFlags` field in bytes
	trimSpaces bool           // whether string values are trimmed
	bits       []dbfFieldBits // indexed by field position
}

// newDbfNullFlags returns the `_NullFlags` layout of a table, or nil when the
// table holds no flag field
func newDbfNullFlags(filePath string, columns []*dbase.Column, trimSpaces bool) *dbfNullFlags {
	offset, length, ok := dbfNullFlagField(filePath)
	if !ok {
		return nil
	}

	flags := &dbfNullFlags{
		offset:     offset,
		length:     length,
		trimSpaces: trimSpaces,
		bits:       make([]dbfFieldBits, len(columns)),
	}

	// a variable length marker per varchar / varbinary field, plus a null marker
	// for the nullable ones, in the order of the table definition
	bit := 0
	for i, column := range columns {
		flags.bits[i] = dbfFieldBits{varlen: -1, null: -1}
		if !g.In(dbase.DataType(column.DataType), dbase.Varchar, dbase.Varbinary) {
			continue
		}
		flags.bits[i].varlen = bit
		bit++
		if column.Flag.Has(byte(dbase.NullableFlag)) {
			flags.bits[i].null = bit
			bit++
		}
	}

	return flags
}

// dbfNullFlagField returns the byte offset and length of the `_NullFlags` field
// of a table, as recorded in its column definitions
func dbfNullFlagField(filePath string) (offset int, length int, ok bool) {
	rawFile, err := os.Open(filePath)
	if err != nil {
		return 0, 0, false
	}
	defer rawFile.Close()

	header := make([]byte, 32)
	if _, err := rawFile.ReadAt(header, 0); err != nil {
		return 0, 0, false
	}

	firstRow := int(binary.LittleEndian.Uint16(header[8:10]))
	descriptor := make([]byte, 32)
	for pos := 32; pos+len(descriptor) <= firstRow; pos += len(descriptor) {
		if _, err := rawFile.ReadAt(descriptor, int64(pos)); err != nil {
			return 0, 0, false
		} else if descriptor[0] == byte(dbase.ColumnEnd) {
			break
		}

		name := string(bytes.TrimRight(descriptor[:11], "\x00"))
		if !strings.EqualFold(name, "_NullFlags") {
			continue
		}
		return int(binary.LittleEndian.Uint32(descriptor[12:16])), int(descriptor[16]), true
	}

	return 0, 0, false
}

// bit returns the value of a `_NullFlags` bit of a record
func (flags *dbfNullFlags) bit(raw []byte, index int) (value bool, ok bool) {
	if flags == nil || index < 0 || index >= flags.length*8 {
		return false, false
	} else if flags.offset+flags.length > len(raw) {
		return false, false
	}

	return raw[flags.offset+index/8]&(1<<(index%8)) != 0, true
}

// variableValue returns the value of a variable length varchar / varbinary
// field, as recorded by the `_NullFlags` bits of the record. The second return
// value reports whether the field is a flagged one, and so whether the value of
// the reader library is to be replaced.
func (flags *dbfNullFlags) variableValue(raw []byte, field *dbase.Field, offset int, colIndex int) (value any, ok bool) {
	if flags == nil || raw == nil {
		return nil, false
	}

	column := field.Column()
	if !g.In(dbase.DataType(column.DataType), dbase.Varchar, dbase.Varbinary) {
		return nil, false
	}

	length := int(column.Length)
	if offset < 0 || offset+length > len(raw) {
		return nil, false
	}

	if colIndex < 0 || colIndex >= len(flags.bits) {
		return nil, false
	}

	bits := flags.bits[colIndex]
	if bits.varlen < 0 {
		return nil, false
	}

	if null, _ := flags.bit(raw, bits.null); null {
		return nil, true
	}

	if varlen, _ := flags.bit(raw, bits.varlen); !varlen {
		return nil, false // a fixed length value is read correctly
	}

	// the last byte of the field holds the length of the value
	if size := int(raw[offset+length-1]); size <= length {
		raw = raw[offset : offset+size]
	} else {
		raw = raw[offset : offset+length]
	}

	if !flags.trimSpaces {
		raw = bytes.ReplaceAll(raw, []byte{0x00}, []byte{})
	} else {
		raw = bytes.TrimSpace(bytes.ReplaceAll(raw, []byte{0x00}, []byte{}))
	}

	if dbase.DataType(column.DataType) == dbase.Varbinary {
		return raw, true
	}
	return string(raw), true
}

// dbfValue returns the value of one column of a record, mapping a blank field
// to NULL
func dbfValue(field *dbase.Field, raw []byte, offset int) any {
	if field == nil {
		return nil
	}

	value := field.GetValue()

	if g.In(field.Type(), dbfTextNullTypes...) {
		// a blank number / date / logical is stored as spaces (datetime as
		// NULs), which the reader library reports as a zero value
		length := int(field.Column().Length)
		if raw != nil && offset >= 0 && offset+length <= len(raw) && dbfBlank(raw[offset:offset+length], field.Type()) {
			return nil
		}
		return value
	}

	// an empty memo / varchar / varbinary / blob is reported as an empty slice
	if bytes, ok := value.([]byte); ok && len(bytes) == 0 {
		return nil
	}

	return value
}

// dbfFieldOffsets returns the byte offset of every column within a record. The
// reader library parses the fields sequentially after the delete flag and does
// not fill in the column displacement, so it is computed here.
func dbfFieldOffsets(columns []*dbase.Column) (offsets []int) {
	offsets = make([]int, len(columns))
	offset := 1 // the delete flag
	for i, column := range columns {
		offsets[i] = offset
		offset += int(column.Length)
	}
	return
}

// dbfBlank returns whether the raw bytes of a field hold no value
func dbfBlank(raw []byte, dataType dbase.DataType) bool {
	for _, b := range raw {
		if b == ' ' || b == 0x00 {
			continue
		} else if dataType == dbase.Logical && b == '?' {
			continue // '?' marks an undetermined logical
		}
		return false
	}
	return true
}

// dbaseSelect is the parsed form of a supported statement
type dbaseSelect struct {
	fields []dbaseField
	table  string
	limit  int
	offset int
	noRows bool // `where 1=0`: columns only, no rows
}

// dbaseField is a selected column, with an optional alias
type dbaseField struct {
	name  string
	alias string
}

// dbaseClauseKeywords are the keywords that can follow the table reference
var dbaseClauseKeywords = []string{"where", "order by", "group by", "having", "limit", "offset", "union", "join"}

// parseDbaseSelect parses the statements the connector accepts: a select of
// plain columns from one table, with limit / offset. dBase has no query engine,
// so anything else is rejected instead of being silently ignored.
func parseDbaseSelect(sql string) (sel dbaseSelect, err error) {
	statement := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))

	if !hasKeywordPrefix(statement, "select") {
		return sel, g.Error("dBase connections only support `select <fields> from <table>`, got: %s", strings.TrimSpace(sql))
	}

	remainder := strings.TrimSpace(statement[len("select"):])

	fromIndex := findDbaseKeyword(remainder, "from")
	if fromIndex < 0 {
		return sel, g.Error("dBase connections only support `select <fields> from <table>`, got: %s", strings.TrimSpace(sql))
	}

	sel.fields, err = parseDbaseFields(remainder[:fromIndex])
	if err != nil {
		return sel, err
	}

	remainder = strings.TrimSpace(remainder[fromIndex+len("from"):])

	// the table reference ends at the first clause keyword
	tableEnd := len(remainder)
	for _, keyword := range dbaseClauseKeywords {
		if index := findDbaseKeyword(remainder, keyword); index >= 0 && index < tableEnd {
			tableEnd = index
		}
	}

	tableRef := strings.TrimSpace(remainder[:tableEnd])
	clauses := strings.TrimSpace(remainder[tableEnd:])

	if err = parseDbaseClauses(clauses, &sel, statement); err != nil {
		return sel, err
	}

	if !strings.HasPrefix(tableRef, "(") {
		sel.table = strings.Trim(tableRef, `"`)
		if sel.table == "" || strings.ContainsAny(sel.table, "(),") {
			return sel, g.Error("dBase connections only support reading one table, got: %s", strings.TrimSpace(sql))
		}
		return sel, nil
	}

	// a derived table over a single table read: `select * from (<select>) as t
	// limit N`, which is how sling applies a limit to a query statement
	inner, rest, err := splitDbaseParens(tableRef)
	if err != nil {
		return sel, err
	}

	alias := strings.TrimSpace(rest)
	if hasKeywordPrefix(alias, "as") {
		alias = strings.TrimSpace(alias[len("as"):])
	}
	alias = strings.Trim(alias, `"`)
	if strings.ContainsAny(alias, " \t\r\n(),") {
		return sel, g.Error("dBase connections only support reading one table, got: %s", strings.TrimSpace(sql))
	}

	innerSel, err := parseDbaseSelect(inner)
	if err != nil {
		return sel, err
	}

	if !isDbaseStar(sel.fields) {
		if !isDbaseStar(innerSel.fields) {
			return sel, g.Error("dBase connections do not support selecting from a query, got: %s", strings.TrimSpace(sql))
		}
		innerSel.fields = sel.fields
	}

	// the outer limit / offset applies to the result of the inner statement
	innerSel.offset += sel.offset
	if innerSel.limit > 0 {
		innerSel.limit = lo.Ternary(innerSel.limit > sel.offset, innerSel.limit-sel.offset, 0)
	}
	if sel.limit > 0 && (innerSel.limit == 0 || sel.limit < innerSel.limit) {
		innerSel.limit = sel.limit
	}
	innerSel.noRows = innerSel.noRows || sel.noRows

	return innerSel, nil
}

// parseDbaseClauses parses the clauses of a statement into sel
func parseDbaseClauses(clauses string, sel *dbaseSelect, statement string) (err error) {
	for clauses != "" {
		switch {
		case hasKeywordPrefix(clauses, "limit"):
			value, rest, err := splitDbaseClause(clauses[len("limit"):])
			if err != nil {
				return err
			}
			if sel.limit, err = strconv.Atoi(value); err != nil {
				return g.Error(err, "invalid limit value: %s", value)
			}
			clauses = rest
		case hasKeywordPrefix(clauses, "offset"):
			value, rest, err := splitDbaseClause(clauses[len("offset"):])
			if err != nil {
				return err
			}
			if sel.offset, err = strconv.Atoi(value); err != nil {
				return g.Error(err, "invalid offset value: %s", value)
			}
			clauses = rest
		case hasKeywordPrefix(clauses, "where"):
			value, rest, err := splitDbaseClause(clauses[len("where"):])
			if err != nil {
				return err
			}
			switch strings.ToLower(strings.Trim(value, "() ")) {
			case "1=0", "false":
				sel.noRows = true // used to read the columns only
			case "1=1", "true":
			default:
				return g.Error("dBase connections do not support `where` filters, got: %s", value)
			}
			clauses = rest
		default:
			return g.Error("dBase connections do not support this statement: %s", statement)
		}
	}

	return nil
}

// parseDbaseFields parses the select list
func parseDbaseFields(list string) (fields []dbaseField, err error) {
	exprs := splitDbaseList(list)

	for _, expr := range exprs {
		field, alias, _, err := iop.ParseSelectExpr(expr)
		if err != nil {
			return nil, err
		}

		field = strings.Trim(strings.TrimSpace(field), `"`)
		alias = strings.Trim(strings.TrimSpace(alias), `"`)

		if field == "*" {
			if len(exprs) > 1 {
				return nil, g.Error("dBase connections cannot combine `*` with other columns, got: %s", list)
			}
			fields = append(fields, dbaseField{})
			continue
		} else if field == "" || strings.ContainsAny(field, "()*") {
			return nil, g.Error("dBase connections only support selecting plain columns, got: %s", expr)
		}

		fields = append(fields, dbaseField{name: field, alias: alias})
	}

	if len(fields) == 0 {
		return nil, g.Error("no fields selected")
	}
	return fields, nil
}

// selectDbaseColumns returns the columns to stream, and their position in the
// table definition
func selectDbaseColumns(columns iop.Columns, fields []dbaseField) (selected iop.Columns, indexes []int, err error) {
	if len(fields) == 1 && fields[0].name == "" {
		indexes = lo.Range(len(columns))
		return columns, indexes, nil
	}

	for _, field := range fields {
		index := -1
		for i, col := range columns {
			if strings.EqualFold(col.Name, field.name) {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, nil, g.Error("column not found: %s (available: %s)", field.name, strings.Join(columns.Names(), ", "))
		}

		col := columns[index]
		if field.alias != "" {
			col.Name = field.alias
		}
		selected = append(selected, col)
		indexes = append(indexes, index)
	}

	for i := range selected {
		selected[i].Position = i + 1
	}
	return selected, indexes, nil
}

// findDbaseKeyword returns the index of a keyword at the top level of a
// statement (outside quotes and parentheses), or -1
func findDbaseKeyword(text string, keyword string) int {
	lower := strings.ToLower(text)
	quoted := false
	depth := 0

	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '"', '`':
			quoted = !quoted
			continue
		case '(':
			if !quoted {
				depth++
			}
			continue
		case ')':
			if !quoted {
				depth--
			}
			continue
		}

		if quoted || depth != 0 || !strings.HasPrefix(lower[i:], keyword) {
			continue
		} else if i > 0 && isDbaseWordChar(lower[i-1]) {
			continue
		} else if i+len(keyword) < len(lower) && isDbaseWordChar(lower[i+len(keyword)]) {
			continue
		}

		return i
	}

	return -1
}

// splitDbaseList splits a comma separated list at the top level
func splitDbaseList(text string) (parts []string) {
	quoted := false
	depth := 0
	start := 0

	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '"', '`':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
			}
		case ',':
			if !quoted && depth == 0 {
				parts = append(parts, strings.TrimSpace(text[start:i]))
				start = i + 1
			}
		}
	}

	return append(parts, strings.TrimSpace(text[start:]))
}

// isDbaseStar returns whether the select list is `*`
func isDbaseStar(fields []dbaseField) bool {
	return len(fields) == 1 && fields[0].name == ""
}

// splitDbaseParens splits a parenthesized expression from the text that follows
func splitDbaseParens(text string) (inner string, rest string, err error) {
	quoted := false
	depth := 0

	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '"', '`':
			quoted = !quoted
			continue
		case '(':
			if !quoted {
				depth++
			}
			continue
		case ')':
			if !quoted {
				depth--
			}
			if depth == 0 {
				return text[1:i], text[i+1:], nil
			}
		}
	}

	return "", "", g.Error("unbalanced parenthesis in statement: %s", text)
}

// splitDbaseClause splits a clause value from the remaining clauses
func splitDbaseClause(text string) (value string, rest string, err error) {
	end := len(text)
	for _, keyword := range dbaseClauseKeywords {
		if index := findDbaseKeyword(text, keyword); index >= 0 && index < end {
			end = index
		}
	}

	value = strings.TrimSpace(text[:end])
	rest = strings.TrimSpace(text[end:])
	if value == "" {
		return "", "", g.Error("missing value for clause in: %s", strings.TrimSpace(text))
	}
	return value, rest, nil
}

// hasKeywordPrefix returns whether the text starts with the keyword
func hasKeywordPrefix(text string, keyword string) bool {
	if len(text) < len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) {
		return false
	}
	return len(text) == len(keyword) || !isDbaseWordChar(text[len(keyword)])
}

// isDbaseWordChar returns whether the byte can be part of an identifier
func isDbaseWordChar(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// sortedTableNames returns the table names of a connection, in order
func sortedTableNames(files map[string]string) (names []string) {
	names = lo.Keys(files)
	sort.Strings(names)
	return names
}
