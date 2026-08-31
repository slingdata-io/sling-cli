package iop

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

var (
	// RemoveTrailingDecZeros removes the trailing zeros in CastToString
	RemoveTrailingDecZeros    = false
	SampleSize                = 900
	replacePattern            = regexp.MustCompile("[^_0-9a-zA-Z]+") // to clean header fields
	regexFirstDigit           = *regexp.MustCompile(`^\d`)
	parseConstraintExpression = func(string) (ConstraintEvalFunc, error) { return nil, nil }
)

// Column represents a schemata column
type Column struct {
	Position    int          `json:"position"`
	Name        string       `json:"name"`
	Type        ColumnType   `json:"type"`
	DbType      string       `json:"db_type,omitempty"`
	DbPrecision int          `json:"db_precision,omitempty"`
	DbScale     int          `json:"db_scale,omitempty"`
	Sourced     bool         `json:"-"` // whether col was sourced/inferred from a typed source
	Stats       ColumnStats  `json:"stats,omitempty"`
	goType      reflect.Type `json:"-"`

	Table       string `json:"table,omitempty"`
	Schema      string `json:"schema,omitempty"`
	Database    string `json:"database,omitempty"`
	Description string `json:"description,omitempty"`
	FileURI     string `json:"file_uri,omitempty"`

	Constraint *ColumnConstraint `json:"constraint,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Columns represent many columns
type Columns []Column

type ColumnType string

const (
	BigIntType     ColumnType = "bigint"
	BinaryType     ColumnType = "binary"
	BoolType       ColumnType = "bool"
	DateType       ColumnType = "date"
	DatetimeType   ColumnType = "datetime"
	DecimalType    ColumnType = "decimal"
	IntegerType    ColumnType = "integer"
	JsonType       ColumnType = "json"
	SmallIntType   ColumnType = "smallint"
	StringType     ColumnType = "string"
	UUIDType       ColumnType = "uuid"
	TextType       ColumnType = "text"
	TimestampType  ColumnType = "timestamp"
	TimestampzType ColumnType = "timestampz"
	FloatType      ColumnType = "float"
	TimeType       ColumnType = "time"
	TimezType      ColumnType = "timez"
	GeometryType   ColumnType = "geometry"
)

type ConstraintEvalFunc func(value any) bool

type ColumnConstraint struct {
	Expression string             `json:"expression,omitempty"`
	Errors     []string           `json:"errors,omitempty"`
	FailCnt    uint64             `json:"fail_cnt,omitempty"`
	EvalFunc   ConstraintEvalFunc `json:"-"`
}

type KeyType string

const (
	AggregateKey    KeyType = "aggregate"
	ClusterKey      KeyType = "cluster"
	DistributionKey KeyType = "distribution"
	DuplicateKey    KeyType = "duplicate"
	HashKey         KeyType = "hash"
	IndexKey        KeyType = "index"
	PartitionKey    KeyType = "partition"
	PrimaryKey      KeyType = "primary"
	SortKey         KeyType = "sort"
	UniqueKey       KeyType = "unique"
	UpdateKey       KeyType = "update"
)

func (kt KeyType) MetadataKey() string {
	return string(kt) + "_key"
}

var KeyTypes = []KeyType{AggregateKey, ClusterKey, DuplicateKey, HashKey, IndexKey, PartitionKey, PrimaryKey, SortKey, UniqueKey, UpdateKey}

// ColMetaKey represents a standardized column metadata key for schema migration
type ColMetaKey string

const (
	// Auto-increment/Identity
	ColMetaAutoIncrement ColMetaKey = "auto_increment"     // "true" if column is auto-increment
	ColMetaIdentitySeed  ColMetaKey = "identity_seed"      // Starting value (e.g., "1")
	ColMetaIdentityIncr  ColMetaKey = "identity_increment" // Increment step (e.g., "1")

	// Nullable
	ColMetaNullable ColMetaKey = "nullable" // "true" or "false"

	// Default Value (stores generalized form)
	ColMetaDefaultValue ColMetaKey = "default_value"

	// Primary Key
	ColMetaIsPrimaryKey ColMetaKey = "is_primary_key" // "true" if column is part of primary key

	// Unique
	ColMetaUnique ColMetaKey = "unique" // "true" if column has a UNIQUE constraint

	// Index (JSON-encoded []IndexDef contributed by this column's inline index modifiers)
	ColMetaIndex ColMetaKey = "index"

	// DDLExplicit marks that DDL constraints were declared explicitly via the
	// columns: modifier DSL (not_null, primary_key, unique, description), so they
	// should be rendered at CREATE time even when schema-migration is not enabled.
	ColMetaDDLExplicit ColMetaKey = "ddl_explicit"

	// Foreign Keys (JSON struct)
	ColMetaForeignKey ColMetaKey = "foreign_key"

	// Check Constraints (native syntax, future AI translation)
	ColMetaCheckConstraint ColMetaKey = "check_constraint"

	// Column Description/Comment
	ColMetaDescription ColMetaKey = "description"
)

func (cmk ColMetaKey) String() string {
	return string(cmk)
}

// ColMetaKeys is a list of all column metadata keys
var ColMetaKeys = []ColMetaKey{
	ColMetaAutoIncrement, ColMetaIdentitySeed, ColMetaIdentityIncr,
	ColMetaNullable, ColMetaDefaultValue, ColMetaIsPrimaryKey, ColMetaUnique,
	ColMetaIndex, ColMetaDDLExplicit, ColMetaForeignKey,
	ColMetaCheckConstraint, ColMetaDescription,
}

// DefaultExpr represents a generalized default expression
// These are database-agnostic representations translated to native syntax
type DefaultExpr string

const (
	// Timestamp/DateTime defaults
	DefaultExprCurrentTimestamp    DefaultExpr = "current_timestamp"
	DefaultExprCurrentTimestampUTC DefaultExpr = "current_timestamp_utc"
	DefaultExprCurrentDate         DefaultExpr = "current_date"
	DefaultExprCurrentTime         DefaultExpr = "current_time"

	// UUID defaults
	DefaultExprUUID           DefaultExpr = "uuid()"
	DefaultExprUUIDSequential DefaultExpr = "uuid_sequential()"

	// Boolean defaults
	DefaultExprTrue  DefaultExpr = "true"
	DefaultExprFalse DefaultExpr = "false"

	// Null
	DefaultExprNull DefaultExpr = "null"
)

func (de DefaultExpr) String() string {
	return string(de)
}

// DefaultExprs is a list of all standard default expressions
var DefaultExprs = []DefaultExpr{
	DefaultExprCurrentTimestamp, DefaultExprCurrentTimestampUTC,
	DefaultExprCurrentDate, DefaultExprCurrentTime,
	DefaultExprUUID, DefaultExprUUIDSequential,
	DefaultExprTrue, DefaultExprFalse, DefaultExprNull,
}

// IsStandardDefault checks if a value matches a known generalized default
func IsStandardDefault(value string) bool {
	for _, de := range DefaultExprs {
		if strings.ToLower(value) == de.String() {
			return true
		}
	}
	return false
}

// ForeignKeyInfo represents a foreign key relationship
type ForeignKeyInfo struct {
	ConstraintName   string `json:"constraint_name"`
	ColumnName       string `json:"column_name,omitempty"`
	ReferencedSchema string `json:"referenced_schema"`
	ReferencedTable  string `json:"referenced_table"`
	ReferencedColumn string `json:"referenced_column"`
	OnDelete         string `json:"on_delete,omitempty"`
	OnUpdate         string `json:"on_update,omitempty"`
}

// IndexInfo represents an index
type IndexInfo struct {
	Name     string   `json:"name"`
	Columns  []string `json:"columns"`
	IsUnique bool     `json:"is_unique"`
}

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen       int    `json:"min_len,omitempty"`
	MaxLen       int    `json:"max_len,omitempty"`
	MaxDecLen    int    `json:"max_dec_len,omitempty"`
	Min          int64  `json:"min,omitempty"`
	Max          int64  `json:"max,omitempty"`
	NullCnt      int64  `json:"null_cnt,omitempty"`
	IntCnt       int64  `json:"int_cnt,omitempty"`
	DecCnt       int64  `json:"dec_cnt,omitempty"`
	BoolCnt      int64  `json:"bool_cnt,omitempty"`
	JsonCnt      int64  `json:"json_cnt,omitempty"`
	StringCnt    int64  `json:"string_cnt,omitempty"`
	DateCnt      int64  `json:"date_cnt,omitempty"`
	DateTimeCnt  int64  `json:"datetime_cnt,omitempty"`
	DateTimeZCnt int64  `json:"datetimez_cnt,omitempty"`
	GeometryCnt  int64  `json:"geometry_cnt,omitempty"`
	TotalCnt     int64  `json:"total_cnt,omitempty"`
	UniqCnt      int64  `json:"uniq_cnt,omitempty"`
	Checksum     uint64 `json:"checksum,omitempty"`
	LastVal      any    `json:"-"` // last non-empty value. useful for state incremental
}

func (cs *ColumnStats) DistinctPercent() float64 {
	val := (cs.UniqCnt) * 100 / cs.TotalCnt
	return cast.ToFloat64(val) / 100
}

func (cs *ColumnStats) DuplicateCount() int64 {
	return cs.TotalCnt - cs.UniqCnt
}

func (cs *ColumnStats) DuplicatePercent() float64 {
	val := (cs.TotalCnt - cs.UniqCnt) * 100 / cs.TotalCnt
	return cast.ToFloat64(val) / 100
}

func init() {
	SetSampleSize()
}

func SetSampleSize() {
	if val := os.Getenv("SAMPLE_SIZE"); val != "" {
		SampleSize = cast.ToInt(val) // legacy
	}

	if val := os.Getenv("SLING_SAMPLE_SIZE"); val != "" {
		SampleSize = cast.ToInt(val)
	}

	if os.Getenv("REMOVE_TRAILING_ZEROS") != "" {
		RemoveTrailingDecZeros = cast.ToBool(os.Getenv("REMOVE_TRAILING_ZEROS"))
	}
}

// Row is a row
func Row(vals ...any) []any {
	return vals
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func IsDummy(columns []Column) bool {
	return Columns(columns).IsDummy()
}

// NewColumns normalizes Columns: defaults missing/invalid Type to StringType
// and assigns 1-based Position.
func NewColumns(cols ...Column) Columns {
	for i, col := range cols {
		if string(col.Type) == "" || !col.Type.IsValid() {
			cols[i].Type = StringType
		}
		cols[i].Position = i + 1
	}
	return cols
}

// NewColumnsFromFields creates Columns from fields
func NewColumnsFromFields(fields ...string) (cols Columns) {
	cols = make(Columns, len(fields))
	for i, field := range fields {
		cols[i].Name = field
		cols[i].Position = i + 1
	}
	return
}

func (cols Columns) Data(includeParent bool) (fields []string, rows [][]any) {
	fields = []string{"ID", "Column Name", "Native Type", "General Type"}
	parentIsDB := false
	parentIsFile := false
	rows = lo.Map(cols, func(col Column, i int) []any {
		if includeParent {
			if col.Table != "" {
				parentIsDB = true
				return []any{col.Database, col.Schema, col.Table, col.Position, col.Name, col.DbType, col.Type}
			} else if col.FileURI != "" {
				parentIsFile = true
				if col.DbType == "" {
					col.DbType = "-"
				}
				return []any{col.FileURI, col.Position, col.Name, col.DbType, col.Type}
			}
		}
		return []any{col.Position, col.Name, col.DbType, col.Type}
	})

	sort.Slice(rows, func(i, j int) bool {
		val := func(r []any) string {
			if parentIsDB {
				return g.F("%s-%s-%s-%04d", r[0], r[1], r[2], r[3])
			}
			if parentIsFile {
				return g.F("%s-%04d", r[0], r[1])
			}
			return g.F("%04d", r[0])
		}
		return val(rows[i]) < val(rows[j])
	})

	if includeParent {
		if parentIsDB {
			fields = []string{"Database", "Schema", "Table", "ID", "Column", "Native Type", "General Type"}
		}
		if parentIsFile {
			fields = []string{"File", "ID", "Column", "Native Type", "General Type"}
		}
	}

	return
}

// PrettyTable returns a text pretty table
func (cols Columns) PrettyTable(includeParent bool) (output string) {
	header, rows := cols.Data(includeParent)
	return g.PrettyTable(header, rows)
}

// PrettyTable returns a text pretty table
func (cols Columns) JSON(includeParent bool) (output string) {
	fields, rows := cols.Data(includeParent)
	return g.Marshal(g.M("fields", fields, "rows", rows))
}

// GetKeys gets key columns
func (cols Columns) GetKeys(keyType KeyType) Columns {
	keys := Columns{}
	for _, col := range cols {
		if col.IsKeyType(keyType) {
			keys = append(keys, col)
		}
	}
	return keys
}

// SetKeys sets key columns
func (cols Columns) SetKeys(keyType KeyType, colNames ...string) (err error) {
	for _, colName := range colNames {
		found := false
		for i, col := range cols {
			if strings.EqualFold(colName, col.Name) {
				col.SetMetadata(keyType.MetadataKey(), "true")
				cols[i] = col
				found = true
			}
		}
		if !found && !g.In(keyType, ClusterKey, PartitionKey, SortKey) {
			return g.Error("could not set %s key. Did not find column %s\navailable: %s", keyType, colName, g.Marshal(cols.Names()))
		}
	}
	return
}

// SetMetadata sets metadata for columns
func (cols Columns) SetMetadata(key, value string, colNames ...string) (err error) {
	for _, colName := range colNames {
		for i, col := range cols {
			if strings.EqualFold(colName, col.Name) {
				col.SetMetadata(key, value)
				cols[i] = col
			}
		}
	}
	return
}

// Sourced returns true if the columns are all sourced
func (cols Columns) Sourced() (sourced bool) {
	sourced = true
	for _, col := range cols {
		if !col.Sourced {
			sourced = false
		}
	}
	return sourced
}

// GetMissing returns the missing columns from newCols
func (cols Columns) GetMissing(newCols ...Column) (missing Columns) {
	fm := cols.FieldMap(true)
	for _, col := range newCols {
		if _, ok := fm[strings.ToLower(col.Name)]; !ok {
			missing = append(missing, col)
		}
	}
	return missing
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func (cols Columns) IsDummy() bool {
	for _, col := range cols {
		if !strings.HasPrefix(col.Name, "col_") || len(col.Name) != 8 {
			return false
		}
	}
	return true
}

// Names return the column names
func (cols Columns) Clone() (newCols Columns) {
	newCols = make(Columns, len(cols))
	for j, col := range cols {
		newCols[j] = Column{
			Position:    col.Position,
			Name:        col.Name,
			Description: col.Description,
			Type:        col.Type,
			DbType:      col.DbType,
			DbPrecision: col.DbPrecision,
			DbScale:     col.DbScale,
			Sourced:     col.Sourced,
			Stats:       col.Stats,
			goType:      col.goType,
			Table:       col.Table,
			Schema:      col.Schema,
			Database:    col.Database,
			Metadata:    col.Metadata,
			Constraint:  col.Constraint,
		}
	}
	return newCols
}

// Names return the column names
// args -> (lower bool, cleanUp bool)
func (cols Columns) Names(args ...bool) []string {
	lower := false
	cleanUp := false
	if len(args) > 1 {
		lower = args[0]
		cleanUp = args[1]
	} else if len(args) > 0 {
		lower = args[0]
	}
	fields := make([]string, len(cols))
	for j, column := range cols {
		field := column.Name

		if lower {
			field = strings.ToLower(column.Name)
		}
		if cleanUp {
			field = CleanName(field) // clean up
		}

		fields[j] = field
	}
	return fields
}

// WithoutMeta returns the columns with metadata columns
func (cols Columns) WithoutMeta() (newCols Columns) {
	for _, column := range cols {
		if column.Metadata == nil {
			column.Metadata = map[string]string{}
		}

		if _, found := column.Metadata["sling_metadata"]; !found {
			// we should not find key `sling_metadata`
			newCols = append(newCols, column)
		}
	}
	return newCols
}

// Names return the column names
// args -> (lower bool, cleanUp bool)
func (cols Columns) Keys() []string {
	fields := make([]string, len(cols))
	for j, column := range cols {
		fields[j] = column.Key()
	}
	return fields
}

// Types return the column names/types
// args -> (lower bool, cleanUp bool)
func (cols Columns) Types(args ...bool) []string {
	lower := false
	cleanUp := false
	if len(args) > 1 {
		lower = args[0]
		cleanUp = args[1]
	} else if len(args) > 0 {
		lower = args[0]
	}
	fields := make([]string, len(cols))
	for j, column := range cols {
		field := column.Name

		if lower {
			field = strings.ToLower(column.Name)
		}
		if cleanUp {
			field = CleanName(field) // clean up
		}

		fields[j] = g.F("%s [%s]", field, column.Type)
		if column.DbType != "" {
			fields[j] = g.F("%s [%s | %s]", field, column.Type, column.DbType)
		}
	}
	return fields
}

func (cols Columns) MakeRec(row []any) map[string]any {
	m := g.M()
	// if len(row) > len(cols) {
	// 	g.Warn("MakeRec Column Length Mismatch: %d != %d", len(row), len(cols))
	// }

	for i, col := range cols {
		if i < len(row) {
			m[col.Name] = row[i]
		}
	}
	return m
}

type Shaper struct {
	Func       func([]any) []any
	SrcColumns Columns
	TgtColumns Columns
	ColMap     map[int]int
}

func (cols Columns) MakeShaper(tgtColumns Columns) (shaper *Shaper, err error) {
	srcColumns := cols

	if len(tgtColumns) < len(srcColumns) {
		err = g.Error("number of target columns is smaller than number of source columns")
		return
	}

	// determine diff, and match order of target columns
	tgtColNames := tgtColumns.Names(true)
	diffCols := len(tgtColumns) != len(srcColumns)
	colMap := map[int]int{}
	for s, col := range srcColumns {
		t := lo.IndexOf(tgtColNames, strings.ToLower(col.Name))
		if t == -1 {
			err = g.Error("column %s not found in target columns", col.Name)
			return
		}
		colMap[s] = t
		if s != t || !strings.EqualFold(tgtColumns[t].Name, srcColumns[s].Name) {
			diffCols = true
		}
	}

	if !diffCols {
		return nil, nil
	}

	// srcColNames := srcColumns.Names(true)
	mapRowCol := func(srcRow []any) []any {
		tgtRow := make([]any, len(tgtColumns))
		for len(srcRow) < len(tgtRow) {
			srcRow = append(srcRow, nil)
		}
		for s, t := range colMap {
			tgtRow[t] = srcRow[s]
		}

		return tgtRow
	}

	shaper = &Shaper{
		Func:       mapRowCol,
		SrcColumns: srcColumns,
		TgtColumns: tgtColumns,
		ColMap:     colMap,
	}

	return shaper, nil
}

// DbTypes return the column names/db types
// args -> (lower bool, cleanUp bool)
func (cols Columns) DbTypes(args ...bool) []string {
	lower := false
	cleanUp := false
	if len(args) > 1 {
		lower = args[0]
		cleanUp = args[1]
	} else if len(args) > 0 {
		lower = args[0]
	}
	fields := make([]string, len(cols))
	for j, column := range cols {
		field := column.Name

		if lower {
			field = strings.ToLower(column.Name)
		}
		if cleanUp {
			field = CleanName(field) // clean up
		}

		fields[j] = g.F("%s [%s]", field, column.DbType)
	}
	return fields
}

// Map return the map of columns
func (cols Columns) Map() map[string]*Column {
	colsMap := map[string]*Column{}
	for _, col := range cols {
		colsMap[col.Name] = &col
		keyLower := strings.ToLower(col.Name)
		if _, exists := colsMap[keyLower]; !exists {
			colsMap[keyLower] = &col // for lower case matching, don't overwrite
		}
	}
	return colsMap
}

// FieldMap return the fields map of indexes
// when `toLower` is true, field keys are lower cased
func (cols Columns) FieldMap(toLower bool) map[string]int {
	fieldColIDMap := map[string]int{}
	for i, col := range cols {
		if toLower {
			fieldColIDMap[strings.ToLower(col.Name)] = i
		} else {
			fieldColIDMap[col.Name] = i
		}
	}
	return fieldColIDMap
}

// Dataset return an empty inferred dataset
func (cols Columns) Dataset() Dataset {
	d := NewDataset(cols)
	d.Inferred = true
	return d
}

// Coerce casts columns into specified types
func (cols Columns) Coerce(castCols Columns, hasHeader bool, casing ColumnCasing, tgtType dbio.Type) (newCols Columns) {
	newCols = cols
	// apply casing first
	nameMap := map[string]string{}
	if !casing.IsEmpty() && tgtType != "" {
		g.Debug(`applying column casing (%s) for target type (%s)`, casing, tgtType)
		for i, col := range newCols {
			newName := casing.Apply(col.Name, tgtType)
			nameMap[strings.ToLower(newName)] = col.Name // map new name to old name
			newCols[i].Name = newName
			if col.Name != newName {
				g.Debug("   %s => %s", col.Name, newName)
			}
		}
	}

	// validate column name lengths, truncate if needed
	newCols = newCols.ValidateNames(tgtType)

	for i, col := range newCols {
		if strings.HasPrefix(col.Description, "Sling.Metadata.") {
			continue // do not modify metadata type
		}

		if !hasHeader && len(castCols) == len(newCols) {
			// assume same order since same number of columns and no header
			col = castCols[i]
			newCols[i].Name = col.Name
			newCols[i].Type = col.Type
			newCols[i].Stats.MaxLen = lo.Ternary(col.Stats.MaxLen > 0, col.Stats.MaxLen, newCols[i].Stats.MaxLen)
			newCols[i].DbPrecision = lo.Ternary(col.DbPrecision > 0, col.DbPrecision, newCols[i].DbPrecision)
			newCols[i].DbScale = lo.Ternary(col.DbScale > 0, col.DbScale, newCols[i].DbScale)
			newCols[i].Sourced = true
			// see note below: don't lock a sample-inferred precision for an
			// unsized decimal cast, or later rows can silently overflow.
			if newCols[i].Type.IsDecimal() && col.DbPrecision == 0 {
				newCols[i].DbPrecision = 0
				newCols[i].DbScale = 0
			}
			newCols[i].mergeDDLMetadata(col)
			if !newCols[i].Type.IsValid() {
				g.Warn("Provided unknown column type (%s) for column '%s'. Using string.", newCols[i].Type, newCols[i].Name)
				newCols[i].Type = StringType
			}
			continue
		}

		castCol := castCols.GetColumn(col.Name)
		if castCol == nil && !casing.IsEmpty() {
			// check old name
			if oldName, ok := nameMap[strings.ToLower(col.Name)]; ok {
				castCol = castCols.GetColumn(oldName)
			}
		}

		if castCol != nil {
			col = *castCol
			newCols[i].mergeDDLMetadata(col)
			if col.Type.IsValid() {
				g.Debug("casting column '%s' as '%s'", col.Name, col.Type)
				newCols[i].Type = col.Type
				newCols[i].Stats.MaxLen = lo.Ternary(col.Stats.MaxLen > 0, col.Stats.MaxLen, newCols[i].Stats.MaxLen)
				newCols[i].DbPrecision = lo.Ternary(col.DbPrecision > 0, col.DbPrecision, newCols[i].DbPrecision)
				newCols[i].DbScale = lo.Ternary(col.DbScale > 0, col.DbScale, newCols[i].DbScale)
				newCols[i].Sourced = true

				// If the user pinned a decimal's *type* without a precision (e.g.
				// `code: decimal`), do not lock the sample-inferred precision/scale as
				// if it were user-specified — a limited sample can under-size it and
				// silently overflow later rows (e.g. StarRocks decimal(3,1) saturating
				// values >= 100). Clear the inferred precision so downstream inference
				// applies safe minimums, while keeping Sourced=true so the decimal type
				// itself still survives streaming (see discussion #763).
				if newCols[i].Type.IsDecimal() && col.DbPrecision == 0 {
					newCols[i].DbPrecision = 0
					newCols[i].DbScale = 0
				}
			} else {
				g.Warn("Provided unknown column type (%s) for column '%s'. Using string.", col.Type, col.Name)
				newCols[i].Type = StringType
			}
		}

		if len(castCols) == 1 && castCols[0].Name == "*" {
			col = castCols[0]
			if col.Type.IsValid() {
				g.Debug("casting column '%s' as '%s'", newCols[i].Name, col.Type)
				newCols[i].Type = col.Type
				newCols[i].Sourced = true
			} else {
				g.Warn("Provided unknown column type (%s) for column '%s'. Using string.", col.Type, newCols[i].Name)
				newCols[i].Type = StringType
			}

		}
	}
	return newCols
}

// GetColumn returns the matched Col
func (cols Columns) GetColumn(name string) *Column {
	colsOrigMap := map[string]*Column{}
	colsMap := map[string]*Column{}
	for _, col := range cols {
		colsOrigMap[col.Name] = &col // for any mixed-cased columns
		colsMap[strings.ToLower(col.Name)] = &col
	}

	// look for column in original casing first
	if col, ok := colsOrigMap[name]; ok && col != nil {
		return col
	}

	return colsMap[strings.ToLower(name)]
}

func (cols Columns) Merge(newCols Columns, overwrite bool) (col2 Columns, added schemaChg, changed []schemaChg) {
	added = schemaChg{Added: true}

	existingIndexMap := cols.FieldMap(true)
	for _, newCol := range newCols {
		key := strings.ToLower(newCol.Name)
		if i, ok := existingIndexMap[key]; ok {
			col := cols[i]
			if overwrite {
				newCol.Position = i + 1
				cols[i] = newCol
			} else if col.Type != newCol.Type && newCol.Stats.TotalCnt > newCol.Stats.NullCnt {
				doChange := true
				switch {
				case col.Type.IsString() && newCol.Stats.TotalCnt > newCol.Stats.NullCnt:
					// leave as is
					doChange = false
				case col.Type == JsonType && g.In(newCol.Type, StringType, TextType):
				case col.Type != DecimalType && newCol.Type == DecimalType:
				case !g.In(col.Type, DecimalType, FloatType) && g.In(newCol.Type, DecimalType, FloatType):
				case !col.Type.IsNumber() && newCol.Type.IsInteger():
				case !col.Type.IsBool() && newCol.Type.IsBool():
				case !col.Type.IsDate() && newCol.Type.IsDate():
				case !col.Type.IsDatetime() && newCol.Type.IsDatetime():
				default:
					doChange = false
				}

				if doChange {
					// g.Debug("Columns.Add Type mismatch for %s => %s != %s", newCol.Name, cols[i].Type, newCol.Type)
					change := schemaChg{Added: false, ChangedIndex: i, ChangedType: newCol.Type}
					changed = append(changed, change)
				}
			}
		} else {
			newCol.Position = len(cols) + 1
			cols = append(cols, newCol)
			added.AddedCols = append(added.AddedCols, newCol)
		}
	}

	return cols, added, changed
}

// IsSimilarTo returns true if has same number of columns
// and contains the same columns, but may be in different order
func (cols Columns) IsSimilarTo(otherCols Columns) bool {
	if len(cols) != len(otherCols) {
		return false
	}

	otherColsMap := cols.FieldMap(true)
	for _, col := range cols {
		colName := strings.ToLower(col.Name)
		if _, found := otherColsMap[colName]; !found {
			return false
		}
	}
	return true
}

func (cols Columns) IsDifferent(newCols Columns) bool {
	if len(cols) != len(newCols) {
		return true
	}
	for i := range newCols {
		if newCols[i].Type != cols[i].Type {
			return true
		} else if !strings.EqualFold(newCols[i].Name, cols[i].Name) {
			return true
		}
	}
	return false
}

func CleanName(name string) (newName string) {
	newName = strings.TrimSpace(name)
	newName = replacePattern.ReplaceAllString(newName, "_") // clean up
	if regexFirstDigit.MatchString(newName) {
		newName = "_" + newName
	}
	return
}

// CompareColumns compared two columns to see if there are similar
func CompareColumns(columns1 Columns, columns2 Columns) (reshape bool, err error) {
	// if len(columns1) != len(columns2) {
	// 	g.Debug("%#v != %#v", columns1.Names(), columns2.Names())
	// 	return reshape, g.Error("columns mismatch: %d fields != %d fields", len(columns1), len(columns2))
	// }

	eG := g.ErrorGroup{}

	// all columns2 need to exist in columns1
	cols1Map := columns1.FieldMap(true)
	for _, c2 := range columns2 {
		if i, found := cols1Map[strings.ToLower(c2.Name)]; found {
			c1 := columns1[i]

			if c1.Name != c2.Name {
				if found {
					// sometimes the orders of columns is different
					// (especially, multiple json files), shape ds to match columns1
					reshape = true
				} else {
					eG.Add(g.Error("column name mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type))
				}
			} else if c1.Type != c2.Type {
				// too unpredictable to mark as error? sometimes one column
				// has not enough data to represent true type. Warn instead
				// eG.Add(g.Error("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type))

				switch {
				case g.In(c1.Type, TextType, StringType) && g.In(c2.Type, TextType, StringType):
				default:
					g.Warn("type mismatch: %s (%s) != %s (%s)", c1.Name, c1.Type, c2.Name, c2.Type)
				}
			}
		} else {
			eG.Add(g.Error("column not found: %s (%s)", c2.Name, c2.Type))
		}
	}

	return reshape, eG.Err()
}

// InferFromStats using the stats to infer data types
func InferFromStats(columns []Column, safe bool, noDebug bool) []Column {
	for j, col := range columns {
		colStats := col.Stats

		if colStats.TotalCnt == 0 || colStats.NullCnt == colStats.TotalCnt || col.Sourced || col.Type.IsBinary() {
			// do nothing, keep existing type if defined.
			// Binary columns are preserved even when their values arrive as
			// []uint8 (which the stats tally as strings) so large binary
			// payloads keep the binary native type instead of degrading to
			// an unsized varchar that truncates at the string limit.
		} else if colStats.StringCnt > 0 && colStats.BoolCnt == 0 && colStats.IntCnt == 0 && colStats.DecCnt == 0 && colStats.DateCnt == 0 && colStats.DateTimeCnt == 0 && colStats.DateTimeZCnt == 0 && colStats.JsonCnt == 0 {
			// Only string values and no other types detected
			col.Sourced = true // do not allow type change

			if colStats.MaxLen > 255 {
				col.Type = TextType
			} else {
				col.Type = StringType
			}
			if safe {
				col.Type = TextType // max out
			}
			col.goType = reflect.TypeOf("string")

			colStats.Min = 0
			if colStats.NullCnt == colStats.TotalCnt {
				colStats.MinLen = 0
			}
		} else if colStats.JsonCnt > 0 && colStats.JsonCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = JsonType
			col.goType = reflect.TypeOf("json")
		} else if colStats.BoolCnt > 0 && colStats.BoolCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = BoolType
			col.goType = reflect.TypeOf(true)
			colStats.Min = 0
		} else if colStats.IntCnt > 0 && colStats.IntCnt+colStats.NullCnt == colStats.TotalCnt && col.Type != DecimalType {
			// Check if the values are too large for a regular int
			if colStats.Min < -2147483648 || colStats.Max > 2147483647 {
				col.Type = BigIntType
			} else {
				col.Type = IntegerType
			}
			col.goType = reflect.TypeOf(int64(0))

			if safe {
				// cast as bigint for safety
				col.Type = BigIntType
			}
		} else if (colStats.DecCnt > 0 && colStats.DecCnt+colStats.IntCnt+colStats.NullCnt == colStats.TotalCnt) || col.Type == DecimalType { // keep as decimal if already set from mapping
			col.Type = DecimalType
			col.goType = reflect.TypeOf(float64(0.0))
			col.DbPrecision = lo.Ternary(col.DbPrecision == 0, colStats.MaxLen, col.DbPrecision)
			col.DbScale = lo.Ternary(col.DbScale == 0, colStats.MaxDecLen, col.DbScale)
		} else if colStats.DateCnt > 0 && colStats.DateCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = DateType
			col.goType = reflect.TypeOf(time.Now())
			colStats.Min = 0
		} else if colStats.DateTimeCnt+colStats.DateTimeZCnt > 0 && colStats.DateTimeCnt+colStats.DateTimeZCnt+colStats.DateCnt+colStats.NullCnt == colStats.TotalCnt {
			if colStats.DateTimeZCnt > 0 {
				col.Type = TimestampzType
			} else {
				col.Type = DatetimeType
			}
			col.goType = reflect.TypeOf(time.Now())
			colStats.Min = 0
		} else if colStats.GeometryCnt > 0 && colStats.GeometryCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = GeometryType
			col.goType = reflect.TypeOf("geometry")
		} else {
			// Mixed types or unrecognized - default to string/text
			if colStats.MaxLen >= 4000 {
				col.Type = TextType
			} else {
				col.Type = StringType
			}
			col.goType = reflect.TypeOf("string")
		}
		if !noDebug {
			g.Trace("%s - %s %s", col.Name, col.Type, g.Marshal(colStats))
		}

		col.Stats = colStats
		columns[j] = col
	}
	return columns
}

type Record struct {
	Columns *Columns
	Values  []any
}

// MakeRowsChan returns a buffered channel with default size
func MakeRowsChan() chan []any {
	return make(chan []any)
}

const regexExtractPrecisionScale = `[a-zA-Z]+ *\( *(\d+) *(, *\d+)* *\)`

func (col *Column) SetConstraint() {
	parts := strings.Split(string(col.Type), "|")
	if len(parts) != 2 {
		return
	}

	// fix type value
	col.Type = ColumnType(strings.TrimSpace(parts[0]))

	cc := &ColumnConstraint{
		Expression: strings.TrimSpace(parts[1]),
	}
	cc.parse()
	if cc.EvalFunc != nil {
		col.Constraint = cc
	}
}

// ValidateNames truncates the column name it exceed the max column length
func (cols Columns) ValidateNames(tgtType dbio.Type) (newCols Columns) {
	newCols = cols
	if string(tgtType) == "" {
		return
	}

	maxLength := cast.ToInt(tgtType.GetTemplateValue("variable.max_column_length"))
	if maxLength == 0 {
		return
	}

	nameMap := newCols.FieldMap(true)
	truncations := []string{}

	for i, col := range newCols {
		if len(col.Name) > maxLength {
			newName := col.Name[:maxLength]
			// look for existing name to not have duplicate name
			// shorten again and append number, need to recheck if new name again doesn't exist
			suffix := 1
			baseNewName := newName
			for {
				if _, ok := nameMap[strings.ToLower(newName)]; ok {
					// Name collision, adjust the name
					// Need to ensure the base name plus suffix stays within maxLength
					suffixStr := fmt.Sprintf("_%d", suffix)
					if len(baseNewName)+len(suffixStr) > maxLength {
						baseNewName = baseNewName[:maxLength-len(suffixStr)]
					}
					newName = baseNewName + suffixStr
					suffix++
				} else {
					break
				}
			}

			// Update the name map with the new name
			delete(nameMap, strings.ToLower(col.Name))
			nameMap[strings.ToLower(newName)] = i

			// Update the column name
			truncations = append(truncations, g.F("%s => %s", newCols[i].Name, newName))
			newCols[i].Name = newName
		}
	}

	// log
	if len(truncations) > 0 {
		g.Debug(`truncated column names (exceeds max length of %d for "%s")`, maxLength, tgtType)
		for _, truncation := range truncations {
			g.Debug("   %s", truncation)
		}
	}

	return
}

// SetLengthPrecisionScale parse length, precision, scale
func (col *Column) SetLengthPrecisionScale() {
	colType := strings.TrimSpace(string(col.Type))
	if !strings.HasSuffix(colType, ")") {
		return
	}

	// fix type value
	parts := strings.Split(colType, "(")
	col.Type = ColumnType(strings.TrimSpace(parts[0]))

	matches := g.Matches(colType, regexExtractPrecisionScale)
	if len(matches) == 1 {
		vals := matches[0].Group

		if len(vals) > 0 {
			vals[0] = strings.TrimSpace(vals[0])
			// grab length or precision
			if col.Type.IsString() {
				col.Stats.MaxLen = cast.ToInt(vals[0])
				col.DbPrecision = cast.ToInt(vals[0])
			} else if col.IsNumber() || col.IsDatetime() {
				col.DbPrecision = cast.ToInt(vals[0])
			}
		}

		if len(vals) > 1 {
			vals[1] = strings.TrimSpace(strings.ReplaceAll(vals[1], ",", ""))
			// grab scale
			if col.Type.IsNumber() {
				col.DbScale = cast.ToInt(vals[1])
			}
		}

		if col.DbPrecision > 0 || col.Stats.MaxLen > 0 {
			col.Sourced = true
		}
	}
}

// EvaluateConstraint evaluates a value against the constraint function
func (col *Column) EvaluateConstraint(value any, sp *StreamProcessor) (err error) {
	if c := col.Constraint; c.EvalFunc != nil && !c.EvalFunc(value) {
		c.FailCnt++
		errMsg := g.F("constraint failure for column '%s', at row number %d, for value: %s", col.Name, sp.N, cast.ToString(value))

		// Only store first 20 error messages to avoid memory issues
		// but always return error regardless of count
		if c.FailCnt <= 20 {
			g.Warn(errMsg)
			c.Errors = append(c.Errors, errMsg)
		} else if c.FailCnt == 21 {
			// Log once when threshold is exceeded
			g.Warn(g.F("constraint failure for column '%s' (logging limit reached, %d total failures so far)", col.Name, c.FailCnt))
		}

		return g.Error(errMsg)
	}
	return
}

// mergeDDLMetadata copies the DDL-relevant metadata (nullable, primary_key,
// unique, index, description, ddl_explicit) and Description from src onto col.
// Used when coercing the configured columns: declarations onto the streamed
// columns so the modifier DSL survives to DDL generation. Only keys present in
// src are copied (source-derived metadata is otherwise preserved).
func (col *Column) mergeDDLMetadata(src Column) {
	if src.Metadata != nil {
		for _, key := range []ColMetaKey{
			ColMetaNullable, ColMetaIsPrimaryKey, ColMetaUnique,
			ColMetaIndex, ColMetaDescription, ColMetaDDLExplicit,
		} {
			if v, ok := src.Metadata[key.String()]; ok {
				col.SetMetadata(key.String(), v)
			}
		}
	}
	if src.Description != "" {
		col.Description = src.Description
	}
}

func (col *Column) SetMetadata(key string, value string) {
	if col.Metadata == nil {
		col.Metadata = map[string]string{}
	}
	col.Metadata[key] = value
}

func (col *Column) IsKeyType(keyType KeyType) bool {
	if col.Metadata == nil {
		return false
	}
	return cast.ToBool(col.Metadata[keyType.MetadataKey()])
}

// IsAutoIncrement returns true if the column is auto-increment/identity
func (col *Column) IsAutoIncrement() bool {
	if col.Metadata == nil {
		return false
	}
	return col.Metadata[ColMetaAutoIncrement.String()] == "true"
}

// GetDefaultValue returns the default value metadata
func (col *Column) GetDefaultValue() string {
	if col.Metadata == nil {
		return ""
	}
	return col.Metadata[ColMetaDefaultValue.String()]
}

// IsNullable returns true if the column allows NULL values
// Defaults to true if not explicitly set
func (col *Column) IsNullable() bool {
	if col.Metadata == nil {
		return true
	}
	val := col.Metadata[ColMetaNullable.String()]
	return val == "" || val == "true"
}

// IsPrimaryKey returns true if the column is part of the primary key
func (col *Column) IsPrimaryKey() bool {
	if col.Metadata == nil {
		return false
	}
	return col.Metadata[ColMetaIsPrimaryKey.String()] == "true"
}

// HasUniqueConstraint returns true if the column has a UNIQUE constraint declared
// via the columns: modifier DSL.
func (col *Column) HasUniqueConstraint() bool {
	if col.Metadata == nil {
		return false
	}
	return col.Metadata[ColMetaUnique.String()] == "true"
}

// IsDDLExplicit returns true if DDL constraints were declared explicitly via the
// columns: modifier DSL (so they apply at CREATE time without schema-migration).
func (col *Column) IsDDLExplicit() bool {
	if col.Metadata == nil {
		return false
	}
	return col.Metadata[ColMetaDDLExplicit.String()] == "true"
}

// GetIndexDefs returns the inline index definitions declared on this column
// via the columns: modifier DSL (index/index(...)/unique_index(...)).
func (col *Column) GetIndexDefs() (defs []IndexDef) {
	if col.Metadata == nil {
		return nil
	}
	val := col.Metadata[ColMetaIndex.String()]
	if val == "" {
		return nil
	}
	if err := g.Unmarshal(val, &defs); err != nil {
		g.Warn("could not parse index metadata for column %s: %s", col.Name, err.Error())
		return nil
	}
	return defs
}

// GetForeignKey returns the foreign key info if present
func (col *Column) GetForeignKey() (*ForeignKeyInfo, error) {
	if col.Metadata == nil {
		return nil, nil
	}
	fkJSON := col.Metadata[ColMetaForeignKey.String()]
	if fkJSON == "" {
		return nil, nil
	}
	var fk ForeignKeyInfo
	err := g.Unmarshal(fkJSON, &fk)
	return &fk, err
}

func (col *Column) Key() string {
	parts := []string{}
	if col.Database != "" {
		parts = append(parts, col.Database)
	}
	if col.Schema != "" {
		parts = append(parts, col.Schema)
	}
	if col.Table != "" {
		parts = append(parts, col.Table)
	}
	if col.Name != "" {
		parts = append(parts, col.Name)
	}
	return strings.ToLower(strings.Join(parts, "."))
}

func (col *Column) GoType() reflect.Type {
	if col.goType != nil {
		return col.goType
	}

	switch {
	case col.IsBool():
		return reflect.TypeOf(true)
	case col.IsInteger():
		return reflect.TypeOf(int64(0))
	case col.IsDatetime() || col.IsDate():
		return reflect.TypeOf(time.Now())
	case col.IsDecimal():
		return reflect.TypeOf(float64(6.6))
	case col.IsFloat():
		return reflect.TypeOf(float64(6.6))
	}

	return reflect.TypeOf("string")
}

func (col *Column) IsUnique() bool {
	if col.Stats.TotalCnt <= 0 {
		return false
	}
	return col.Stats.TotalCnt == col.Stats.UniqCnt
}

func (col *Column) HasNulls() bool {
	return col.Stats.TotalCnt > 0 && col.Stats.TotalCnt == col.Stats.NullCnt
}

// HasNullsPlus1 denotes when a column is all nulls plus 1 non-null
func (col *Column) HasNullsPlus1() bool {
	return col.Stats.TotalCnt > 0 && col.Stats.TotalCnt == col.Stats.NullCnt+1
}

// IsBinary returns whether the column is a binary
func (col *Column) IsBinary() bool {
	return col.Type.IsBinary()
}

// IsString returns whether the column is a string
func (col *Column) IsString() bool {
	return col.Type.IsString()
}

// IsInteger returns whether the column is an integer
func (col *Column) IsInteger() bool {
	return col.Type.IsInteger()
}

// IsFloat returns whether the column is a float
func (col *Column) IsFloat() bool {
	return col.Type.IsFloat()
}

// IsDecimal returns whether the column is a decimal
func (col *Column) IsDecimal() bool {
	return col.Type.IsDecimal()
}

// IsNumber returns whether the column is a decimal or an integer
func (col *Column) IsNumber() bool {
	return col.Type.IsNumber()
}

// IsBool returns whether the column is a boolean
func (col *Column) IsBool() bool {
	return col.Type.IsBool()
}

// IsDate returns whether the column is a datet object
func (col *Column) IsDate() bool {
	return col.Type.IsDate()
}

// IsDatetime returns whether the column is a datetime object
func (col *Column) IsDatetime() bool {
	return col.Type.IsDatetime()
}

// IsBinary returns whether the column is a binary
func (ct ColumnType) IsBinary() bool {
	switch ct {
	case BinaryType:
		return true
	}
	return false
}

// IsString returns whether the column is a string
func (ct ColumnType) IsString() bool {
	switch ct {
	case StringType, TextType, JsonType, BinaryType, UUIDType, GeometryType:
		return true
	}
	return false
}

// IsJSON returns whether the column is a json
func (ct ColumnType) IsJSON() bool {
	switch ct {
	case JsonType:
		return true
	}
	return false
}

// IsInteger returns whether the column is an integer
func (ct ColumnType) IsInteger() bool {
	switch ct {
	case IntegerType, BigIntType, SmallIntType:
		return true
	}
	return false
}

// IsFloat returns whether the column is a float
func (ct ColumnType) IsFloat() bool {
	return ct == FloatType
}

// IsDecimal returns whether the column is a decimal
func (ct ColumnType) IsDecimal() bool {
	return ct == DecimalType
}

// IsNumber returns whether the column is a decimal or an integer
func (ct ColumnType) IsNumber() bool {
	return ct.IsInteger() || ct.IsDecimal() || ct.IsFloat()
}

// IsBool returns whether the column is a boolean
func (ct ColumnType) IsBool() bool {
	return ct == BoolType
}

// IsDatetime returns whether the column is a datetime object
func (ct ColumnType) IsDate() bool {
	switch ct {
	case DateType:
		return true
	}
	return false
}

// IsDatetime returns whether the column is a datetime object
func (ct ColumnType) IsDatetime() bool {
	switch ct {
	case DatetimeType, TimestampType, TimestampzType:
		return true
	}
	return false
}

// IsTime returns whether the column is a time object
func (ct ColumnType) IsTime() bool {
	switch ct {
	case TimeType, TimezType:
		return true
	}
	return false
}

// IsGeometry returns whether the column is a geometry object
func (ct ColumnType) IsGeometry() bool {
	switch ct {
	case GeometryType:
		return true
	}
	return false
}

// IsValid returns whether the column has a valid type
func (ct ColumnType) IsValid() bool {
	return ct.IsBinary() || ct.IsString() || ct.IsJSON() || ct.IsNumber() || ct.IsBool() || ct.IsDate() || ct.IsDatetime() || ct.IsTime() || ct.IsGeometry()
}

func isDate(t *time.Time) bool {
	return t != nil && t.Unix()%(24*60*60) == 0
	// return t.Format("15:04:05.000") == "00:00:00.000" // much slower
}

func isUTC(t *time.Time) bool {
	return t != nil && t.Location().String() == "UTC"
}

// parse parses the constraint expression and sets the function
func (cc *ColumnConstraint) parse() {
	var err error
	cc.EvalFunc, err = parseConstraintExpression(cc.Expression)
	if err != nil {
		g.Warn(err.Error())
		return
	}
}

// GetNativeType returns the native column type from generic
func (col *Column) GetNativeType(t dbio.Type, ct ColumnTyping) (nativeType string, err error) {
	template, _ := t.Template()

remap:
	nativeType, ok := template.GeneralTypeMap[string(col.Type)]
	if !ok {
		err = g.Error(
			"No native type mapping defined for col '%s', with type '%s' ('%s') for '%s'",
			col.Name,
			col.Type,
			col.DbType,
			t,
		)

		g.Warn(err.Error() + ". Using 'string'")
		err = nil
		nativeType = template.GeneralTypeMap["string"]
	}

	// when column_typing string max_length is set and column is text,
	// remap to string so the varchar() precision path is used
	if ct.String != nil && ct.String.MaxLength > 0 && col.Type == TextType {
		if stringType, ok := template.GeneralTypeMap["string"]; ok && strings.HasSuffix(stringType, "()") {
			nativeType = stringType
			col.Type = StringType
		}
	}

	// Add precision as needed
	if strings.HasSuffix(nativeType, "()") {
		maxStringLength := cast.ToInt(template.Value("variable.max_string_length"))
		maxStringType := template.Value("variable.max_string_type")

		length := col.Stats.MaxLen
		// IsString() also reports true for binary; route binary columns to
		// the dedicated binary branch below so they get an explicit
		// binary(<size>) instead of falling through to varchar.
		if col.IsString() && !col.IsBinary() {
			isSourced := col.Sourced && col.DbPrecision > 0
			if isSourced {
				// string length was manually provided
				length = col.DbPrecision

				// If the source column was a decimal, we need to account for the decimal point
				// and potential negative sign when converting to string
				if t := strings.ToLower(col.DbType); strings.Contains(t, "decimal") || strings.Contains(t, "float") {
					// Add 1 for decimal point if scale > 0
					if col.DbScale > 0 {
						length = length + col.DbScale + 1
					} else {
						length = length + env.DdlMinDecScale + 1
					}
					// Add 1 for potential negative sign
					length = length + 1
				} else if col.DbScale > 0 {
					length = length + col.DbScale + 1 + 1
				}

				if ct.String != nil {
					newLength := ct.String.Apply(length, maxStringLength)
					if newLength != length {
						if ct.String.Note != "" {
							g.Debug(`  applied string length mapping for column "%s" (%d => %d) [%s]`, col.Name, length, newLength, ct.String.Note)
						} else {
							g.Debug(`  applied string length mapping for column "%s" (%d => %d)`, col.Name, length, newLength)
						}
					}
					length = newLength
				}
			} else if ct.String != nil {
				// apply column_typing even for non-sourced columns (e.g. CSV/file sources)
				if length <= 0 {
					length = col.Stats.MaxLen * 2
					if length < 255 {
						length = 255
					}
				}
				newLength := ct.String.Apply(length, maxStringLength)
				if newLength != length {
					g.Debug(`  applied string length mapping for column "%s" (%d => %d)`, col.Name, length, newLength)
				}
				length = newLength
			} else if length <= 0 {
				length = col.Stats.MaxLen * 2
				if length < 255 {
					length = 255
				}
			}

			if !isSourced && ct.String == nil && maxStringType != "" {
				nativeType = maxStringType // use specified default
			} else if length >= maxStringLength {
				// let's make text since high
				nativeType = template.GeneralTypeMap["text"]
			} else {
				nativeType = strings.ReplaceAll(
					nativeType,
					"()",
					fmt.Sprintf("(%d)", length),
				)
			}
		} else if col.IsBinary() {
			// Binary columns: fill the () with an explicit byte length so the
			// target doesn't fall back to a small account-default size (which
			// truncates large LOBs, e.g. Oracle BLOB/LONG RAW -> Snowflake).
			maxBinaryLength := cast.ToInt(template.Value("variable.max_binary_length"))
			maxBinaryType := template.Value("variable.max_binary_type")

			length := col.Stats.MaxLen
			if col.Sourced && col.DbPrecision > 0 {
				length = col.DbPrecision
			}

			overMax := maxBinaryLength > 0 && length > maxBinaryLength
			if (length <= 0 || overMax) && maxBinaryType != "" {
				// unknown or over the sized limit -> use the unbounded type
				// (e.g. varbinary(max), longblob)
				nativeType = maxBinaryType
			} else if length <= 0 || overMax {
				if maxBinaryLength > 0 {
					length = maxBinaryLength
				}
				if length > 0 {
					nativeType = strings.ReplaceAll(nativeType, "()", fmt.Sprintf("(%d)", length))
				} else {
					nativeType = strings.ReplaceAll(nativeType, "()", "")
				}
			} else {
				nativeType = strings.ReplaceAll(nativeType, "()", fmt.Sprintf("(%d)", length))
			}
		} else if col.IsInteger() {
			if !col.Sourced && length < env.DdlDefDecLength {
				length = env.DdlDefDecLength
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		} else if col.Type.IsTime() {
			// For time types, use a default varchar length of 100 to accommodate various time formats
			// (e.g., "HH:MM:SS.ffffff" from SQL Server)
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				"(100)",
			)
		}
	} else if strings.Contains(nativeType, "(,)") || g.In(nativeType, "numeric") {

		precision := col.DbPrecision
		scale := col.DbScale

		if col.IsDecimal() {
			if ct.Decimal == nil {
				ct.Decimal = &DecimalColumnTyping{}
			}
			precision, scale = ct.Decimal.Apply(*col)

			switch ct.Decimal.CastAs {
			case "float":
				col.Type = FloatType
				goto remap
			case "string":
				col.Type = StringType
				goto remap
			}
		}

		if strings.Contains(nativeType, "(,)") {
			nativeType = strings.ReplaceAll(
				nativeType,
				"(,)",
				fmt.Sprintf("(%d,%d)", precision, scale),
			)
		} else if t == dbio.TypeDbBigQuery && strings.EqualFold(nativeType, "numeric") &&
			// BigQuery: use BIGNUMERIC if scale > 9 or precision > 38
			(scale > 9 || precision > 38) {
			nativeType = "bignumeric"
		}

	} else if col.Type.IsJSON() {
		if ct.JSON != nil {
			origType := col.Type
			ct.JSON.Apply(col)
			if col.Type != origType {
				goto remap
			}
		}
	} else if col.IsBool() {
		if ct.Boolean != nil {
			origType := col.Type
			ct.Boolean.Apply(col)
			if col.Type != origType {
				goto remap
			}
		}
	}

	return
}

func NativeTypeToGeneral(name, dbType string, connType dbio.Type) (colType ColumnType) {
	dbType = strings.ToLower(dbType)

	switch connType {
	case dbio.TypeDbClickhouse:
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = strings.ReplaceAll(dbType, "nullable(", "")
			dbType = strings.TrimSuffix(dbType, ")")
		}
	case dbio.TypeDbProton:
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = strings.ReplaceAll(dbType, "nullable(", "")
			dbType = strings.TrimSuffix(dbType, ")")
		}
	case dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck:
		if strings.HasSuffix(dbType, "[]") {
			dbType = "list"
		}
	case dbio.TypeDbOracle:
		// Oracle NUMBER(p,s) with scale=0 should map to integer types
		if strings.HasPrefix(dbType, "number") && strings.Contains(dbType, "(") {
			// Extract precision and scale from NUMBER(p,s) or NUMBER(p)
			re := regexp.MustCompile(`number\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)`)
			matches := re.FindStringSubmatch(dbType)
			if len(matches) >= 2 {
				precision := cast.ToInt(matches[1])
				scale := 0
				if len(matches) >= 3 && matches[2] != "" {
					scale = cast.ToInt(matches[2])
				}
				// If scale is 0, use integer types based on precision
				if scale == 0 {
					if precision <= 5 {
						return SmallIntType
					} else if precision <= 10 {
						return IntegerType
					} else if precision <= 19 {
						return BigIntType
					}
					// For precision > 19 with scale=0, fall through to decimal
				}
			}
		}
	}

	dbType = strings.ToLower(dbType)

	template, _ := connType.Template()

	// Try matching with parenthesized type first (e.g. "tinyint(1)")
	// before stripping parentheses. This allows MySQL/MariaDB/StarRocks
	// to distinguish tinyint(1) (boolean) from tinyint (small integer).
	dbTypeFull := strings.Split(dbType, "<")[0]
	dbTypeFull = strings.TrimRight(dbTypeFull, " ")
	if matchedType, ok := template.NativeTypeMap[dbTypeFull]; ok {
		return ColumnType(matchedType)
	}

	dbType = strings.Split(dbType, "(")[0]
	dbType = strings.Split(dbType, "<")[0]

	if matchedType, ok := template.NativeTypeMap[dbType]; ok {
		colType = ColumnType(matchedType)
	} else {
		if dbType != "" {
			g.Debug("using text since type '%s' not mapped for col '%s'", dbType, name)
		}
		colType = TextType // default as text
	}
	return
}

// FormatValue format as sql expression (adds quotes)
func FormatValue(val any, columnType ColumnType, connType dbio.Type) (newVal string) {
	template, _ := connType.Template()

	if val == nil || val == "" {
		return ""
	} else if columnType.IsDate() {
		newVal = g.R(
			template.Variable["date_layout_str"],
			"value", cast.ToTime(val).Format(template.Variable["date_layout"]),
		)
	} else if columnType.IsDatetime() {
		// set timestampz_layout_str and timestampz_layout if missing
		if _, ok := template.Variable["timestampz_layout_str"]; !ok {
			template.Variable["timestampz_layout_str"] = template.Variable["timestamp_layout_str"]
		}
		if _, ok := template.Variable["timestampz_layout"]; !ok {
			template.Variable["timestampz_layout"] = template.Variable["timestamp_layout"]
		}

		if columnType == TimestampzType {
			newVal = g.R(
				template.Variable["timestampz_layout_str"],
				"value", cast.ToTime(val).Format(template.Variable["timestampz_layout"]),
			)
		} else {
			newVal = g.R(
				template.Variable["timestamp_layout_str"],
				"value", cast.ToTime(val).Format(template.Variable["timestamp_layout"]),
			)
		}
	} else if columnType.IsNumber() {
		newVal = cast.ToString(val)
	} else {
		newVal = strings.ReplaceAll(cast.ToString(val), `'`, `''`)
		newVal = `'` + newVal + `'`
	}
	return
}

// ColumnCasing is the casing method to use
type ColumnCasing string

const (
	// see https://github.com/slingdata-io/sling-cli/issues/538
	NormalizeColumnCasing ColumnCasing = "normalize" // normalize to target, leaves mixed cases columns as it
	SourceColumnCasing    ColumnCasing = "source"    // keeps source column name casing. The default.
	TargetColumnCasing    ColumnCasing = "target"    // converts casing according to target database. Lower-case for files.
	SnakeColumnCasing     ColumnCasing = "snake"     // converts snake casing according to target database. Lower-case for files.
	UpperColumnCasing     ColumnCasing = "upper"     // make it upper case
	LowerColumnCasing     ColumnCasing = "lower"     // make it lower case
	CamelColumnCasing     ColumnCasing = "camel"     // converts to camelCase
)

// Equals evaluates equality for column casing (pointer safe)
func (cc *ColumnCasing) Equals(val ColumnCasing) bool {
	if cc.IsEmpty() {
		return false
	}
	return *cc == val
}

// IsEmpty return true if nil or blank
func (cc *ColumnCasing) IsEmpty() bool {
	if cc == nil || string(*cc) == "" {
		return true
	}
	return false
}

// IsEmpty return true if nil or blank
func (cc *ColumnCasing) ApplyColumns(cols Columns, tgtType dbio.Type) (newCols Columns) {
	newCols = cols

	// apply casing first
	nameMap := map[string]string{}
	if !cc.IsEmpty() && tgtType != "" {
		g.Debug(`applying column casing (%s) for target type (%s)`, *cc, tgtType)
		for i, col := range newCols {
			newName := cc.Apply(col.Name, tgtType)
			nameMap[strings.ToLower(newName)] = col.Name // map new name to old name
			newCols[i].Name = newName
			if col.Name != newName {
				g.Debug("   %s => %s", col.Name, newName)
			}
		}
	}

	return
}

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// Apply applies column casing to provided name.
// If cc is nil or SourceColumnCasing, it returns the original value
func (cc *ColumnCasing) Apply(name string, tgtConnType dbio.Type) string {
	if cc.IsEmpty() || cc.Equals(SourceColumnCasing) {
		return name
	} else if cc.Equals(NormalizeColumnCasing) {
		// use legacy behavior, so that we don't need to qualify column name where querying
		if !dbio.HasVariedCase(name) && !dbio.HasStrangeChar(name) {
			if tgtConnType.DBNameUpperCase() {
				name = strings.ToUpper(name)
			} else {
				name = strings.ToLower(name)
			}
		}
		return name
	}

	// convert to snake case
	if cc.Equals(SnakeColumnCasing) {
		name = matchAllCap.ReplaceAllString(name, "${1}_${2}")
	}

	// clean up other weird chars
	name = CleanName(name)

	switch {
	case cc.Equals(UpperColumnCasing):
		return strings.ToUpper(name)
	case cc.Equals(LowerColumnCasing):
		return strings.ToLower(name)
	case cc.Equals(CamelColumnCasing):
		return toCamelCase(name)
	case cc.Equals(TargetColumnCasing), cc.Equals(SnakeColumnCasing):
		// lower case for target file system
		if tgtConnType.DBNameUpperCase() {
			return strings.ToUpper(name)
		}
		return strings.ToLower(name)
	}

	return name
}

// toCamelCase converts a string to camelCase
// Examples: "first_name" -> "firstName", "FirstName" -> "firstName", "FIRST_NAME" -> "firstName"
func toCamelCase(name string) string {
	if name == "" {
		return name
	}

	// First normalize to snake_case to handle various input formats
	// Handle camelCase/PascalCase transitions
	name = matchAllCap.ReplaceAllString(name, "${1}_${2}")
	name = strings.ToLower(name)

	// Split by underscores and other separators
	parts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})

	if len(parts) == 0 {
		return name
	}

	// First part stays lowercase, subsequent parts get capitalized
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			result += strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}

	return result
}

// Selector provides cached field selection, exclusion, and renaming.
// Build via NewSelector; call Apply per field, or OrderFields to lay out
// the final column list following expression order.
type Selector struct {
	included     map[string]struct{} // lowercase exact includes
	excluded     map[string]struct{} // lowercase exact excludes
	renamed      map[string]string   // lowercase old -> new name
	includeGlobs []string            // glob patterns to include (lowercase)
	excludeGlobs []string            // glob patterns to exclude (lowercase)
	exprs        []string            // original expressions in the order given
	all          bool                // true if "*" was specified
	excludeOnly  bool                // true if only exclusions were specified (implies select-all)
	cache        map[string]string   // cached new name
	casing       *ColumnCasing
	ConnType     dbio.Type
}

// NewSelector creates a Selector from select expressions.
// All field matching is case-insensitive.
func NewSelector(selectExprs []string, casing ColumnCasing) *Selector {
	s := &Selector{
		included: make(map[string]struct{}),
		excluded: make(map[string]struct{}),
		renamed:  make(map[string]string),
		cache:    make(map[string]string),
		casing:   &casing,
	}

	for _, expr := range selectExprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		s.exprs = append(s.exprs, expr)
		if expr == "*" {
			s.all = true
			continue
		}

		field, newName, isExclude, _ := ParseSelectExpr(expr)
		fieldLower := strings.ToLower(field)

		if isExclude {
			if strings.Contains(field, "*") {
				s.excludeGlobs = append(s.excludeGlobs, fieldLower)
			} else {
				s.excluded[fieldLower] = struct{}{}
			}
		} else if newName != "" {
			s.renamed[fieldLower] = newName
		} else {
			if strings.Contains(field, "*") {
				s.includeGlobs = append(s.includeGlobs, fieldLower)
			} else {
				s.included[fieldLower] = struct{}{}
			}
		}
	}

	// Exclude-only lists imply "*" (select-all-except), matching replication
	// `select` semantics.
	if !s.all && len(s.included) == 0 && len(s.includeGlobs) == 0 && len(s.renamed) == 0 &&
		(len(s.excluded) > 0 || len(s.excludeGlobs) > 0) {
		s.excludeOnly = true
	}

	return s
}

// Apply returns the (possibly renamed) field name and whether it's included.
// Priority: rename > exclude > glob exclude > All > include > glob include.
// Results are cached per field.
func (s *Selector) Apply(name string) (new string, included bool) {
	nameLower := strings.ToLower(name)
	if res, ok := s.cache[nameLower]; ok {
		if res == "" {
			return "", false
		}
		return res, true
	}
	res := s.compute(name, nameLower)
	s.cache[nameLower] = res
	if res == "" {
		return "", false
	}
	return res, true
}

// compute calculates the uncached selector result. Returns "" when excluded.
func (s *Selector) compute(name, nameLower string) string {
	if s.casing != nil {
		name = s.casing.Apply(name, s.ConnType)
	}
	if newName, ok := s.renamed[nameLower]; ok {
		return newName
	}
	if _, excluded := s.excluded[nameLower]; excluded {
		return ""
	}
	for _, pattern := range s.excludeGlobs {
		if MatchesSelectGlob(nameLower, pattern) {
			return ""
		}
	}
	if s.all || s.excludeOnly {
		return name
	}
	if _, included := s.included[nameLower]; included {
		return name
	}
	for _, pattern := range s.includeGlobs {
		if MatchesSelectGlob(nameLower, pattern) {
			return name
		}
	}
	return ""
}

// OrderFields lays out final output names following expression order.
// Source-order is preserved within each `*` or glob expansion. Exact
// names pin to their list position; renames keep source-order position;
// `*`/globs skip pins and excluded names. `excludeOnly` selectors
// behave as if `*` were prepended.
func (s *Selector) OrderFields(fields []string) []string {
	if len(s.exprs) == 0 && !s.excludeOnly {
		return fields
	}

	isExcluded := func(nameLower string) bool {
		if _, ok := s.excluded[nameLower]; ok {
			return true
		}
		for _, pattern := range s.excludeGlobs {
			if MatchesSelectGlob(nameLower, pattern) {
				return true
			}
		}
		return false
	}

	// Bare exact names pin position; `*`/globs skip them.
	pinned := map[string]struct{}{}
	for _, expr := range s.exprs {
		field, newName, isExclude, _ := ParseSelectExpr(strings.TrimSpace(expr))
		if isExclude || strings.Contains(field, "*") || field == "" || newName != "" {
			continue
		}
		pinned[strings.ToLower(field)] = struct{}{}
	}

	emitted := make(map[string]struct{}, len(fields))
	result := make([]string, 0, len(fields))

	emitName := func(srcName, nameLower string, fromStar bool) {
		if _, done := emitted[nameLower]; done {
			return
		}
		if isExcluded(nameLower) {
			emitted[nameLower] = struct{}{}
			return
		}
		if fromStar {
			if _, isPin := pinned[nameLower]; isPin {
				return
			}
		}
		emitted[nameLower] = struct{}{}
		if newName, ok := s.renamed[nameLower]; ok {
			result = append(result, newName)
			return
		}
		if s.casing != nil {
			result = append(result, s.casing.Apply(srcName, s.ConnType))
			return
		}
		result = append(result, srcName)
	}

	exprs := s.exprs
	if s.excludeOnly {
		exprs = append([]string{"*"}, exprs...)
	}

	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}

		if expr == "*" {
			for _, f := range fields {
				emitName(f, strings.ToLower(f), true)
			}
			continue
		}

		field, _, isExclude, _ := ParseSelectExpr(expr)
		if isExclude {
			continue
		}

		fieldLower := strings.ToLower(field)
		if strings.Contains(field, "*") {
			for _, f := range fields {
				if MatchesSelectGlob(strings.ToLower(f), fieldLower) {
					emitName(f, strings.ToLower(f), true)
				}
			}
			continue
		}

		// Exact include — silently skip missing fields (ApplySelect errors).
		var matched string
		for _, f := range fields {
			if strings.EqualFold(f, field) {
				matched = f
				break
			}
		}
		if matched == "" {
			continue
		}
		emitName(matched, strings.ToLower(matched), false)
	}

	return result
}

// ApplySelect filters, renames, and reorders fields per the select grammar:
//
//	"field" include · "-field" exclude · "field as new" rename
//	"*" all-not-pinned · "prefix*"/"*suffix" glob include · "-prefix*"/"-*suffix" glob exclude
//
// Output follows expression order; `*` and globs expand in source order and
// skip names pinned elsewhere in the list (so `[id, *, created_at]` keeps
// `created_at` at the tail). Matching is case-insensitive. In explicit mode
// (no `*`), an exact include of a missing source field is a hard error.
func ApplySelect(fields []string, selectExprs []string) (newFields []string, err error) {
	if len(selectExprs) == 0 {
		return fields, nil
	}

	// Explicit-mode missing-field errors are gated on the absence of `*`.
	hasSelectAll := false
	for _, expr := range selectExprs {
		if strings.TrimSpace(expr) == "*" {
			hasSelectAll = true
			break
		}
	}

	// Renames don't pin — `[*, firstName as first_name]` keeps the column in
	// source-order position. Only bare exact names pin.
	excludedExact := map[string]struct{}{}
	excludeGlobs := []string{}
	renames := map[string]string{}
	pinned := map[string]struct{}{}
	for _, expr := range selectExprs {
		field, newName, isExclude, perr := ParseSelectExpr(strings.TrimSpace(expr))
		if perr != nil {
			return nil, perr
		}
		if isExclude {
			if strings.Contains(field, "*") {
				excludeGlobs = append(excludeGlobs, strings.ToLower(field))
			} else {
				excludedExact[strings.ToLower(field)] = struct{}{}
			}
			continue
		}
		if newName != "" {
			renames[strings.ToLower(field)] = newName
			continue
		}
		if !isSelectGlob(field, newName) && field != "" {
			pinned[strings.ToLower(field)] = struct{}{}
		}
	}

	isExcluded := func(nameLower string) bool {
		if _, ok := excludedExact[nameLower]; ok {
			return true
		}
		for _, pattern := range excludeGlobs {
			if MatchesSelectGlob(nameLower, pattern) {
				return true
			}
		}
		return false
	}
	displayName := func(srcName, nameLower string) string {
		if newName, ok := renames[nameLower]; ok {
			return newName
		}
		return srcName
	}

	emitted := make(map[string]struct{}, len(fields))
	newFields = make([]string, 0, len(fields))

	for _, expr := range selectExprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}

		if expr == "*" {
			for _, f := range fields {
				fl := strings.ToLower(f)
				if _, done := emitted[fl]; done {
					continue
				}
				if isExcluded(fl) {
					continue
				}
				if _, isPin := pinned[fl]; isPin {
					continue
				}
				emitted[fl] = struct{}{}
				newFields = append(newFields, displayName(f, fl))
			}
			continue
		}

		field, newName, isExclude, perr := ParseSelectExpr(expr)
		if perr != nil {
			return nil, perr
		}
		if isExclude {
			continue
		}

		fieldLower := strings.ToLower(field)
		matchedGlob := false
		if isSelectGlob(field, newName) {
			for _, f := range fields {
				fl := strings.ToLower(f)
				if _, done := emitted[fl]; done {
					continue
				}
				if isExcluded(fl) {
					continue
				}
				if _, isPin := pinned[fl]; isPin {
					continue
				}
				if MatchesSelectGlob(fl, fieldLower) {
					matchedGlob = true
					emitted[fl] = struct{}{}
					newFields = append(newFields, displayName(f, fl))
				}
			}
			if !matchedGlob {
				g.Warn("select pattern '%s' matched 0 columns", field)
			}
			continue
		}

		// Exact include: case-insensitive match preserves source casing.
		var matched string
		var matchedLower string
		for _, f := range fields {
			if strings.EqualFold(f, field) {
				matched = f
				matchedLower = strings.ToLower(f)
				break
			}
		}
		if matched == "" {
			if newName != "" {
				// computed expression: contributes the alias as an output
				// column name. A bare name is a typo, and still errors.
				if !isSQLExpr(field) {
					return nil, g.Error("field '%s' not found for rename", field)
				}
				if _, done := emitted[strings.ToLower(newName)]; done {
					continue
				}
				emitted[strings.ToLower(newName)] = struct{}{}
				newFields = append(newFields, newName)
				continue
			}
			if !hasSelectAll {
				return nil, g.Error("field '%s' not found", field)
			}
			continue
		}
		if _, done := emitted[matchedLower]; done {
			continue
		}
		emitted[matchedLower] = struct{}{}
		newFields = append(newFields, displayName(matched, matchedLower))
	}

	return newFields, nil
}

// ApplySelectExprs is ApplySelect but emits "field as alias" in the output
// instead of the post-rename name — for SQL builders that need the source
// column name before the AS keyword. Semantics otherwise identical.
func ApplySelectExprs(fields []string, selectExprs []string) (newFields []string, err error) {
	if len(selectExprs) == 0 {
		return fields, nil
	}

	hasSelectAll := false
	for _, expr := range selectExprs {
		if strings.TrimSpace(expr) == "*" {
			hasSelectAll = true
			break
		}
	}

	excludedExact := map[string]struct{}{}
	excludeGlobs := []string{}
	renames := map[string]string{}
	exprAliases := map[string]struct{}{}
	pinned := map[string]struct{}{}
	for _, expr := range selectExprs {
		field, newName, isExclude, perr := ParseSelectExpr(strings.TrimSpace(expr))
		if perr != nil {
			return nil, perr
		}
		if isExclude {
			if strings.Contains(field, "*") {
				excludeGlobs = append(excludeGlobs, strings.ToLower(field))
			} else {
				excludedExact[strings.ToLower(field)] = struct{}{}
			}
			continue
		}
		if newName != "" {
			// a computed expression aliased over an existing column name
			// replaces it; `*` must not also emit the raw column
			if isSQLExpr(field) {
				exprAliases[strings.ToLower(newName)] = struct{}{}
				continue
			}
			renames[strings.ToLower(field)] = newName
			continue
		}
		if !isSelectGlob(field, newName) && field != "" {
			pinned[strings.ToLower(field)] = struct{}{}
		}
	}

	isExcluded := func(nameLower string) bool {
		if _, ok := excludedExact[nameLower]; ok {
			return true
		}
		if _, ok := exprAliases[nameLower]; ok {
			return true
		}
		for _, pattern := range excludeGlobs {
			if MatchesSelectGlob(nameLower, pattern) {
				return true
			}
		}
		return false
	}
	emitExpr := func(srcName, nameLower string) string {
		if newName, ok := renames[nameLower]; ok {
			return srcName + " as " + newName
		}
		return srcName
	}

	emitted := make(map[string]struct{}, len(fields))
	newFields = make([]string, 0, len(fields))

	for _, expr := range selectExprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}

		if expr == "*" {
			for _, f := range fields {
				fl := strings.ToLower(f)
				if _, done := emitted[fl]; done {
					continue
				}
				if isExcluded(fl) {
					continue
				}
				if _, isPin := pinned[fl]; isPin {
					continue
				}
				emitted[fl] = struct{}{}
				newFields = append(newFields, emitExpr(f, fl))
			}
			continue
		}

		field, newName, isExclude, perr := ParseSelectExpr(expr)
		if perr != nil {
			return nil, perr
		}
		if isExclude {
			continue
		}

		fieldLower := strings.ToLower(field)
		matchedGlob := false
		if isSelectGlob(field, newName) {
			for _, f := range fields {
				fl := strings.ToLower(f)
				if _, done := emitted[fl]; done {
					continue
				}
				if isExcluded(fl) {
					continue
				}
				if _, isPin := pinned[fl]; isPin {
					continue
				}
				if MatchesSelectGlob(fl, fieldLower) {
					matchedGlob = true
					emitted[fl] = struct{}{}
					newFields = append(newFields, emitExpr(f, fl))
				}
			}
			if !matchedGlob {
				g.Warn("select pattern '%s' matched 0 columns", field)
			}
			continue
		}

		var matched string
		var matchedLower string
		for _, f := range fields {
			if strings.EqualFold(f, field) {
				matched = f
				matchedLower = strings.ToLower(f)
				break
			}
		}
		if matched == "" {
			if newName != "" {
				// pass through unmatched rename as a SQL expression alias
				newFields = append(newFields, field+" as "+newName)
				continue
			}
			if !hasSelectAll {
				return nil, g.Error("field '%s' not found", field)
			}
			continue
		}
		if _, done := emitted[matchedLower]; done {
			continue
		}
		emitted[matchedLower] = struct{}{}
		newFields = append(newFields, emitExpr(matched, matchedLower))
	}

	return newFields, nil
}

// ParseSelectExpr splits one select expression into (field, newName, exclude).
// Recognizes "-field" exclusion and case-insensitive " as " rename; the latter
// requires surrounding spaces so column names like "has_value" don't match.
func ParseSelectExpr(expr string) (field string, newName string, exclude bool, err error) {
	expr = strings.TrimSpace(expr)

	if strings.HasPrefix(expr, "-") {
		exclude = true
		expr = strings.TrimPrefix(expr, "-")
	}

	parts := strings.SplitN(strings.ToLower(expr), " as ", 2)
	if len(parts) == 2 {
		field = strings.TrimSpace(expr[:len(parts[0])])
		newName = strings.TrimSpace(expr[len(parts[0])+4:]) // +4 for " as "
		if exclude {
			return "", "", false, g.Error("cannot combine exclusion (-) with rename (as): %s", expr)
		}
		return field, newName, false, nil
	}

	field = strings.TrimSpace(expr)
	return field, "", exclude, nil
}

// isSQLExpr reports whether field is a computed SQL expression rather than a
// bare column reference. Parens or quotes mean the text can't be an identifier,
// so a `*` inside it (`concat('a*', id)`, JSONPath `'$[*].amount'`) is data,
// not a glob — and an unmatched name is intentional, not a typo.
func isSQLExpr(field string) bool {
	return strings.ContainsAny(field, "('\"`")
}

// isSelectGlob reports whether field should be treated as a column-name glob.
// An aliased SQL expression is never a glob.
func isSelectGlob(field, newName string) bool {
	if newName != "" && isSQLExpr(field) {
		return false
	}
	return strings.Contains(field, "*")
}

// MatchesSelectGlob matches name against a simple glob (prefix*, *suffix,
// *middle*, prefix*suffix). Both inputs must already be lowercased.
func MatchesSelectGlob(name, pattern string) bool {
	if !strings.Contains(pattern, "*") {
		return name == pattern
	}

	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		return strings.Contains(name, strings.Trim(pattern, "*"))
	} else if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(name, strings.TrimPrefix(pattern, "*"))
	} else if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
	}
	// prefix*suffix
	idx := strings.Index(pattern, "*")
	return strings.HasPrefix(name, pattern[:idx]) && strings.HasSuffix(name, pattern[idx+1:])
}

// ColumnTyping contains type-specific mapping configurations
type ColumnTyping struct {
	String  *StringColumnTyping  `json:"string,omitempty" yaml:"string,omitempty"`
	Decimal *DecimalColumnTyping `json:"decimal,omitempty" yaml:"decimal,omitempty"`
	JSON    *JsonColumnTyping    `json:"json,omitempty" yaml:"json,omitempty"`
	Boolean *BooleanColumnTyping `json:"boolean,omitempty" yaml:"boolean,omitempty"`
}

func (ct *ColumnTyping) MaxDecimals() int {
	if ct == nil {
		return -1
	}

	dec := g.PtrVal(ct.Decimal)
	if dec.MaxScale > 0 {
		return dec.MaxScale
	} else if minScale := g.PtrVal(dec.MinScale); minScale > 0 {
		return minScale
	}

	return -1
}

// StringColumnTyping contains string type mapping configurations
type StringColumnTyping struct {
	LengthFactor int  `json:"length_factor,omitempty" yaml:"length_factor,omitempty"`
	MinLength    int  `json:"min_length,omitempty" yaml:"min_length,omitempty"`
	MaxLength    int  `json:"max_length,omitempty" yaml:"max_length,omitempty"`
	UseMax       bool `json:"use_max,omitempty" yaml:"use_max,omitempty"`

	Note string `json:"note,omitempty" yaml:"note,omitempty"`
}

func (sct *StringColumnTyping) Apply(length, max int) (newLength int) {
	if sct.MaxLength > 0 && sct.MaxLength < max {
		// cap at MaxLength when it's smaller than the DB's native max
		max = sct.MaxLength
	} else if sct.MaxLength > max {
		max = sct.MaxLength
	}
	if max == 0 {
		max = 4000 // some safe large max
	}

	if sct.UseMax {
		return max
	}

	if sct.LengthFactor > 0 {
		newLength = length * sct.LengthFactor
		if newLength > max {
			return max
		}
		if newLength < sct.MinLength {
			return sct.MinLength
		}
		return newLength
	}

	if length < sct.MinLength {
		return sct.MinLength
	}

	// cap at max_length when set
	if sct.MaxLength > 0 && length > max {
		return max
	}

	return length
}

// DecimalColumnTyping contains decimal type mapping configurations
type DecimalColumnTyping struct {
	MinPrecision *int   `json:"min_precision,omitempty" yaml:"min_precision,omitempty"` // Total number of digits
	MaxPrecision int    `json:"max_precision,omitempty" yaml:"max_precision,omitempty"` // Total number of digits
	MinScale     *int   `json:"min_scale,omitempty" yaml:"min_scale,omitempty"`         // Number of digits after decimal point
	MaxScale     int    `json:"max_scale,omitempty" yaml:"max_scale,omitempty"`
	CastAs       string `json:"cast_as,omitempty" yaml:"cast_as,omitempty"`
}

func (dct *DecimalColumnTyping) Apply(col Column) (precision, scale int) {

	minPrecision := col.Stats.MaxLen
	precision = col.DbPrecision
	scale = col.DbScale

	if precision == 0 {
		if scale == 0 {
			minScale := col.Stats.MaxDecLen
			scale = lo.Ternary(scale < env.DdlMinDecScale, env.DdlMinDecScale, scale)
			scale = lo.Ternary(scale < minScale, minScale, scale)
		}
		precision = lo.Ternary(precision < (scale*2), scale*2, precision)
		precision = lo.Ternary(precision < env.DdlMinDecLength, env.DdlMinDecLength, precision)
		precision = lo.Ternary(precision < minPrecision, minPrecision, precision)
	}

	if !col.Sourced {
		dct.MinScale = lo.Ternary(dct.MinScale == nil, g.Ptr(env.DdlMinDecScale), dct.MinScale)
		dct.MaxScale = lo.Ternary(dct.MaxScale == 0, env.DdlMaxDecScale, dct.MaxScale)
		dct.MinPrecision = lo.Ternary(dct.MinPrecision == nil, g.Ptr(env.DdlMinDecLength), dct.MinPrecision)
		dct.MaxPrecision = lo.Ternary(dct.MaxPrecision == 0, env.DdlMaxDecLength, dct.MaxPrecision)

		precisionDelta := lo.Ternary(precision > env.DdlMinDecLength, precision-env.DdlMinDecLength, 0)
		scaleDelta := lo.Ternary(scale > env.DdlMinDecScale, scale-env.DdlMinDecScale, 0)
		precision = env.DdlMinDecLength + precisionDelta + scaleDelta // safe if scale if present
	}

	if dct.MinPrecision != nil && precision < *dct.MinPrecision {
		precision = *dct.MinPrecision
	}

	if precision < minPrecision {
		precision = minPrecision
	}

	if dct.MaxPrecision > 0 && precision > dct.MaxPrecision {
		precision = dct.MaxPrecision
	}

	if dct.MinScale != nil && scale < *dct.MinScale {
		scale = *dct.MinScale
	}

	if dct.MaxScale > 0 && scale > dct.MaxScale {
		scale = dct.MaxScale
	}

	return
}

// JsonColumnTyping contains json type mapping configurations
type JsonColumnTyping struct {
	AsText bool `json:"as_text,omitempty" yaml:"as_text,omitempty"`
}

func (jct *JsonColumnTyping) Apply(col *Column) {
	if jct.AsText {
		// set to text type
		col.Type = TextType
	}
}

// BooleanColumnTyping contains boolean type mapping configurations
type BooleanColumnTyping struct {
	CastAs string `json:"cast_as,omitempty" yaml:"cast_as,omitempty"` // "integer" or "string"
}

func (bct *BooleanColumnTyping) Apply(col *Column) {
	switch strings.ToLower(bct.CastAs) {
	case "integer":
		col.Type = SmallIntType
	case "string":
		col.Type = StringType
		col.Sourced = true
		col.DbPrecision = 10
	}
}
