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

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen       int    `json:"min_len,omitempty"`
	MaxLen       int    `json:"max_len,omitempty"`
	MaxDecLen    int    `json:"max_dec_len,omitempty"`
	Min          int64  `json:"min"`
	Max          int64  `json:"max"`
	NullCnt      int64  `json:"null_cnt"`
	IntCnt       int64  `json:"int_cnt,omitempty"`
	DecCnt       int64  `json:"dec_cnt,omitempty"`
	BoolCnt      int64  `json:"bool_cnt,omitempty"`
	JsonCnt      int64  `json:"json_cnt,omitempty"`
	StringCnt    int64  `json:"string_cnt,omitempty"`
	DateCnt      int64  `json:"date_cnt,omitempty"`
	DateTimeCnt  int64  `json:"datetime_cnt,omitempty"`
	DateTimeZCnt int64  `json:"datetimez_cnt,omitempty"`
	TotalCnt     int64  `json:"total_cnt"`
	UniqCnt      int64  `json:"uniq_cnt"`
	Checksum     uint64 `json:"checksum"`
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

// NewColumnsFromFields creates Columns from fields
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
			if col.Type.IsValid() {
				g.Debug("casting column '%s' as '%s'", col.Name, col.Type)
				newCols[i].Type = col.Type
				newCols[i].Stats.MaxLen = lo.Ternary(col.Stats.MaxLen > 0, col.Stats.MaxLen, newCols[i].Stats.MaxLen)
				newCols[i].DbPrecision = lo.Ternary(col.DbPrecision > 0, col.DbPrecision, newCols[i].DbPrecision)
				newCols[i].DbScale = lo.Ternary(col.DbScale > 0, col.DbScale, newCols[i].DbScale)
				newCols[i].Sourced = true
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
			newCol.Position = len(cols)
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

		if colStats.TotalCnt == 0 || colStats.NullCnt == colStats.TotalCnt || col.Sourced {
			// do nothing, keep existing type if defined
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
		} else if colStats.DecCnt > 0 && colStats.DecCnt+colStats.IntCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = DecimalType
			col.goType = reflect.TypeOf(float64(0.0))
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
		if c.FailCnt <= 20 {
			errMsg := g.F("constraint failure for column '%s', at row number %d, for value: %s", col.Name, sp.N, cast.ToString(value))
			g.Warn(errMsg)
			c.Errors = append(c.Errors, errMsg)
			return g.Error(errMsg)
		}
	}
	return
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
	case StringType, TextType, JsonType, TimeType, BinaryType, UUIDType:
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

// IsValid returns whether the column has a valid type
func (ct ColumnType) IsValid() bool {
	return ct.IsBinary() || ct.IsString() || ct.IsJSON() || ct.IsNumber() || ct.IsBool() || ct.IsDate() || ct.IsDatetime()
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

	// Add precision as needed
	if strings.HasSuffix(nativeType, "()") {
		maxStringLength := cast.ToInt(template.Value("variable.max_string_length"))
		maxStringType := template.Value("variable.max_string_type")

		length := col.Stats.MaxLen
		if col.IsString() {
			isSourced := col.Sourced && col.DbPrecision > 0
			if isSourced {
				// string length was manually provided
				length = col.DbPrecision
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
			} else if length <= 0 {
				length = col.Stats.MaxLen * 2
				if length < 255 {
					length = 255
				}
			}

			if !isSourced && maxStringType != "" {
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
		} else if col.IsInteger() {
			if !col.Sourced && length < env.DdlDefDecLength {
				length = env.DdlDefDecLength
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		}
	} else if strings.Contains(nativeType, "(,)") {

		precision := col.DbPrecision
		scale := col.DbScale

		if col.IsDecimal() {
			if ct.Decimal == nil {
				ct.Decimal = &DecimalColumnTyping{}
			}
			precision, scale = ct.Decimal.Apply(col)
		}

		nativeType = strings.ReplaceAll(
			nativeType,
			"(,)",
			fmt.Sprintf("(%d,%d)", precision, scale),
		)
	} else if col.Type.IsJSON() {
		if ct.JSON != nil {
			ct.JSON.Apply(col)
		}
	}

	return
}

func NativeTypeToGeneral(name, dbType string, connType dbio.Type) (colType ColumnType) {
	dbType = strings.ToLower(dbType)

	if connType == dbio.TypeDbClickhouse {
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = strings.ReplaceAll(dbType, "nullable(", "")
			dbType = strings.TrimSuffix(dbType, ")")
		}
	} else if connType == dbio.TypeDbProton {
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = strings.ReplaceAll(dbType, "nullable(", "")
			dbType = strings.TrimSuffix(dbType, ")")
		}
	} else if connType == dbio.TypeDbDuckDb || connType == dbio.TypeDbMotherDuck {
		if strings.HasSuffix(dbType, "[]") {
			dbType = "list"
		}
	}

	dbType = strings.Split(strings.ToLower(dbType), "(")[0]
	dbType = strings.Split(dbType, "<")[0]

	template, _ := connType.Template()
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
	case cc.Equals(TargetColumnCasing), cc.Equals(SnakeColumnCasing):
		// lower case for target file system
		if tgtConnType.DBNameUpperCase() {
			return strings.ToUpper(name)
		}
		return strings.ToLower(name)
	}

	return name
}

// ColumnTyping contains type-specific mapping configurations
type ColumnTyping struct {
	String  *StringColumnTyping  `json:"string,omitempty" yaml:"string,omitempty"`
	Decimal *DecimalColumnTyping `json:"decimal,omitempty" yaml:"decimal,omitempty"`
	JSON    *JsonColumnTyping    `json:"json,omitempty" yaml:"json,omitempty"`
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
	if sct.MaxLength > max {
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

	return length
}

// DecimalColumnTyping contains decimal type mapping configurations
type DecimalColumnTyping struct {
	MinPrecision *int `json:"min_precision,omitempty" yaml:"min_precision,omitempty"` // Total number of digits
	MaxPrecision int  `json:"max_precision,omitempty" yaml:"max_precision,omitempty"` // Total number of digits
	MinScale     *int `json:"min_scale,omitempty" yaml:"min_scale,omitempty"`         // Number of digits after decimal point
	MaxScale     int  `json:"max_scale,omitempty" yaml:"max_scale,omitempty"`         // Number of digits after decimal point
}

func (dct *DecimalColumnTyping) Apply(col *Column) (precision, scale int) {

	precision = col.DbPrecision
	scale = col.DbScale
	if col.Stats.MaxDecLen > scale {
		scale = col.Stats.MaxDecLen
	}

	if precision == 0 {
		minPrecision := col.Stats.MaxLen + scale
		precision = lo.Ternary(precision < (scale*2), scale*2, precision)
		precision = lo.Ternary(precision < minPrecision, minPrecision, precision)
	} else if col.Sourced {
		return
	}

	dct.MinScale = lo.Ternary(dct.MinScale == nil, g.Ptr(env.DdlMinDecScale), dct.MinScale)
	dct.MaxScale = lo.Ternary(dct.MaxScale == 0, env.DdlMaxDecScale, dct.MaxScale)
	dct.MinPrecision = lo.Ternary(dct.MinPrecision == nil, g.Ptr(env.DdlMinDecLength), dct.MinPrecision)
	dct.MaxPrecision = lo.Ternary(dct.MaxPrecision == 0, env.DdlMaxDecLength, dct.MaxPrecision)

	if precision < *dct.MinPrecision {
		precision = *dct.MinPrecision
	}

	if precision > dct.MaxPrecision {
		precision = dct.MaxPrecision
	}

	if scale < *dct.MinScale {
		scale = *dct.MinScale
	}

	if scale > dct.MaxScale {
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
