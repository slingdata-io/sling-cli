package iop

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

var (
	// RemoveTrailingDecZeros removes the trailing zeros in CastToString
	RemoveTrailingDecZeros = false
	SampleSize             = 900
	replacePattern         = regexp.MustCompile("[^_0-9a-zA-Z]+") // to clean header fields
	regexFirstDigit        = *regexp.MustCompile(`^\d`)
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

	Metadata map[string]string `json:"metadata,omitempty"`
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
	TextType       ColumnType = "text"
	TimestampType  ColumnType = "timestamp"
	TimestampzType ColumnType = "timestampz"
	FloatType      ColumnType = "float"
	TimeType       ColumnType = "time"
	TimezType      ColumnType = "timez"
)

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

var KeyTypes = []KeyType{AggregateKey, ClusterKey, DuplicateKey, HashKey, PartitionKey, PrimaryKey, SortKey, UniqueKey, UpdateKey}

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen    int    `json:"min_len,omitempty"`
	MaxLen    int    `json:"max_len,omitempty"`
	MaxDecLen int    `json:"max_dec_len,omitempty"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
	NullCnt   int64  `json:"null_cnt"`
	IntCnt    int64  `json:"int_cnt,omitempty"`
	DecCnt    int64  `json:"dec_cnt,omitempty"`
	BoolCnt   int64  `json:"bool_cnt,omitempty"`
	JsonCnt   int64  `json:"json_cnt,omitempty"`
	StringCnt int64  `json:"string_cnt,omitempty"`
	DateCnt   int64  `json:"date_cnt,omitempty"`
	TotalCnt  int64  `json:"total_cnt"`
	UniqCnt   int64  `json:"uniq_cnt"`
	Checksum  uint64 `json:"checksum"`
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
	if os.Getenv("SAMPLE_SIZE") != "" {
		SampleSize = cast.ToInt(os.Getenv("SAMPLE_SIZE"))
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

// GetKeys gets key columns
func (cols Columns) GetKeys(keyType KeyType) Columns {
	keys := Columns{}
	for _, col := range cols {
		key := string(keyType) + "_key"
		if cast.ToBool(col.Metadata[key]) {
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
				key := string(keyType) + "_key"
				col.SetMetadata(key, "true")
				cols[i] = col
				found = true
			}
		}
		if !found {
			return g.Error("could not set %s key. Did not find column %s", keyType, colName)
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

		fields[j] = g.F("%s [%s | %s]", field, column.Type, column.DbType)
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
	tgtColNames := tgtColumns.Names()
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

		// srcRec := srcColumns.MakeRec(srcRow)
		// tgtRec := tgtColumns.MakeRec(tgtRow)
		// for k := range srcRec {
		// 	if srcRec[k] != tgtRec[k] {
		// 		sI := lo.IndexOf(srcColNames, strings.ToLower(k))
		// 		tI := lo.IndexOf(tgtColNames, strings.ToLower(k))
		// 		g.Warn("Key `%s` is mapped from %d to %d => %#v != %#v", k, sI, tI, srcRec[k], tgtRec[k])
		// 	}
		// }

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
func (cols Columns) Coerce(castCols Columns, hasHeader bool) (newCols Columns) {
	newCols = cols
	colMap := castCols.FieldMap(true)
	for i, col := range newCols {
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

		if j, found := colMap[strings.ToLower(col.Name)]; found {
			col = castCols[j]
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
func (cols Columns) GetColumn(name string) Column {
	colsMap := map[string]Column{}
	for _, col := range cols {
		colsMap[strings.ToLower(col.Name)] = col
	}
	return colsMap[strings.ToLower(name)]
}

func (cols Columns) Add(newCols Columns, overwrite bool) (col2 Columns, added Columns) {
	existingIndexMap := cols.FieldMap(true)
	for _, newCol := range newCols {
		key := strings.ToLower(newCol.Name)
		if i, ok := existingIndexMap[key]; ok {
			if overwrite {
				newCol.Position = i + 1
				cols[i] = newCol
			} else if cols[i].Type != newCol.Type && newCol.Stats.TotalCnt > newCol.Stats.NullCnt {
				warn := true
				switch {
				case newCol.Type.IsString() && !cols[i].Type.IsString():
				case newCol.Type.IsNumber() && !cols[i].Type.IsNumber():
				case newCol.Type.IsBool() && !cols[i].Type.IsBool():
				case newCol.Type.IsDatetime() && !cols[i].Type.IsDatetime():
				default:
					warn = false
				}

				if warn && g.IsDebugLow() {
					g.Warn("Columns.Add Type mismatch for %s > %s != %s", newCol.Name, cols[i].Type, newCol.Type)
				}
			}
		} else {
			newCol.Position = len(cols)
			cols = append(cols, newCol)
			added = append(added, newCol)
		}
	}
	return cols, added
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
		} else if colStats.StringCnt > 0 {
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
		} else if colStats.JsonCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = JsonType
			col.goType = reflect.TypeOf("json")
		} else if colStats.BoolCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = BoolType
			col.goType = reflect.TypeOf(true)
			colStats.Min = 0
		} else if colStats.IntCnt+colStats.NullCnt == colStats.TotalCnt {
			if colStats.Min*10 < -2147483648 || colStats.Max*10 > 2147483647 {
				col.Type = BigIntType
			} else {
				col.Type = IntegerType
			}
			col.goType = reflect.TypeOf(int64(0))

			// if safe {
			// 	// cast as decimal for safety
			// 	col.Type = DecimalType
			// 	col.goType = reflect.TypeOf(float64(0.0))
			// }
		} else if colStats.DateCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = DatetimeType
			col.goType = reflect.TypeOf(time.Now())
			colStats.Min = 0
		} else if colStats.DecCnt+colStats.IntCnt+colStats.NullCnt == colStats.TotalCnt {
			col.Type = DecimalType
			col.goType = reflect.TypeOf(float64(0.0))
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
	}
}

func (col *Column) SetMetadata(key string, value string) {
	if col.Metadata == nil {
		col.Metadata = map[string]string{}
	}
	col.Metadata[key] = value
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
	case col.IsDatetime():
		return reflect.TypeOf(time.Now())
	case col.IsDecimal():
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

// IsString returns whether the column is a string
func (col *Column) IsString() bool {
	return col.Type.IsString()
}

// IsInteger returns whether the column is an integer
func (col *Column) IsInteger() bool {
	return col.Type.IsInteger()
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

// IsDatetime returns whether the column is a datetime object
func (col *Column) IsDatetime() bool {
	return col.Type.IsDatetime()
}

// IsString returns whether the column is a string
func (ct ColumnType) IsString() bool {
	switch ct {
	case StringType, TextType, JsonType, TimeType, BinaryType, "":
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

// IsDecimal returns whether the column is a decimal
func (ct ColumnType) IsDecimal() bool {
	return ct == FloatType || ct == DecimalType
}

// IsNumber returns whether the column is a decimal or an integer
func (ct ColumnType) IsNumber() bool {
	return ct.IsInteger() || ct.IsDecimal()
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
	case DatetimeType, DateType, TimestampType, TimestampzType:
		return true
	}
	return false
}

// IsValid returns whether the column has a valid type
func (ct ColumnType) IsValid() bool {
	return ct.IsString() || ct.IsJSON() || ct.IsNumber() || ct.IsBool() || ct.IsDatetime()
}
