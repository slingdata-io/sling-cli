package database

import (
	"context"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

/*
we have tables and where have no info on primary keys.

1. determine all PKs
- get columns
- get a sample, and iterate on each row. do
select
	count(*) tot_cnt,
	count({field}) {field}_cnt,
	count({field}) {field}_distct_cnt,
	min,
	max,
	min_len,
	max_len
from (
	select * from {schema}.{table}
	limit {limit}
) t

on each row, determine the unique ones and cross-reference on potential join matches and types
*/

type DataAnalyzerOptions struct {
	DbName      string
	SchemaNames []string
}

type DataAnalyzer struct {
	Conn        Connection
	Schemata    Schemata
	ColumnMap   map[string]iop.Column
	RelationMap map[string]map[string]map[string]Relation // table > column A > column B > relation
	Options     DataAnalyzerOptions
}

type Relation string

const RelationOneToOne = "one_to_one"
const RelationOneToMany = "one_to_many"
const RelationManyToOne = "many_to_one"
const RelationManyToMany = "many_to_many"

func NewDataAnalyzer(conn Connection, opts DataAnalyzerOptions) (da *DataAnalyzer, err error) {
	if len(opts.SchemaNames) == 0 {
		err = g.Error("must provide SchemaNames")
		return
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "could not connect to database")
		return
	}

	da = &DataAnalyzer{
		Conn:        conn,
		Options:     opts,
		ColumnMap:   map[string]iop.Column{},
		RelationMap: map[string]map[string]map[string]Relation{},
		Schemata:    Schemata{Databases: map[string]Database{}},
	}

	return
}

func (da *DataAnalyzer) GetSchemata(force bool) (err error) {
	if !(force || len(da.Schemata.Databases) == 0) {
		return nil
	}

	for _, schema := range da.Options.SchemaNames {
		g.Info("getting schemata for %s", schema)
		schemata, err := da.Conn.GetSchemata(SchemataLevelTable, schema, "")
		if err != nil {
			return g.Error(err, "could not get schemata")
		}

		// merge into da.Schemata
		for dbKey, db := range schemata.Databases {
			if _, ok := da.Schemata.Databases[dbKey]; ok {
				for schemaKey, schema := range db.Schemas {
					da.Schemata.Databases[dbKey].Schemas[schemaKey] = schema
				}
			} else {
				da.Schemata.Databases[dbKey] = db
			}
		}
	}

	return
}

var sqlAnalyzeColumns = `
	select {cols_sql}
	from ( select * from {table} order by {order_col} desc limit {limit} ) t
`

type StatFieldSQL struct {
	Name        string
	TemplateSQL string
}

func getOrderCol(table Table) (orderCol iop.Column) {
	for _, col := range table.Columns {
		key := strings.ToLower(col.Name)
		if (col.IsDatetime() || col.IsDate() || col.IsNumber()) && strings.Contains(key, "create") {
			return col
		}
	}

	for _, col := range table.Columns {
		key := strings.ToLower(col.Name)
		if (col.IsDatetime() || col.IsDate() || col.IsNumber()) &&
			(strings.Contains(key, "update") || strings.Contains(key, "modified")) {
			return col
		}
	}

	for _, col := range table.Columns {
		key := strings.ToLower(col.Name)
		if (col.IsString() || col.IsNumber()) && key == "id" {
			return col
		}
	}

	for _, col := range table.Columns {
		key := strings.ToLower(col.Name)
		if (col.IsString() || col.IsNumber()) && (strings.HasSuffix(key, "id")) {
			return col
		}
	}

	// if none matched, get first
	return table.Columns[0]
}

func (da *DataAnalyzer) AnalyzeColumns(sampleSize int, includeViews bool) (err error) {
	err = da.GetSchemata(false)
	if err != nil {
		err = g.Error(err, "could not get schemata")
		return
	}

	fieldAsString := da.Conn.Template().Function["cast_to_string"]
	var statsFields = []StatFieldSQL{
		{"total_cnt", `count(*)`},
		{"null_cnt", `count(*) - count({field})`},
		{"uniq_cnt", `count(distinct {field})`},
		{"min_len", g.F(`min(length(%s))`, fieldAsString)},
		{"max_len", g.F(`max(length(%s))`, fieldAsString)},
		// {"value_minimun", "min({field}::text)"}, // pulls customer data
		// {"value_maximum", "max({field}::text)"}, // pulls customer data
	}

	ctx := g.NewContext(context.Background(), 2) // threads max
	analyze := func(table Table, cols []iop.Column) {
		defer ctx.Wg.Read.Done()

		colsSQL := []string{}
		for _, col := range cols {
			for _, sf := range statsFields {
				colSQL := g.R(
					g.F("%s as {alias}_%s", sf.TemplateSQL, sf.Name),
					"field", da.Conn.Quote(col.Name),
					"alias", strings.ReplaceAll(col.Name, "-", "_"),
				)
				colsSQL = append(colsSQL, colSQL)
			}
		}

		sql := g.R(
			sqlAnalyzeColumns,
			"cols_sql", strings.Join(colsSQL, ", "),
			"table", table.FDQN(),
			"order_col", getOrderCol(table).Name,
			"limit", cast.ToString(sampleSize),
		)
		data, err := da.Conn.Query(sql)
		if err != nil {
			ctx.ErrGroup.Capture(g.Error(err, "could not get analysis sql for %s", table.FullName()))
		} else if len(data.Rows) == 0 {
			ctx.ErrGroup.Capture(g.Error("got zero rows for analysis sql for %s", table.FullName()))
		}

		// retrieve values, since in order
		row := data.Rows[0]
		i := 0
		for _, col := range cols {
			m := g.M()
			for _, sf := range statsFields {
				m[sf.Name] = row[i]
				i++
			}
			// unmarshal
			err = g.Unmarshal(g.Marshal(m), &col.Stats)
			if err != nil {
				ctx.ErrGroup.Capture(g.Error(err, "could not get unmarshal sql stats for %s:\n%s", table.FullName(), g.Marshal(m)))
			}

			// store in master map
			da.ColumnMap[col.Key()] = col
			if col.IsUnique() {
				g.Info("    %s is unique [%d rows]", col.Key(), col.Stats.TotalCnt)
			}
		}
	}

	for _, table := range da.Schemata.Tables() {
		if !includeViews && table.IsView {
			continue
		}
		g.Info("analyzing table %s", table.FullName())

		tableColMap := table.ColumnsMap()
		// g.PP(tableColMap)

		// need order to retrieve values
		colsAll := lo.Filter(lo.Values(tableColMap), func(c iop.Column, i int) bool {
			// t := strings.ToLower(c.DbType)
			isText := c.IsString() && c.Type != iop.JsonType
			// isNumber := strings.Contains(t, "int") || strings.Contains(t, "double")
			isNumber := c.IsInteger()
			return isText || isNumber
			// TODO: should be string or number?
			// skip date column for now
			return !(strings.Contains(c.Name, "time") || strings.Contains(c.Name, "date"))
		})

		g.Debug("getting stats for %d columns", len(colsAll))
		// chunk to not submit too many
		for _, cols := range lo.Chunk(colsAll, 50) {
			ctx.Wg.Read.Add()
			go analyze(table, cols)
		}
	}

	ctx.Wg.Read.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return
}

func (da *DataAnalyzer) ProcessRelations() (err error) {
	err = da.ProcessRelationsString()
	if err != nil {
		return g.Error(err, "could not process relations for string columns")
	}

	err = da.ProcessRelationsInteger()
	if err != nil {
		return g.Error(err, "could not process relations for integer columns")
	}
	return
}

func (da *DataAnalyzer) ProcessRelationsString() (err error) {

	// same length text fields, uuid
	lenColMap := map[int]iop.Columns{}
	for _, col := range da.ColumnMap {
		if col.IsString() && col.Type != iop.JsonType &&
			col.Stats.MaxLen-col.Stats.MinLen <= 1 && // min/max len are about the same
			col.Stats.MinLen > 0 {
			if arr, ok := lenColMap[col.Stats.MinLen]; ok {
				lenColMap[col.Stats.MinLen] = append(arr, col)
			} else {
				lenColMap[col.Stats.MinLen] = iop.Columns{col}
			}
		}
	}

	// iterate over non-unique ones, and grab a value, and try to join to unique ones
	uniqueCols := iop.Columns{}
	nonUniqueCols := iop.Columns{}
	for lenI, cols := range lenColMap {
		if lenI > 4 && len(cols) > 1 {
			// do process
		} else if !g.In(lenI, 36, 18, 19, 28, 27) {
			// only UUID (36), sfdc id (18), stripe (18, 19, 28, 27) for now
			continue
		}
		g.Info("Got %d string columns with min length %d: %s", len(cols), lenI, g.Marshal(cols.Keys()))
		for _, col := range cols {
			if col.Stats.TotalCnt <= 1 {
				g.Debug("skipped %s", col.Key())
				continue // skip single row tables for now
			} else if col.IsUnique() {
				uniqueCols = append(uniqueCols, col)
			} else {
				nonUniqueCols = append(nonUniqueCols, col)
			}
		}
	}

	g.Info("processing string relations: OneToMany")
	err = da.GetOneToMany(uniqueCols, nonUniqueCols, true)
	if err != nil {
		return g.Error(err, "could not run GetOneToMany")
	}

	g.Info("processing string relations: OneToOne")
	err = da.GetOneToOne(uniqueCols, true)
	if err != nil {
		return g.Error(err, "could not run GetOneToOne")
	}

	g.Info("processing string relations: ManyToMany")
	err = da.GetManyToMany(nonUniqueCols, true)
	if err != nil {
		return g.Error(err, "could not run GetManyToMany")
	}

	return
}

func (da *DataAnalyzer) ProcessRelationsInteger() (err error) {

	// same length text fields, uuid
	lenColMap := map[int]iop.Columns{}
	for _, col := range da.ColumnMap {
		if col.IsInteger() && col.Stats.MaxLen > 0 {
			if arr, ok := lenColMap[col.Stats.MaxLen]; ok {
				lenColMap[col.Stats.MaxLen] = append(arr, col)
			} else {
				lenColMap[col.Stats.MaxLen] = iop.Columns{col}
			}
		}
	}

	if len(lenColMap) == 0 {
		return
	}

	// iterate over non-unique ones, and grab a value, and try to join to unique ones
	uniqueCols := iop.Columns{}
	nonUniqueCols := iop.Columns{}
	for lenI, cols := range lenColMap {
		if len(cols) < 2 {
			continue
		}

		g.Info("Got %d integer columns with max length %d: %s", len(cols), lenI, g.Marshal(cols.Keys()))
		for _, col := range cols {
			if col.Stats.TotalCnt <= 1 {
				g.Debug("skipped %s", col.Key())
				continue // skip single row tables for now
			} else if col.IsUnique() {
				uniqueCols = append(uniqueCols, col)
			} else {
				nonUniqueCols = append(nonUniqueCols, col)
			}
		}
	}

	g.Info("processing integer relations: OneToMany")
	err = da.GetOneToMany(uniqueCols, nonUniqueCols, false)
	if err != nil {
		return g.Error(err, "could not run GetOneToMany")
	}

	g.Info("processing integer relations: OneToOne")
	err = da.GetOneToOne(uniqueCols, false)
	if err != nil {
		return g.Error(err, "could not run GetOneToOne")
	}

	g.Info("processing integer relations: ManyToMany")
	err = da.GetManyToMany(nonUniqueCols, false)
	if err != nil {
		return g.Error(err, "could not run GetManyToMany")
	}

	return
}

func (da *DataAnalyzer) WriteRelationsYaml(path string) (err error) {
	out, err := yaml.Marshal(da.RelationMap)
	if err != nil {
		return g.Error(err, "could not marshal to yaml")
	}

	err = os.WriteFile(path, out, 0755)
	if err != nil {
		return g.Error(err, "could not write to yaml")
	}

	return nil
}

func (da *DataAnalyzer) GetOneToMany(uniqueCols, nonUniqueCols iop.Columns, asString bool) (err error) {
	if len(uniqueCols) == 0 || len(nonUniqueCols) == 0 {
		return g.Error("len(uniqueCols) == %d || len(nonUniqueCols) == %d", len(uniqueCols), len(nonUniqueCols))
	}
	// build all_non_unique_values
	stringType := da.Conn.Template().Function["string_type"]
	nonUniqueExpressions := lo.Map(nonUniqueCols, func(col iop.Column, i int) string {
		// integer template, matches only the max value on both sides
		template := `select * from (select '{col_key}' as non_unique_column_key, max({field}) as val from {schema}.{table} where {field} is not null limit 1) t`
		// if asString {
		// 	// string template, matches any value
		// 	template = `select * from (select '{col_key}' as non_unique_column_key, {field} as val from {schema}.{table} where {field} is not null limit 1) t`
		// }
		return g.R(
			template,
			"col_key", col.Key(),
			"field", lo.Ternary(asString, g.F("cast(%s as %s)", da.Conn.Quote(col.Name), stringType), da.Conn.Quote(col.Name)),
			"schema", da.Conn.Quote(col.Schema),
			"table", da.Conn.Quote(col.Table),
		)
	})
	nonUniqueSQL := strings.Join(nonUniqueExpressions, " union all\n    ")

	// get 1-N and N-1
	matchingSQLs := lo.Map(uniqueCols, func(col iop.Column, i int) string {
		// integer template, matches only the max value on both sides
		template := `select nuv.non_unique_column_key, '{col_key}' as unique_column_key	from all_non_unique_values nuv
				where nuv.val = ( select max({field}) from {schema}.{table} )`
		if asString {
			// string template, matches any value
			template = `select nuv.non_unique_column_key, '{col_key}' as unique_column_key	from all_non_unique_values nuv
				inner join {schema}.{table} t on {field} = nuv.val`
		}
		return g.R(
			template,
			"col_key", col.Key(),
			"field", da.Conn.Quote(col.Name),
			"field", lo.Ternary(asString, g.F("cast(t.%s as %s)", da.Conn.Quote(col.Name), stringType), da.Conn.Quote(col.Name)),
			"string_type", stringType,
			"schema", da.Conn.Quote(col.Schema),
			"table", da.Conn.Quote(col.Table),
		)
	})
	matchingSQL := strings.Join(matchingSQLs, "    union all\n    ")

	// run
	sql := g.R(`with all_non_unique_values as (
				{non_unique_sql}
			)
			, matching as (
				{matching_sql}
			)
			select unique_column_key, non_unique_column_key
			from matching
			order by unique_column_key
			`,
		"non_unique_sql", nonUniqueSQL,
		"matching_sql", matchingSQL,
	)
	data, err := da.Conn.Query(sql)
	if err != nil {
		return g.Error(err, "could not get matching columns")
	}

	for _, rec := range data.Records() {
		uniqueColumnKey := cast.ToString(rec["unique_column_key"])
		nonUniqueColumnKey := cast.ToString(rec["non_unique_column_key"])

		uniqueColumn := da.ColumnMap[uniqueColumnKey]
		nonUniqueColumn := da.ColumnMap[nonUniqueColumnKey]

		uniqueColumnTable := g.F("%s.%s", uniqueColumn.Schema, uniqueColumn.Table)
		nonUniqueColumnTable := g.F("%s.%s", nonUniqueColumn.Schema, nonUniqueColumn.Table)

		// one to many
		if mt, ok := da.RelationMap[uniqueColumnTable]; ok {
			if m, ok := mt[uniqueColumnKey]; ok {
				m[nonUniqueColumnKey] = RelationOneToMany
			} else {
				mt[uniqueColumnKey] = map[string]Relation{nonUniqueColumnKey: RelationOneToMany}
			}
		} else {
			da.RelationMap[uniqueColumnTable] = map[string]map[string]Relation{
				uniqueColumnKey: {nonUniqueColumnKey: RelationOneToMany},
			}
		}

		// many to one
		if mt, ok := da.RelationMap[nonUniqueColumnTable]; ok {
			if m, ok := mt[nonUniqueColumnKey]; ok {
				m[uniqueColumnKey] = RelationManyToOne
			} else {
				mt[nonUniqueColumnKey] = map[string]Relation{uniqueColumnKey: RelationManyToOne}
			}
		} else {
			da.RelationMap[nonUniqueColumnTable] = map[string]map[string]Relation{
				nonUniqueColumnKey: {uniqueColumnKey: RelationManyToOne},
			}
		}
	}
	return
}

func (da *DataAnalyzer) GetOneToOne(uniqueCols iop.Columns, asString bool) (err error) {
	stringType := da.Conn.Template().Function["string_type"]
	uniqueExpressions := lo.Map(uniqueCols, func(col iop.Column, i int) string {
		// integer template, matches only the max value on both sides
		template := `select * from (select '{col_key}' as unique_column_key_1, max({field}) as val from {schema}.{table} where {field} is not null limit 1) t`
		// if asString {
		// 	// string template, matches any value
		// 	template = `select * from (select '{col_key}' as unique_column_key_1, {field} as val from {schema}.{table} where {field} is not null limit 1) t`
		// }
		return g.R(
			template,
			"col_key", col.Key(),
			"field", lo.Ternary(asString, g.F("cast(%s as %s)", da.Conn.Quote(col.Name), stringType), da.Conn.Quote(col.Name)),
			"string_type", stringType,
			"schema", da.Conn.Quote(col.Schema),
			"table", da.Conn.Quote(col.Table),
		)
	})
	nonUniqueSQL := strings.Join(uniqueExpressions, " union all\n    ")

	// get 1-1
	matchingSQLs := lo.Map(uniqueCols, func(col iop.Column, i int) string {
		// integer template, matches only the max value on both sides
		template := `select uv.unique_column_key_1,	'{col_key}' as unique_column_key_2	from unique_values uv
				where uv.val = ( select max({field}) from {schema}.{table} )`
		if asString {
			// string template, matches any value
			template = `select uv.unique_column_key_1,	'{col_key}' as unique_column_key_2	from unique_values uv
				inner join {schema}.{table} t on {field} = uv.val`
		}
		return g.R(
			template,
			"col_key", col.Key(),
			"field", lo.Ternary(asString, g.F("cast(%s as %s)", da.Conn.Quote(col.Name), stringType), da.Conn.Quote(col.Name)),
			"string_type", stringType,
			"schema", da.Conn.Quote(col.Schema),
			"table", da.Conn.Quote(col.Table),
		)
	})
	matchingSQL := strings.Join(matchingSQLs, "    union all\n    ")

	// run
	sql := g.R(`with unique_values as (
				{non_unique_sql}
			)
			, matching as (
				{matching_sql}
			)
			select unique_column_key_1, unique_column_key_2
			from matching
			where unique_column_key_2 != unique_column_key_1
			order by unique_column_key_1
			`,
		"non_unique_sql", nonUniqueSQL,
		"matching_sql", matchingSQL,
	)
	data, err := da.Conn.Query(sql)
	if err != nil {
		return g.Error(err, "could not get matching columns")
	}

	for _, rec := range data.Records() {
		uniqueColumnKey1 := cast.ToString(rec["unique_column_key_1"])
		uniqueColumnKey2 := cast.ToString(rec["unique_column_key_2"])

		uniqueColumn1 := da.ColumnMap[uniqueColumnKey1]
		uniqueColumn2 := da.ColumnMap[uniqueColumnKey2]

		table1 := g.F("%s.%s", uniqueColumn1.Schema, uniqueColumn1.Table)
		table2 := g.F("%s.%s", uniqueColumn2.Schema, uniqueColumn2.Table)

		// one to one
		if mt, ok := da.RelationMap[table1]; ok {
			if m, ok := mt[uniqueColumnKey1]; ok {
				m[uniqueColumnKey2] = RelationOneToOne
			} else {
				mt[uniqueColumnKey1] = map[string]Relation{uniqueColumnKey2: RelationOneToOne}
			}
		} else {
			da.RelationMap[table1] = map[string]map[string]Relation{
				uniqueColumnKey1: {uniqueColumnKey2: RelationOneToOne},
			}
		}

		// one to one
		if mt, ok := da.RelationMap[table2]; ok {
			if m, ok := mt[uniqueColumnKey2]; ok {
				m[uniqueColumnKey1] = RelationOneToOne
			} else {
				mt[uniqueColumnKey2] = map[string]Relation{uniqueColumnKey1: RelationOneToOne}
			}
		} else {
			da.RelationMap[table2] = map[string]map[string]Relation{
				uniqueColumnKey2: {uniqueColumnKey1: RelationOneToOne},
			}
		}
	}
	return
}

func (da *DataAnalyzer) GetManyToMany(nonUniqueCols iop.Columns, asString bool) (err error) {
	return nil
}
