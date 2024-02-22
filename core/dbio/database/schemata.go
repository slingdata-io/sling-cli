package database

import (
	"strings"
	"unicode"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

// Table represents a schemata table
type Table struct {
	Name     string      `json:"name"`
	Schema   string      `json:"schema"`
	Database string      `json:"database,omitempty"`
	IsView   bool        `json:"is_view,omitempty"` // whether is a view
	SQL      string      `json:"sql,omitempty"`
	DDL      string      `json:"ddl,omitempty"`
	Dialect  dbio.Type   `json:"dialect,omitempty"`
	Columns  iop.Columns `json:"columns,omitempty"`
	Keys     TableKeys   `json:"keys,omitempty"`

	Raw string `json:"raw"`
}

func (t *Table) IsQuery() bool {
	return t.SQL != ""
}

func (t *Table) FullName() string {
	q := GetQualifierQuote(t.Dialect)

	fdqnArr := []string{}
	if t.Schema != "" {
		fdqnArr = append(fdqnArr, q+t.Schema+q)
	}
	if t.Name != "" {
		if t.Name == "*" {
			fdqnArr = append(fdqnArr, t.Name)
		} else {
			fdqnArr = append(fdqnArr, q+t.Name+q)
		}
	}
	return strings.Join(fdqnArr, ".")
}

func (t *Table) DatabaseQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Database + q
}

func (t *Table) SchemaQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Schema + q
}

func (t *Table) NameQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Name + q
}

func (t *Table) FDQN() string {
	q := GetQualifierQuote(t.Dialect)

	fdqnArr := []string{}
	if t.Database != "" {
		fdqnArr = append(fdqnArr, q+t.Database+q)
	}
	if t.Schema != "" {
		fdqnArr = append(fdqnArr, q+t.Schema+q)
	}
	if t.Name != "" {
		if t.Name == "*" {
			fdqnArr = append(fdqnArr, t.Name)
		} else {
			fdqnArr = append(fdqnArr, q+t.Name+q)
		}
	}
	return strings.Join(fdqnArr, ".")
}

func (t *Table) ColumnsMap() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, column := range t.Columns {
		key := strings.ToLower(column.Name)
		columns[key] = column
	}
	return columns
}

func (t *Table) Select(fields ...string) string {
	fields = lo.Map(fields, func(f string, i int) string {
		q := GetQualifierQuote(t.Dialect)
		f = strings.TrimSpace(f)
		return q + strings.ReplaceAll(f, q, "") + q
	})

	if t.IsQuery() {
		if len(fields) > 0 {
			fieldsStr := strings.Join(fields, ", ")
			return g.F("select %s from (\n%s\n) t", fieldsStr, t.SQL)
		}
		return t.SQL
	}

	fieldsStr := "*"
	if len(fields) > 0 {
		fieldsStr = strings.Join(fields, ", ")
	}
	return g.F("select %s from %s", fieldsStr, t.FDQN())
}

type TableKeys map[iop.KeyType][]string

// Database represents a schemata database
type Database struct {
	Name    string            `json:"name"`
	Schemas map[string]Schema `json:"schemas"`
}

func (db *Database) Tables() map[string]Table {
	tables := map[string]Table{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			key := strings.ToLower(g.F("%s.%s", schema.Name, table.Name))
			tables[key] = table
		}
	}
	return tables
}

func (db *Database) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			for _, column := range table.Columns {
				key := strings.ToLower(g.F("%s.%s.%s", schema.Name, table.Name, column.Name))
				columns[key] = column
			}
		}
	}
	return columns
}

// Schema represents a schemata schema
type Schema struct {
	Name   string           `json:"name"`
	Tables map[string]Table `json:"tables"`
}

func (schema *Schema) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			key := strings.ToLower(g.F("%s.%s", table.Name, column.Name))
			columns[key] = column
		}
	}
	return columns
}

// ToData converts schema objects to tabular format
func (schema *Schema) ToData() (data iop.Dataset) {
	columns := []string{"schema_name", "table_name", "is_view", "column_id", "column_name", "column_type"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.DbType}
			data.Rows = append(data.Rows, row)
		}
	}
	return
}

// Schemata contains the full schema for a connection
type Schemata struct {
	Databases map[string]Database `json:"databases"`
	conn      Connection          `json:"-"`
}

// LoadTablesJSON loads from a json string
func (s *Schemata) LoadTablesJSON(payload string) error {
	tables := map[string]Table{}
	err := g.Unmarshal(payload, &tables)
	if err != nil {
		return g.Error(err, "could not unmarshal TablesJSON")
	}

	// reconstruct data
	databases := map[string]Database{}
	for key, table := range tables {
		keyArr := strings.Split(key, ".")
		if len(keyArr) != 3 {
			return g.Error("table key must be formatted as `database.schema.table`, got `%s`", key)
		}

		databaseName := strings.ToLower(keyArr[0])
		schemaName := strings.ToLower(keyArr[1])
		tableName := strings.ToLower(keyArr[2])

		if _, ok := databases[databaseName]; !ok {
			databases[databaseName] = Database{
				Name:    table.Database,
				Schemas: map[string]Schema{},
			}
		}
		database := databases[databaseName]

		if _, ok := database.Schemas[schemaName]; !ok {
			database.Schemas[schemaName] = Schema{
				Name:   table.Schema,
				Tables: map[string]Table{},
			}
		}
		schema := database.Schemas[schemaName]

		// fill in positions
		for i := range table.Columns {
			table.Columns[i].Position = i + 1
		}

		// store data
		schema.Tables[tableName] = table
		database.Schemas[schemaName] = schema
		databases[databaseName] = database
	}

	s.Databases = databases

	return nil
}

// Database returns the first encountered database
func (s *Schemata) Database() Database {
	for _, db := range s.Databases {
		return db
	}
	return Database{}
}

func (s *Schemata) Tables() map[string]Table {
	tables := map[string]Table{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				key := strings.ToLower(g.F("%s.%s.%s", db.Name, schema.Name, table.Name))
				tables[key] = table
			}
		}
	}
	return tables
}

func (s *Schemata) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					// get general type
					column.Type = NativeTypeToGeneral(column.Name, column.DbType, s.conn)
					column.SetLengthPrecisionScale()
					key := strings.ToLower(g.F("%s.%s.%s.%s", db.Name, schema.Name, table.Name, column.Name))
					columns[key] = column
				}
			}
		}
	}
	return columns
}

type ColumnType struct {
	Name             string
	DatabaseTypeName string
	Length           int
	Precision        int
	Scale            int
	Nullable         bool
	Sourced          bool
}

func ParseTableName(text string, dialect dbio.Type) (table Table, err error) {
	table.Dialect = dialect
	table.Raw = text

	textLower := strings.ToLower(text)
	if strings.Contains(textLower, "select") && strings.Contains(textLower, "from") && (strings.Contains(text, " ") || strings.Contains(text, "\n")) {
		table.SQL = strings.TrimSpace(text)
		return
	}

	quote := GetQualifierQuote(dialect)

	inQuote := false
	words := []string{}
	word := ""

	addWord := func(caseAsIs bool) {
		if word == "" {
			return
		}

		var hasLower, hasUpper, hasSpecial bool

		for _, r := range word {
			if unicode.IsUpper(r) {
				hasUpper = true
			} else if unicode.IsLower(r) {
				hasLower = true
			}
		}

		defCaseAsIs := (hasLower && hasUpper) || hasSpecial
		if caseAsIs || defCaseAsIs {
		} else {
			word = lo.Ternary(dialect.DBNameUpperCase(), strings.ToUpper(word), strings.ToLower(word))
		}
		words = append(words, word)
		word = ""
	}

	for _, r := range text {
		c := string(r)

		switch c {
		case quote:
			if inQuote {
				addWord(true)
			}
			inQuote = !inQuote
			continue
		case ".":
			if !inQuote {
				addWord(false)
				continue
			}
		case " ", "\n", "\t", "\r", "(", ")", "'":
			if !inQuote {
				table.SQL = strings.TrimSpace(text)
				return
			}
		}

		word = word + c
	}

	if inQuote {
		return table, g.Error("unterminated qualifier quote")
	} else if word != "" {
		addWord(false)
	}

	if len(words) == 0 {
		err = g.Error("invalid table name")
		return
	} else if len(words) == 1 {
		table.Name = words[0]
	} else if len(words) == 2 {
		table.Schema = words[0]
		table.Name = words[1]
	} else if len(words) == 3 {
		table.Database = words[0]
		table.Schema = words[1]
		table.Name = words[2]
	} else {
		table.SQL = strings.TrimSpace(text)
	}

	return
}

func ParseColumnName(text string, dialect dbio.Type) (colName string, err error) {

	quote := GetQualifierQuote(dialect)

	inQuote := false
	words := []string{}
	word := ""

	addWord := func(caseAsIs bool) {
		if word == "" {
			return
		}

		var hasLower, hasUpper, hasSpecial bool

		for _, r := range word {
			if unicode.IsUpper(r) {
				hasUpper = true
			} else if unicode.IsLower(r) {
				hasLower = true
			}
		}

		defCaseAsIs := (hasLower && hasUpper) || hasSpecial
		if caseAsIs || defCaseAsIs {
		} else {
			word = lo.Ternary(dialect.DBNameUpperCase(), strings.ToUpper(word), strings.ToLower(word))
		}
		words = append(words, word)
		word = ""
	}

	for _, r := range text {
		c := string(r)

		switch c {
		case quote:
			if inQuote {
				addWord(true)
			}
			inQuote = !inQuote
			continue
		case ".":
			if !inQuote {
				addWord(false)
				continue
			}
		case " ", "\n", "\t", "\r", "(", ")", "'":
			if !inQuote {
				err = g.Error("invalid character: %#v", c)
				return
			}
		}

		word = word + c
	}

	if inQuote {
		return colName, g.Error("unterminated qualifier quote")
	} else if word != "" {
		addWord(false)
	}

	if len(words) == 0 {
		err = g.Error("invalid column name")
	} else {
		colName = words[len(words)-1]
	}

	return
}

func GetQualifierQuote(dialect dbio.Type) string {
	quote := `"`
	switch dialect {
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks, dbio.TypeDbBigQuery, dbio.TypeDbClickhouse:
		quote = "`"
	case dbio.TypeDbBigTable:
		quote = ""
	}
	return quote
}

// GetTablesSchemata obtains the schemata for specified tables
func GetTablesSchemata(conn Connection, tableNames ...string) (schemata Schemata, err error) {
	schemata = Schemata{Databases: map[string]Database{}}

	schemaTableGroups := map[string][]Table{}
	for _, tableName := range tableNames {
		table, err := ParseTableName(tableName, conn.GetType())
		if err != nil {
			return schemata, g.Error(err, "could not parse table: %s", tableName)
		}
		if arr, ok := schemaTableGroups[table.Schema]; ok {
			schemaTableGroups[table.Schema] = append(arr, table)
		} else {
			schemaTableGroups[table.Schema] = []Table{table}
		}
	}

	getSchemata := func(schema string, tables []Table) {
		defer conn.Context().Wg.Read.Done()

		// pull down schemata
		names := lo.Map(tables, func(t Table, i int) string { return t.Name })
		newSchemata, err := conn.GetSchemata(schema, names...)
		if err != nil {
			g.Warn("could not obtain schemata for schema: %s. %s", schema, err)
			return
		}

		// merge all schematas
		database := schemata.Database()
		schemas := database.Schemas
		if len(schemas) == 0 {
			schemas = map[string]Schema{}
		}

		for name, schema := range newSchemata.Database().Schemas {
			g.Debug(
				"   collected %d columns, in %d tables/views from schema %s",
				len(schema.Columns()),
				len(schema.Tables),
				schema.Name,
			)
			schemas[name] = schema
		}
		database.Schemas = schemas
		schemata.Databases[strings.ToLower(schemata.Database().Name)] = database
	}

	// loop an connect to each
	for schema, tables := range schemaTableGroups {
		conn.Context().Wg.Read.Add()
		go getSchemata(schema, tables)
	}

	conn.Context().Wg.Read.Wait()

	return schemata, nil
}

// GetSchemataAll obtains the schemata for all databases detected
func GetSchemataAll(conn Connection) (schemata Schemata, err error) {
	schemata = Schemata{Databases: map[string]Database{}}

	connInfo := conn.Info()

	// get all databases
	data, err := conn.GetDatabases()
	if err != nil {
		err = g.Error(err, "could not obtain list of databases")
		return
	}
	dbNames := data.ColValuesStr(0)

	getSchemata := func(dbName string) {
		defer conn.Context().Wg.Read.Done()

		// create new connection for database
		g.Debug("getting schemata for database: %s", dbName)

		// remove schema if specified
		connInfo.URL.PopParam("schema")

		// replace database with new one
		connURL := strings.ReplaceAll(
			connInfo.URL.String(),
			"/"+connInfo.Database,
			"/"+dbName,
		)

		newConn, err := NewConn(connURL)
		if err != nil {
			g.Warn("could not connect using database %s. %s", dbName, err)
			return
		}

		// pull down schemata
		newSchemata, err := newConn.GetSchemata("", "")
		if err != nil {
			g.Warn("could not obtain schemata for database: %s. %s", dbName, err)
			return
		}

		// merge all schematas
		for name, database := range newSchemata.Databases {
			g.Debug(
				"   collected %d columns, in %d tables/views from database %s",
				len(database.Columns()),
				len(database.Tables()),
				database.Name,
			)
			schemata.Databases[name] = database
		}
	}

	// loop an connect to each
	for _, dbName := range dbNames {
		conn.Context().Wg.Read.Add()
		go getSchemata(dbName)
	}

	conn.Context().Wg.Read.Wait()

	return schemata, nil
}

func HasVariedCase(text string) bool {
	hasUpper := false
	hasLower := false
	for _, c := range text {
		if unicode.IsUpper(c) {
			hasUpper = true
		}
		if unicode.IsLower(c) {
			hasLower = true
		}
		if hasUpper && hasLower {
			break
		}
	}

	return hasUpper && hasLower
}
