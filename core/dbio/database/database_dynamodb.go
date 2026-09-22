package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// DynamoDB has no SQL engine: this connector maps sling's operations onto the
// DynamoDB API.
//
//   - reading is a `Scan` (paginated with ExclusiveStartKey). The rendered
//     `select` is a JSON descriptor: {"filter": {...}, "fields": [...], "limit": n}.
//     Filters accept {"col": value} for equality and {"col": {"$gt": value, ...}}
//     for comparisons ($eq, $ne, $lt, $lte, $gt, $gte, $between, $in,
//     $begins_with, $contains, $exists).
//   - writing is `BatchWriteItem` with `PutRequest`, which upserts by primary
//     key and is idempotent per key.
//   - tables are key-value collections: a partition key is required, an
//     optional sort key may follow, and every other attribute is schemaless.
//     The primary key is read off the generated `create table` DDL, so set
//     `primary_key` (or let sling pick a column named `id`).
//
// Only key attributes are typed in DynamoDB (S, N or B). Values are stored as:
// text as S, numbers as N, booleans as BOOL, binary as B, JSON as M/L, and
// timestamps/dates as ISO-8601 S strings (DynamoDB has no date type).
type DynamoDBConn struct {
	BaseConn
	URL      string
	Client   *dynamodb.Client
	Region   string
	Endpoint string

	syntheticKeySeq uint64 // sequence for values of an added key column
}

const dynamoDBBatchSize = 25 // BatchWriteItem allows at most 25 requests
const dynamoDBSampleSize = 25

// dynamoDBSyntheticKeyPrefix names the key column that is added when a stream
// declares no primary key: DynamoDB has no key-less tables, and keying on a
// data column would collapse rows whenever its values repeat (DynamoDB upserts
// on the key). This mirrors the other key-value connectors (MongoDB's `_id`,
// Azure Tables' RowKey).
const dynamoDBSyntheticKeyPrefix = "_sling_id"

// isDynamoDBSyntheticKey reports whether the key column was added by sling
func isDynamoDBSyntheticKey(name string) bool {
	return strings.HasPrefix(name, dynamoDBSyntheticKeyPrefix)
}

// dynamoDBSyntheticKeyName returns a key column name that the data does not use
func dynamoDBSyntheticKeyName(columns iop.Columns) (name string) {
	name = dynamoDBSyntheticKeyPrefix
	for i := 2; g.In(name, columns.Names()...); i++ {
		name = g.F("%s%d", dynamoDBSyntheticKeyPrefix, i)
	}
	return name
}

// dynamoDBSelectRegex matches the only SQL shape DynamoDB can honor: a field
// list and a table (the connector has no SQL engine)
var dynamoDBSelectRegex = regexp.MustCompile(`(?is)^select\s+(.+?)\s+from\s+([^\s,;()]+)`)

// dynamoDBSelectLimitRegex matches a trailing `limit n`
var dynamoDBSelectLimitRegex = regexp.MustCompile(`(?is)\blimit\s+(\d+)\s*;?\s*$`)

// dynamoDBSetRegex and dynamoDBWhereRegex locate the clauses of the statements
// sling renders for table lifecycle and deletes
var dynamoDBSetRegex = regexp.MustCompile(`(?is)\sset\s`)
var dynamoDBWhereRegex = regexp.MustCompile(`(?is)\swhere\s`)

// dynamoDBClauseRegex matches SQL clauses DynamoDB cannot apply
var dynamoDBClauseRegex = regexp.MustCompile(`(?is)\b(where|group\s+by|order\s+by|having|join|offset|union)\b`)

// dynamoDBTableNameRegex matches a DynamoDB table name (3-255 chars of a-z, A-Z,
// 0-9, `_`, `-` and `.`), optionally quoted
var dynamoDBTableNameRegex = regexp.MustCompile("^[`\"']?[A-Za-z0-9_.\\-]+[`\"']?$")

// dynamoDBIsTableName reports whether the text can be a table name (as opposed
// to a rendered condition or a statement)
func dynamoDBIsTableName(text string) bool {
	return dynamoDBTableNameRegex.MatchString(strings.TrimSpace(text))
}

// dynamoDBUnquote strips the identifier quoting a statement may carry
func dynamoDBUnquote(name string) string {
	name = strings.TrimSpace(name)
	if len(name) >= 2 {
		if (name[0] == '"' && name[len(name)-1] == '"') ||
			(name[0] == '`' && name[len(name)-1] == '`') ||
			(name[0] == '[' && name[len(name)-1] == ']') ||
			(name[0] == '\'' && name[len(name)-1] == '\'') {
			return name[1 : len(name)-1]
		}
	}
	return name
}

// dynamoDBScanRef resolves the table reference handed to the connector. sling
// renders a `select` into a JSON scan descriptor ({"table": ..., "filter": ...}),
// so accept that, a `select ... from <table> [limit n]` statement, or a plain
// table name.
func dynamoDBScanRef(ref string) (tableName string, descriptor map[string]any, err error) {
	ref = strings.TrimSpace(ref)

	if ref == "" {
		return "", nil, g.Error("no table specified for DynamoDB")
	}

	if strings.HasPrefix(ref, "{") {
		// sling appends markers to the rendered select (e.g. `/* nD */`), so read
		// the leading JSON value instead of requiring the whole text to be JSON
		if err = json.NewDecoder(strings.NewReader(ref)).Decode(&descriptor); err != nil {
			return "", nil, g.Error(err, "could not parse scan descriptor: %s", ref)
		}
		tableName = cast.ToString(descriptor["table"])
		if tableName == "" {
			return "", nil, g.Error("scan descriptor is missing the table: %s", ref)
		}
		return tableName, descriptor, nil
	}

	if !dynamoDBSelectRegex.MatchString(ref) {
		return ref, nil, nil
	}

	// anything else would be silently dropped, which would return wrong rows
	if clause := dynamoDBClauseRegex.FindString(ref); clause != "" {
		return "", nil, g.Error(
			"DynamoDB has no SQL engine: `%s` cannot be applied here. Use the `where` stream option with a filter expression",
			strings.ToUpper(strings.Join(strings.Fields(clause), " ")),
		)
	}

	matches := dynamoDBSelectRegex.FindStringSubmatch(ref)
	tableName = dynamoDBUnquote(matches[2])
	descriptor = map[string]any{"table": tableName}

	if fields := strings.TrimSpace(matches[1]); fields != "*" {
		descriptor["fields"] = lo.Map(strings.Split(fields, ","), func(field string, _ int) string {
			return dynamoDBUnquote(field)
		})
	}

	if limit := dynamoDBSelectLimitRegex.FindStringSubmatch(ref); limit != nil {
		descriptor["limit"] = cast.ToInt(limit[1])
	}

	return tableName, descriptor, nil
}

// mergeDynamoDBScanOptions merges a scan descriptor over the caller's options:
// the descriptor is the rendered statement, so it wins (a `limit` inside the
// statement is more specific than a default), while caller-only options such as
// `columns` are kept
func mergeDynamoDBScanOptions(opts map[string]any, descriptor map[string]any) map[string]any {
	merged := map[string]any{}
	for key, val := range opts {
		merged[key] = val
	}
	for key, val := range descriptor {
		merged[key] = val
	}
	delete(merged, "table")
	return merged
}

// dynamoDBResult implements sql.Result (DynamoDB has no row counts)
type dynamoDBResult struct {
	rowsAffected int64
}

func (r *dynamoDBResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *dynamoDBResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Init initiates the object
func (conn *DynamoDBConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDynamoDB

	// rows are written straight to the table (no SQL temp table)
	conn.BaseConn.SetProp("use_bulk", "true")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Connect connects to the database
func (conn *DynamoDBConn) Connect(timeOut ...int) (err error) {
	ctx := conn.Context().Ctx
	if len(timeOut) > 0 && timeOut[0] > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeOut[0])*time.Second)
		defer cancel()
	}

	conn.Region = conn.GetProp("aws_region", "region")
	conn.Endpoint = conn.GetProp("aws_endpoint", "endpoint")

	props := conn.Props()
	if conn.Endpoint != "" && conn.GetProp("aws_access_key_id", "access_key_id") == "" {
		// local / non-AWS endpoints (DynamoDB Local) ignore credentials, but the
		// SDK still needs a signer, so fall back to placeholder values
		props["aws_access_key_id"] = "local"
		props["aws_secret_access_key"] = "local"
	}

	cfg, err := iop.MakeAwsConfig(ctx, props)
	if err != nil {
		return g.Error(err, "could not create AWS config")
	}
	conn.Client = dynamodb.NewFromConfig(cfg)

	if _, err = conn.ListTables(ctx); err != nil {
		return g.Error(err, "could not list DynamoDB tables")
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

// Close closes the connection
// ensureClient connects on demand. sling hands out fresh connection objects for
// metadata calls (counts, discovery, checksums) that are not connected yet, and
// reuses a connection object after Close(), so every entry point that talks to
// the API must be able to (re)build the client.
func (conn *DynamoDBConn) ensureClient() (err error) {
	if conn.Client != nil {
		return nil
	}
	return conn.Connect()
}

func (conn *DynamoDBConn) Close() error {
	conn.Client = nil
	g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	return nil
}

// NewTransaction creates a new transaction (unsupported in DynamoDB)
func (conn *DynamoDBConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	return nil, g.Error("transactions not supported in DynamoDB")
}

// ListTables returns the table names
func (conn *DynamoDBConn) ListTables(ctx context.Context) (names []string, err error) {
	if err = conn.ensureClient(); err != nil {
		return nil, err
	}
	var start *string
	for {
		out, err := conn.Client.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: start})
		if err != nil {
			return nil, g.Error(err, "could not list DynamoDB tables")
		}
		names = append(names, out.TableNames...)
		if out.LastEvaluatedTableName == nil || *out.LastEvaluatedTableName == "" {
			break
		}
		start = out.LastEvaluatedTableName
	}
	return names, nil
}

// describeTable returns the table description, or found=false when it does not exist
func (conn *DynamoDBConn) describeTable(ctx context.Context, tableName string) (table *ddbtypes.TableDescription, found bool, err error) {
	if err = conn.ensureClient(); err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, false, g.Error("did not provide a table name")
	}

	out, err := conn.Client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	if err != nil {
		if isDynamoDBNotFound(err) {
			return nil, false, nil
		}
		return nil, false, g.Error(err, "could not describe table %s", tableName)
	} else if out.Table == nil {
		return nil, false, g.Error("could not describe table %s", tableName)
	}
	return out.Table, true, nil
}

// isDynamoDBNotFound returns true when the error is a ResourceNotFoundException
func isDynamoDBNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ResourceNotFoundException")
}

// isDynamoDBAlreadyExists returns true when the table already exists (or is being deleted)
func isDynamoDBAlreadyExists(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "ResourceInUseException") ||
		strings.Contains(err.Error(), "TableAlreadyExistsException"))
}

// TableExists checks if a table exists
func (conn *DynamoDBConn) TableExists(table Table) (exists bool, err error) {
	desc, found, err := conn.describeTable(conn.Context().Ctx, table.Name)
	if err != nil || !found {
		return false, err
	}

	// a table being deleted cannot be re-created until DynamoDB is done with it,
	// so wait it out and report it as gone
	if desc.TableStatus == ddbtypes.TableStatusDeleting {
		err = conn.waitForTableAbsent(conn.Context().Ctx, table.Name)
		return false, err
	}

	return true, nil
}

// waitForTableActive waits until the table is ready to serve reads and writes
func (conn *DynamoDBConn) waitForTableActive(ctx context.Context, tableName string) (err error) {
	timeOut := cast.ToInt(conn.GetProp("table_timeout"))
	if timeOut == 0 {
		timeOut = 60
	}

	start := time.Now()
	for {
		desc, found, err := conn.describeTable(ctx, tableName)
		if err != nil {
			return err
		} else if !found {
			return g.Error("table %s does not exist anymore", tableName)
		} else if desc.TableStatus == ddbtypes.TableStatusActive {
			return nil
		}

		if time.Since(start) > time.Duration(timeOut)*time.Second {
			return g.Error("table %s is not active after %d seconds (status: %s)", tableName, timeOut, desc.TableStatus)
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// waitForTableAbsent waits until the table is fully deleted
func (conn *DynamoDBConn) waitForTableAbsent(ctx context.Context, tableName string) (err error) {
	timeOut := cast.ToInt(conn.GetProp("table_timeout"))
	if timeOut == 0 {
		timeOut = 60
	}

	start := time.Now()
	for {
		_, found, err := conn.describeTable(ctx, tableName)
		if err != nil {
			return err
		} else if !found {
			return nil
		}

		if time.Since(start) > time.Duration(timeOut)*time.Second {
			return g.Error("table %s is still not deleted after %d seconds", tableName, timeOut)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// dropTable deletes a table and waits for the deletion to complete
func (conn *DynamoDBConn) dropTable(ctx context.Context, tableName string) (err error) {
	if err = conn.ensureClient(); err != nil {
		return err
	}
	_, err = conn.Client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	if err != nil {
		if isDynamoDBNotFound(err) {
			return nil // already gone
		}
		return g.Error(err, "could not delete table %s", tableName)
	}
	return conn.waitForTableAbsent(ctx, tableName)
}

// GenerateDDL generates the DDL that ExecContext parses to create the table.
// DynamoDB requires a primary key: the target's `table_keys`, sling's
// `primary_key`, or a suitable column supplies it (see dynamoDBKeyColumns).
func (conn *DynamoDBConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	if pkCols := conn.dynamoDBKeyColumns(table, data.Columns); len(pkCols) > 0 {
		if err = data.Columns.SetKeys(iop.PrimaryKey, pkCols...); err != nil {
			return "", g.Error(err)
		}
	} else {
		// no key declared: add one so every row survives, instead of keying on a
		// data column that may repeat values
		keyCol := iop.Column{
			Name:     dynamoDBSyntheticKeyName(data.Columns),
			Type:     iop.StringType,
			Position: len(data.Columns) + 1,
		}
		keyCol.SetMetadata(string(iop.PrimaryKey.MetadataKey()), "true")
		data.Columns = append(data.Columns, keyCol)
		g.Warn("no primary key specified for DynamoDB; adding column %s as the key", keyCol.Name)
	}

	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	return strings.TrimSpace(ddl), nil
}

// dynamoDBKeyColumns resolves the table's key columns: explicit target keys
// first, then the source primary key. sling records `primary_key` as column
// metadata instead of setting the key type on target columns, but DynamoDB has
// no key-less tables and the key is the upsert identity, so it is honored here.
func (conn *DynamoDBConn) dynamoDBKeyColumns(table Table, columns iop.Columns) (pkCols []string) {
	if keys := table.Keys[iop.PrimaryKey]; len(keys) > 0 {
		return keys
	} else if keys := table.Keys[iop.UniqueKey]; len(keys) > 0 && len(keys) <= 2 {
		// a declared unique key is the natural upsert identity in a key-value
		// store (DynamoDB keys serve as its unique constraints)
		return keys
	} else if keys := columns.GetKeys(iop.PrimaryKey); len(keys) > 0 {
		return keys.Names()
	}

	for _, col := range columns {
		if strings.EqualFold(col.Metadata[iop.PrimaryKey.MetadataKey()], "source") {
			pkCols = append(pkCols, col.Name)
		}
	}
	return pkCols
}

// ExecContext executes a DDL statement against DynamoDB. Only the statements
// sling emits for table lifecycle are supported; there is no SQL engine.
func (conn *DynamoDBConn) ExecContext(ctx context.Context, sqlText string, args ...any) (result sql.Result, err error) {
	text := strings.TrimSpace(sqlText)
	if !isDynamoDBStatement(text) {
		return nil, g.Error("SQL operation not supported on DynamoDB: %s", sqlText)
	}

	lower := strings.ToLower(text)
	switch {
	case createTableRegex.MatchString(lower):
		err = conn.createTableFromDDL(ctx, text)
	case strings.HasPrefix(lower, "drop table"):
		err = conn.dropTable(ctx, parseDynamoDBTableToken(text, "drop table"))
	case strings.HasPrefix(lower, "truncate table"):
		err = conn.truncateTable(ctx, parseDynamoDBTableToken(text, "truncate table"))
	case strings.HasPrefix(lower, "update "):
		// sling soft-deletes missing rows with a plain update
		err = conn.updateItems(ctx, text)
	default: // `delete from`
		err = conn.deleteItems(ctx, text)
	}

	if err != nil {
		return nil, err
	}
	return &dynamoDBResult{}, nil
}

// isDynamoDBStatement reports whether the text is one of the lifecycle
// statements sling emits, as opposed to a table name or a scan descriptor
func isDynamoDBStatement(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	return createTableRegex.MatchString(lower) ||
		strings.HasPrefix(lower, "drop table") ||
		strings.HasPrefix(lower, "truncate table") ||
		strings.HasPrefix(lower, "update ") ||
		strings.HasPrefix(lower, "delete from")
}

// parseDynamoDBTableToken returns the table name following the given keyword,
// dropping any schema qualifier and quotes.
func parseDynamoDBTableToken(text string, keyword string) (name string) {
	fields := strings.Fields(strings.TrimSpace(text[len(keyword):]))
	if len(fields) == 0 {
		return ""
	}

	if strings.EqualFold(fields[0], "if") && len(fields) > 2 && strings.EqualFold(fields[1], "exists") {
		fields = fields[2:] // `drop table if exists <table>`
	}

	name = strings.TrimRight(fields[0], ";")
	if parts := strings.Split(name, "."); len(parts) > 1 {
		name = parts[len(parts)-1] // drop the schema qualifier
	}

	return strings.Trim(name, "`\"")
}

// dynamoDBTableDef carries the information DynamoDB needs to create a table
type dynamoDBTableDef struct {
	Name       string
	KeyColumns []string
	KeyTypes   map[string]ddbtypes.ScalarAttributeType
}

// parseDynamoDBDDL reads the create statement generated by GenerateDDL
func parseDynamoDBDDL(ddl string) (def dynamoDBTableDef, err error) {
	def = dynamoDBTableDef{KeyTypes: map[string]ddbtypes.ScalarAttributeType{}}

	loc := createTableRegex.FindStringIndex(ddl)
	if loc == nil {
		return def, g.Error("could not find CREATE TABLE in DDL")
	}

	rest := ddl[loc[1]:]
	openParen := strings.Index(rest, "(")
	if openParen == -1 {
		return def, g.Error("could not find column list in DDL")
	}
	def.Name = parseDynamoDBTableToken("x "+strings.TrimSpace(rest[:openParen]), "x")

	// balanced parenthesis scan for the column list
	depth, closeParen := 0, -1
	for i := openParen; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				closeParen = i
			}
		}
		if closeParen != -1 {
			break
		}
	}
	if closeParen == -1 {
		return def, g.Error("could not find closing parenthesis in DDL")
	}

	columnTypes := map[string]ddbtypes.ScalarAttributeType{}
	for _, item := range splitDDLTopLevel(rest[openParen+1 : closeParen]) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if strings.HasPrefix(strings.ToLower(item), "primary key") {
			start, end := strings.Index(item, "("), strings.LastIndex(item, ")")
			if start == -1 || end <= start {
				return def, g.Error("could not parse PRIMARY KEY clause: %s", item)
			}
			for _, name := range strings.Split(item[start+1:end], ",") {
				name = strings.Trim(strings.TrimSpace(name), `"`)
				if name != "" {
					def.KeyColumns = append(def.KeyColumns, name)
				}
			}
			continue
		}

		fields := strings.Fields(item)
		if len(fields) == 0 {
			continue
		}
		colName := strings.Trim(fields[0], `"`)
		colType := ""
		if len(fields) > 1 {
			colType = fields[1]
		}
		columnTypes[colName] = dynamoDBKeyAttributeType(colType)
	}

	if len(def.KeyColumns) == 0 {
		return def, g.Error("DynamoDB requires a primary key: set `primary_key` for the stream")
	} else if len(def.KeyColumns) > 2 {
		return def, g.Error("DynamoDB supports at most 2 key columns (partition key + sort key), got %d: %s",
			len(def.KeyColumns), strings.Join(def.KeyColumns, ", "))
	}

	for _, name := range def.KeyColumns {
		keyType, ok := columnTypes[name]
		if !ok {
			return def, g.Error("key column %s is not part of the table definition", name)
		}
		def.KeyTypes[name] = keyType
	}

	return def, nil
}

// splitDDLTopLevel splits a column list on commas that are not inside parenthesis
func splitDDLTopLevel(text string) (items []string) {
	depth := 0
	start := 0
	for i, r := range text {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				items = append(items, text[start:i])
				start = i + 1
			}
		}
	}
	return append(items, text[start:])
}

// dynamoDBKeyAttributeType maps a DDL type to the DynamoDB key attribute type
func dynamoDBKeyAttributeType(ddlType string) ddbtypes.ScalarAttributeType {
	ddlType = strings.ToLower(strings.Split(strings.TrimSpace(ddlType), "(")[0])
	switch {
	case strings.Contains(ddlType, "int"), strings.Contains(ddlType, "number"),
		strings.Contains(ddlType, "decimal"), strings.Contains(ddlType, "numeric"),
		strings.Contains(ddlType, "float"), strings.Contains(ddlType, "double"),
		strings.Contains(ddlType, "real"):
		return ddbtypes.ScalarAttributeTypeN
	case strings.Contains(ddlType, "binary"), strings.Contains(ddlType, "blob"), strings.Contains(ddlType, "bytea"):
		return ddbtypes.ScalarAttributeTypeB
	}
	return ddbtypes.ScalarAttributeTypeS
}

// createTableFromDDL creates a table from the DDL generated by GenerateDDL
func (conn *DynamoDBConn) createTableFromDDL(ctx context.Context, ddl string) (err error) {
	if err = conn.ensureClient(); err != nil {
		return err
	}
	def, err := parseDynamoDBDDL(ddl)
	if err != nil {
		return err
	}

	keySchema := []ddbtypes.KeySchemaElement{{
		AttributeName: aws.String(def.KeyColumns[0]),
		KeyType:       ddbtypes.KeyTypeHash,
	}}
	if len(def.KeyColumns) == 2 {
		keySchema = append(keySchema, ddbtypes.KeySchemaElement{
			AttributeName: aws.String(def.KeyColumns[1]),
			KeyType:       ddbtypes.KeyTypeRange,
		})
	}

	attrDefs := []ddbtypes.AttributeDefinition{}
	for _, name := range def.KeyColumns {
		attrDefs = append(attrDefs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String(name),
			AttributeType: def.KeyTypes[name],
		})
	}

	conn.LogSQL(g.F("create table %s (%s)", def.Name, strings.Join(def.KeyColumns, ", ")))
	_, err = conn.Client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(def.Name),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		if isDynamoDBAlreadyExists(err) {
			return g.Error(err, "table %s already exists", def.Name)
		}
		return g.Error(err, "could not create table %s", def.Name)
	}

	return conn.waitForTableActive(ctx, def.Name)
}

// truncateTable deletes every item, keeping the table and its key schema
func (conn *DynamoDBConn) truncateTable(ctx context.Context, tableName string) (err error) {
	desc, found, err := conn.describeTable(ctx, tableName)
	if err != nil {
		return err
	} else if !found {
		return nil
	}

	keyNames := []string{}
	projection := []string{}
	names := map[string]string{}
	for i, ks := range desc.KeySchema {
		keyNames = append(keyNames, *ks.AttributeName)
		alias := g.F("#k%d", i)
		names[alias] = *ks.AttributeName
		projection = append(projection, alias)
	}

	var lastKey map[string]ddbtypes.AttributeValue
	for {
		out, err := conn.Client.Scan(ctx, &dynamodb.ScanInput{
			TableName:                aws.String(tableName),
			ProjectionExpression:     aws.String(strings.Join(projection, ", ")),
			ExpressionAttributeNames: names,
			ExclusiveStartKey:        lastKey,
		})
		if err != nil {
			return g.Error(err, "could not scan table %s for truncate", tableName)
		}

		keys := []map[string]ddbtypes.AttributeValue{}
		for _, item := range out.Items {
			key := map[string]ddbtypes.AttributeValue{}
			for _, name := range keyNames {
				if val, ok := item[name]; ok {
					key[name] = val
				}
			}
			if len(key) == len(keyNames) {
				keys = append(keys, key)
			}
		}

		if err = conn.batchWriteRequests(ctx, tableName, deleteRequests(keys)); err != nil {
			return g.Error(err, "could not delete items from table %s", tableName)
		}

		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
		}
	}

	return nil
}

// GetSchemas returns schemas (DynamoDB has no schema, `default` is used)
func (conn *DynamoDBConn) GetSchemas() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]any{"default"})
	return data, nil
}

// GetTables returns the list of tables
func (conn *DynamoDBConn) GetTables(schema string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))

	names, err := conn.ListTables(conn.Context().Ctx)
	if err != nil {
		return data, err
	}

	for _, name := range names {
		data.Append([]any{name})
	}

	return data, nil
}

// dynamoDBMatchTableNames reports whether a table matches a discover pattern:
// an exact name, or a name carrying the `*`/`?` wildcards sling passes through
func dynamoDBMatchTableNames(tableName string, patterns []string) bool {
	for _, pattern := range patterns {
		if g.In(tableName, pattern) {
			return true
		}
		if strings.ContainsAny(pattern, "*?[") {
			if matched, err := path.Match(pattern, tableName); err == nil && matched {
				return true
			}
		}
	}
	return false
}

// GetSchemata obtain full schemata info for a schema and/or table
func (conn *DynamoDBConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	database := Database{
		Name:    "dynamodb",
		Schemas: map[string]Schema{},
	}

	schema := Schema{
		Name:     "default",
		Database: database.Name,
		Tables:   map[string]Table{},
	}

	tablesData, err := conn.GetTables(schema.Name)
	if err != nil {
		return schemata, g.Error(err, "Could not get tables")
	}

	// discover passes an empty table name when the pattern only names a schema
	tableNames = lo.Filter(tableNames, func(name string, _ int) bool {
		return strings.TrimSpace(name) != ""
	})

	for _, tableRow := range tablesData.Rows {
		tableName := cast.ToString(tableRow[0])
		if len(tableNames) > 0 && !dynamoDBMatchTableNames(tableName, tableNames) {
			continue
		}

		table := Table{
			Name:     tableName,
			Schema:   schema.Name,
			Database: database.Name,
			IsView:   false,
			Dialect:  conn.GetType(),
		}

		if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
			if level == SchemataLevelColumn {
				columns, err := conn.GetTableColumns(&table)
				if err != nil {
					g.Warn("could not get columns for table %s: %s", tableName, err)
				} else {
					table.Columns = columns
				}
			}
			schema.Tables[strings.ToLower(tableName)] = table
		}
	}

	database.Schemas[strings.ToLower(schema.Name)] = schema
	schemata.Databases[strings.ToLower(database.Name)] = database

	return schemata, nil
}

// GetTableColumns returns the columns of a table: the key attributes (the only
// typed columns) followed by the attributes found in a sample of items.
// A table that does not exist yields no columns and no error, like an empty
// SQL result set.
func (conn *DynamoDBConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	ctx := conn.Context().Ctx
	desc, found, err := conn.describeTable(ctx, table.Name)
	if err != nil {
		return nil, err
	} else if !found {
		return iop.Columns{}, nil
	}

	attrTypes := map[string]ddbtypes.ScalarAttributeType{}
	for _, attrDef := range desc.AttributeDefinitions {
		attrTypes[*attrDef.AttributeName] = attrDef.AttributeType
	}

	seen := map[string]bool{}
	position := 1
	for _, keySchema := range desc.KeySchema {
		name := *keySchema.AttributeName
		col := dynamoDBColumn(name, string(attrTypes[name]), nil)
		col.Table = table.Name
		col.Schema = table.Schema
		col.Position = position
		columns = append(columns, col)
		seen[name] = true
		position++
	}

	items, err := conn.sampleItems(ctx, table.Name, dynamoDBSampleSize)
	if err != nil {
		return columns, err
	}

	for _, item := range items {
		for name, av := range item {
			if seen[name] {
				continue
			}
			col := dynamoDBColumn(name, dynamoDBAttributeType(av), av)
			col.Table = table.Name
			col.Schema = table.Schema
			col.Position = position
			columns = append(columns, col)
			seen[name] = true
			position++
		}
	}

	return columns, nil
}

// sampleItems returns up to count items, so column names and types can be inferred
func (conn *DynamoDBConn) sampleItems(ctx context.Context, tableName string, count int) (items []map[string]ddbtypes.AttributeValue, err error) {
	if err = conn.ensureClient(); err != nil {
		return nil, err
	}
	out, err := conn.Client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(int32(count)),
	})
	if err != nil {
		if isDynamoDBNotFound(err) {
			return nil, nil
		}
		return nil, g.Error(err, "could not sample table %s", tableName)
	}

	if len(out.Items) > count {
		return out.Items[:count], nil
	}
	return out.Items, nil
}

// dynamoDBColumn builds a column from a DynamoDB attribute (value is optional)
func dynamoDBColumn(name string, attrType string, av ddbtypes.AttributeValue) iop.Column {
	colType, dbType := dynamoDBTypeToIop(attrType)
	col := iop.Column{Name: name, Type: colType, DbType: dbType}

	switch attrType {
	case string(ddbtypes.ScalarAttributeTypeN):
		col.Type = dynamoDBNumberType(av)
		col.DbType = "number"
	case string(ddbtypes.ScalarAttributeTypeS):
		if av != nil {
			if member, ok := av.(*ddbtypes.AttributeValueMemberS); ok && isISODateString(member.Value) {
				col.Type = iop.TimestampType
				col.DbType = "timestamp"
			}
		}
	}

	return col
}

// dynamoDBTypeToIop maps a DynamoDB attribute type to a general type
func dynamoDBTypeToIop(attrType string) (colType iop.ColumnType, dbType string) {
	switch attrType {
	case string(ddbtypes.ScalarAttributeTypeS):
		return iop.TextType, "string"
	case string(ddbtypes.ScalarAttributeTypeN):
		return iop.DecimalType, "number"
	case string(ddbtypes.ScalarAttributeTypeB):
		return iop.BinaryType, "binary"
	case "BOOL":
		return iop.BoolType, "bool"
	case "NULL":
		return iop.TextType, "null"
	case "L", "M", "SS", "NS", "BS":
		return iop.JsonType, "json"
	}
	return iop.TextType, "string"
}

// dynamoDBNumberType returns the narrowest general numeric type for a number value
func dynamoDBNumberType(av ddbtypes.AttributeValue) iop.ColumnType {
	if av == nil {
		return iop.DecimalType
	}
	member, ok := av.(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return iop.DecimalType
	}
	if _, err := strconv.ParseInt(member.Value, 10, 64); err == nil {
		return iop.BigIntType
	}
	return iop.DecimalType
}

// dynamoDBAttributeType names a DynamoDB attribute value's type
func dynamoDBAttributeType(av ddbtypes.AttributeValue) string {
	switch av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return "S"
	case *ddbtypes.AttributeValueMemberN:
		return "N"
	case *ddbtypes.AttributeValueMemberB:
		return "B"
	case *ddbtypes.AttributeValueMemberBOOL:
		return "BOOL"
	case *ddbtypes.AttributeValueMemberNULL:
		return "NULL"
	case *ddbtypes.AttributeValueMemberL:
		return "L"
	case *ddbtypes.AttributeValueMemberM:
		return "M"
	case *ddbtypes.AttributeValueMemberSS:
		return "SS"
	case *ddbtypes.AttributeValueMemberNS:
		return "NS"
	case *ddbtypes.AttributeValueMemberBS:
		return "BS"
	}
	return "NULL"
}

// isISODateString returns true for RFC3339 timestamps and ISO dates
func isISODateString(text string) bool {
	if len(text) < 10 {
		return false
	}
	if _, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return true
	}
	_, err := time.Parse("2006-01-02", text)
	return err == nil
}

// GetCount returns the number of items in a table
func (conn *DynamoDBConn) GetCount(tableFName string) (int64, error) {
	if err := conn.ensureClient(); err != nil {
		return 0, err
	}
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	ctx := conn.Context().Ctx
	var count int64
	var lastKey map[string]ddbtypes.AttributeValue
	for {
		out, err := conn.Client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(table.Name),
			Select:            ddbtypes.SelectCount,
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			return 0, g.Error(err, "could not count table %s", table.Name)
		}
		count += cast.ToInt64(out.Count)
		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
		}
	}

	return count, nil
}

// GetMaxValue returns the maximum value of a column. DynamoDB has no
// aggregates, so the attribute is scanned (projected) and reduced client side.
func (conn *DynamoDBConn) GetMaxValue(table Table, colName string) (value any, maxCol iop.Column, err error) {
	if err = conn.ensureClient(); err != nil {
		return nil, iop.Column{}, err
	}
	ctx := conn.Context().Ctx
	maxCol = iop.Column{Name: colName, Table: table.Name, Schema: table.Schema}

	_, found, err := conn.describeTable(ctx, table.Name)
	if err != nil {
		return nil, maxCol, err
	} else if !found {
		return nil, maxCol, nil
	}

	var maxAttr ddbtypes.AttributeValue
	var lastKey map[string]ddbtypes.AttributeValue
	for {
		out, err := conn.Client.Scan(ctx, &dynamodb.ScanInput{
			TableName:                aws.String(table.Name),
			ProjectionExpression:     aws.String("#max_col"),
			ExpressionAttributeNames: map[string]string{"#max_col": colName},
			ExclusiveStartKey:        lastKey,
		})
		if err != nil {
			return nil, maxCol, g.Error(err, "could not get max value of %s", colName)
		}

		for _, item := range out.Items {
			av, ok := item[colName]
			if !ok {
				continue
			}
			if maxAttr == nil || dynamoDBAttributeLess(maxAttr, av) {
				maxAttr = av
			}
		}

		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
		}
	}

	if maxAttr == nil {
		return nil, maxCol, nil
	}

	attrType := dynamoDBAttributeType(maxAttr)
	sampled := dynamoDBColumn(colName, attrType, maxAttr)
	maxCol.Type = sampled.Type
	maxCol.DbType = sampled.DbType

	// return the value as stored, so the incremental filter matches it exactly
	raw, _ := dynamoDBAttributeString(maxAttr)
	return raw, maxCol, nil
}

// dynamoDBAttributeLess compares two attribute values of the same attribute
func dynamoDBAttributeLess(a, b ddbtypes.AttributeValue) bool {
	switch first := a.(type) {
	case *ddbtypes.AttributeValueMemberN:
		second, ok := b.(*ddbtypes.AttributeValueMemberN)
		return ok && cast.ToFloat64(first.Value) < cast.ToFloat64(second.Value)
	case *ddbtypes.AttributeValueMemberB:
		second, ok := b.(*ddbtypes.AttributeValueMemberB)
		return ok && string(first.Value) < string(second.Value)
	}

	firstStr, _ := dynamoDBAttributeString(a)
	secondStr, _ := dynamoDBAttributeString(b)
	return firstStr < secondStr
}

// dynamoDBAttributeString renders an attribute value as text
func dynamoDBAttributeString(av ddbtypes.AttributeValue) (text string, ok bool) {
	switch val := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return val.Value, true
	case *ddbtypes.AttributeValueMemberN:
		return val.Value, true
	case *ddbtypes.AttributeValueMemberB:
		return string(val.Value), true
	case *ddbtypes.AttributeValueMemberBOOL:
		return strconv.FormatBool(val.Value), true
	case *ddbtypes.AttributeValueMemberNULL:
		return "", false
	}

	if text, err := marshalDynamoJSON(av); err == nil {
		return text, true
	}
	return "", false
}

// BulkExportFlow returns a dataflow for the table
func (conn *DynamoDBConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	options, _ := g.UnmarshalMap(table.SQL)

	// add columns if present
	if len(table.Columns) > 0 {
		options["columns"] = table.Columns
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, conn.scanRef(table), options)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

// BulkExportStream returns a datastream for the table
func (conn *DynamoDBConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	options, _ := g.UnmarshalMap(table.SQL)
	return conn.StreamRowsContext(conn.Context().Ctx, conn.scanRef(table), options)
}

// scanRef returns the reference to hand to StreamRowsContext: the table name
// when it is known, else the rendered select (a JSON scan descriptor, or a
// `select ... from <table>` statement)
func (conn *DynamoDBConn) scanRef(table Table) string {
	if name := table.FullName(); name != "" {
		return name
	}
	return table.SQL
}

// StreamRowsContext scans a table, applying the rendered filter/fields/limit
func (conn *DynamoDBConn) StreamRowsContext(ctx context.Context, tableName string, Opts ...map[string]any) (ds *iop.Datastream, err error) {
	if err = conn.ensureClient(); err != nil {
		return nil, err
	}

	// a lifecycle statement (`drop table if exists x`) reaches the read path
	// when a query hook runs it, since it cannot tell a statement from a select
	// for a connector without a SQL engine: run it and return no rows
	if isDynamoDBStatement(tableName) {
		if _, err = conn.ExecContext(ctx, tableName); err != nil {
			return nil, err
		}

		ds = iop.NewDatastreamIt(ctx, iop.Columns{}, func(it *iop.Iterator) bool { return false })
		if err = ds.Start(); err != nil {
			return ds, g.Error(err, "could start datastream")
		}
		return ds, nil
	}

	opts := getQueryOptions(Opts)

	// the reference is a table name, a JSON scan descriptor (as rendered by the
	// template) or a `select ... from <table>` statement
	tableRef, descriptor, err := dynamoDBScanRef(tableName)
	if err != nil {
		return nil, err
	} else if len(descriptor) > 0 {
		opts = mergeDynamoDBScanOptions(opts, descriptor)
	}

	table, err := ParseTableName(tableRef, conn.Type)
	if err != nil {
		return nil, g.Error(err, "could not parse table name: %s", tableRef)
	}

	columns := iop.Columns{}
	if val, ok := opts["columns"]; ok {
		g.JSONConvert(val, &columns)
	}
	if len(columns) == 0 {
		if columns, err = conn.GetTableColumns(&table); err != nil {
			return nil, g.Error(err, "could not get columns for table %s", table.Name)
		} else if len(columns) == 0 {
			// an existing table always has at least its key attributes
			return nil, g.Error("table %s does not exist", table.Name)
		}
	}
	allColumns := columns

	// select fields
	fields := cast.ToStringSlice(opts["fields"])
	projection := []string{}
	if len(fields) > 0 && fields[0] != "*" {
		selected := iop.Columns{}
		for _, field := range fields {
			for _, col := range columns {
				if strings.EqualFold(col.Name, field) {
					projection = append(projection, col.Name)
					selected = append(selected, col)
					break
				}
			}
		}
		columns = selected
	}

	// filter -> FilterExpression. Filters resolve values against every column
	// (not just the projected ones) so that a filtered attribute keeps its
	// stored type.
	filter := newDynamoDBFilter()
	if val, ok := opts["filter"]; ok && val != nil {
		if err = filter.add(val, allColumns); err != nil {
			return nil, g.Error(err, "could not build filter for table %s", table.Name)
		}
	}

	// incremental and backfill conditions, rendered into options by the template
	updateKey := cast.ToString(opts["update_key"])
	incrementalValue := cast.ToString(opts["value"])
	startValue := cast.ToString(opts["start_value"])
	endValue := cast.ToString(opts["end_value"])
	if updateKey != "" {
		switch {
		case incrementalValue != "":
			err = filter.addCondition(updateKey, map[string]any{"$gt": incrementalValue}, allColumns)
		case startValue != "" && endValue != "":
			err = filter.addCondition(updateKey, map[string]any{"$gte": startValue, "$lte": endValue}, allColumns)
		}
		if err != nil {
			return nil, g.Error(err, "could not build incremental filter for table %s", table.Name)
		}
	}

	limit := cast.ToInt64(opts["limit"])

	input := &dynamodb.ScanInput{TableName: aws.String(table.Name)}
	if fexpr := filter.expression(); fexpr != "" {
		input.FilterExpression = aws.String(fexpr)
	}
	if len(filter.names) > 0 {
		input.ExpressionAttributeNames = filter.names
	}
	if len(filter.values) > 0 {
		input.ExpressionAttributeValues = filter.values
	}
	if len(projection) > 0 {
		input.ProjectionExpression = aws.String(strings.Join(projection, ", "))
	}

	conn.LogSQL(g.F("table=%s options=%s", table.Name, g.Marshal(opts)))

	ds = iop.NewDatastreamContext(ctx, nil)
	ds.Columns = columns

	counter := uint64(0)
	var pageItems []map[string]ddbtypes.AttributeValue
	var lastKey map[string]ddbtypes.AttributeValue
	itemIndex := 0
	done := false

	nextFunc := func(it *iop.Iterator) bool {
		if limit > 0 && counter >= uint64(limit) {
			return false
		} else if it.Context.Err() != nil {
			return false
		}

		for {
			if itemIndex < len(pageItems) {
				item := pageItems[itemIndex]
				itemIndex++

				row, err := conn.itemToRow(item, ds.Columns)
				if err != nil {
					it.Context.CaptureErr(err)
					return false
				}

				it.Row = row
				counter++
				return true
			}

			if done {
				return false
			}

			input.ExclusiveStartKey = lastKey
			out, err := conn.Client.Scan(ctx, input)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "could not scan table %s", table.Name))
				return false
			}

			pageItems = out.Items
			itemIndex = 0
			lastKey = out.LastEvaluatedKey
			done = len(lastKey) == 0
		}
	}

	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))
	ds.NoDebug = strings.Contains(tableName, noDebugKey)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	if err = ds.Start(); err != nil {
		return ds, g.Error(err, "could start datastream")
	}

	// unmarshal columns if none detected,
	// otherwise this may error when creating a temp table with no columns
	if len(ds.Columns) == 0 {
		g.JSONConvert(opts["columns"], &ds.Columns)
	}

	return ds, nil
}

// itemToRow converts an item into a row matching the given columns
func (conn *DynamoDBConn) itemToRow(item map[string]ddbtypes.AttributeValue, columns iop.Columns) (row []any, err error) {
	row = make([]any, len(columns))

	for i, col := range columns {
		av, ok := item[col.Name]
		if !ok {
			row[i] = nil
			continue
		}
		if row[i], err = dynamoDBValueToGo(av, col.Type); err != nil {
			return nil, g.Error(err, "could not convert column %s", col.Name)
		}
	}

	return row, nil
}

// dynamoDBValueToGo converts an attribute value to a Go value, guided by the column type
func dynamoDBValueToGo(av ddbtypes.AttributeValue, colType iop.ColumnType) (any, error) {
	switch val := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		if colType.IsDate() || colType.IsDatetime() {
			if parsed, err := parseISOTime(val.Value); err == nil {
				return parsed, nil
			}
		}
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberN:
		if colType == iop.BigIntType {
			if parsed, err := strconv.ParseInt(val.Value, 10, 64); err == nil {
				return parsed, nil
			}
		}
		if colType == iop.FloatType {
			return cast.ToFloat64(val.Value), nil
		}
		if parsed, err := strconv.ParseInt(val.Value, 10, 64); err == nil {
			return parsed, nil
		} else if _, err := strconv.ParseFloat(val.Value, 64); err == nil {
			return val.Value, nil // keep full precision as a string
		}
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberB:
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberBOOL:
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberNULL:
		return nil, nil
	case *ddbtypes.AttributeValueMemberL, *ddbtypes.AttributeValueMemberM,
		*ddbtypes.AttributeValueMemberSS, *ddbtypes.AttributeValueMemberNS,
		*ddbtypes.AttributeValueMemberBS:
		native, err := dynamoDBValueToGoNative(av, colType)
		if err != nil {
			return nil, err
		}
		bytes, err := json.Marshal(native)
		return string(bytes), err
	}
	return nil, nil
}

// dynamoDBValueToGoNative converts an attribute value into a Go value, keeping
// nested structures native (a list stays a slice, a map stays a map) so that a
// document attribute is not stringified field by field
func dynamoDBValueToGoNative(av ddbtypes.AttributeValue, colType iop.ColumnType) (any, error) {
	switch val := av.(type) {
	case *ddbtypes.AttributeValueMemberL:
		arr := make([]any, len(val.Value))
		for i, item := range val.Value {
			v, err := dynamoDBValueToGoNative(item, "")
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case *ddbtypes.AttributeValueMemberM:
		m := map[string]any{}
		for key, item := range val.Value {
			v, err := dynamoDBValueToGoNative(item, "")
			if err != nil {
				return nil, err
			}
			m[key] = v
		}
		return m, nil
	case *ddbtypes.AttributeValueMemberSS:
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberNS:
		return val.Value, nil
	case *ddbtypes.AttributeValueMemberBS:
		arr := make([]string, len(val.Value))
		for i, item := range val.Value {
			arr[i] = string(item)
		}
		return arr, nil
	}
	return dynamoDBValueToGo(av, colType)
}

// parseISOTime parses RFC3339 timestamps and ISO dates
func parseISOTime(text string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return parsed, nil
	}
	return time.Parse("2006-01-02", text)
}

// marshalDynamoJSON renders a complex attribute value as JSON text
func marshalDynamoJSON(av any) (string, error) {
	switch val := av.(type) {
	case []ddbtypes.AttributeValue:
		arr := make([]any, len(val))
		for i, item := range val {
			v, err := dynamoDBValueToGoNative(item, "")
			if err != nil {
				return "", err
			}
			arr[i] = v
		}
		bytes, err := json.Marshal(arr)
		return string(bytes), err
	case map[string]ddbtypes.AttributeValue:
		m := map[string]any{}
		for k, item := range val {
			v, err := dynamoDBValueToGoNative(item, "")
			if err != nil {
				return "", err
			}
			m[k] = v
		}
		bytes, err := json.Marshal(m)
		return string(bytes), err
	case []string:
		bytes, err := json.Marshal(val)
		return string(bytes), err
	case [][]byte:
		arr := make([]string, len(val))
		for i, item := range val {
			arr[i] = string(item)
		}
		bytes, err := json.Marshal(arr)
		return string(bytes), err
	}
	bytes, err := json.Marshal(av)
	return string(bytes), err
}

// BulkImportFlow imports data into DynamoDB
func (conn *DynamoDBConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	df.Context.SetConcurrencyLimit(conn.Context().Wg.Limit)

	doImport := func(tableFName string, ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()

		cnt, err := conn.BulkImportStream(tableFName, ds)
		count += cnt
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not bulk import into %s", tableFName))
		} else if err = ds.Err(); err != nil {
			df.Context.CaptureErr(g.Error(err, "could not bulk import into %s", tableFName))
		}
	}

	for ds := range df.StreamCh {
		df.Context.Wg.Write.Add()
		doImport(tableFName, ds)
	}

	df.Context.Wg.Write.Wait()

	return count, df.Err()
}

// BulkImportStream writes a datastream with BatchWriteItem
func (conn *DynamoDBConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	desc, found, err := conn.describeTable(conn.Context().Ctx, table.Name)
	if err != nil {
		return 0, err
	} else if !found {
		return 0, g.Error("table %s does not exist", table.Name)
	}

	keyNames := []string{}
	for _, keySchema := range desc.KeySchema {
		keyNames = append(keyNames, *keySchema.AttributeName)
	}

	ctx := conn.Context().Ctx
	items := []map[string]ddbtypes.AttributeValue{}

	flush := func() (err error) {
		if len(items) == 0 {
			return nil
		}

		// a batch cannot carry the same key twice, so repeated keys collapse to
		// their last value (DynamoDB upserts on write)
		unique := dedupeByKey(items, keyNames)
		if collapsed := len(items) - len(unique); collapsed > 0 {
			g.Warn("collapsed %d row(s) on duplicate key(s) %s (DynamoDB writes are keyed upserts)",
				collapsed, strings.Join(keyNames, ", "))
		}

		if err = conn.batchWriteRequests(ctx, table.Name, putRequests(unique)); err != nil {
			return g.Error(err, "could not write to table %s", table.Name)
		}
		count += uint64(len(unique))
		items = items[:0]
		return nil
	}

	for row := range ds.Rows() {
		item, err := conn.rowToItem(ds.Columns, row, keyNames)
		if err != nil {
			return count, err
		}
		items = append(items, item)

		if len(items) >= dynamoDBBatchSize {
			if err = flush(); err != nil {
				return count, err
			}
		}
	}

	if err = ds.Err(); err != nil {
		return count, g.Error(err, "stream error")
	}

	if err = flush(); err != nil {
		return count, err
	}

	return count, nil
}

// InsertStream writes a datastream (DynamoDB upserts on write)
func (conn *DynamoDBConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertBatchStream writes a datastream (DynamoDB upserts on write)
func (conn *DynamoDBConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// dynamoDBNowExpressions are the SQL functions that mean "now"
var dynamoDBNowExpressions = []string{
	"current_timestamp", "current_timestamp()", "now()", "getdate()", "sysdate()", "sysdate", "localtimestamp",
}

// updateItems applies `update <table> set <col> = <value> where <cond>` by
// rewriting the matching items. sling renders this shape to soft-delete the rows
// that are missing from a stream (`delete_missing: soft`), which is the only
// update DynamoDB needs to support.
func (conn *DynamoDBConn) updateItems(ctx context.Context, text string) (err error) {
	setLoc := dynamoDBSetRegex.FindStringIndex(text)
	if setLoc == nil {
		return g.Error("could not parse update statement: %s", text)
	}

	tableName := parseDynamoDBTableToken(text, "update")
	setEnd := len(text)
	if whereLoc := dynamoDBWhereRegex.FindStringIndex(text[setLoc[1]:]); whereLoc != nil {
		setEnd = setLoc[1] + whereLoc[0]
	}

	assignments, err := parseDynamoDBAssignments(text[setLoc[1]:setEnd])
	if err != nil {
		return err
	}

	filter := map[string]any{}
	join := dynamoDBNotExistsJoin{}
	if setEnd < len(text) {
		whereText := text[setEnd:]
		join, err = parseDynamoDBNotExistsJoin(whereText)
		if err != nil {
			return err
		} else if join.Table != "" {
			whereText = stripDynamoDBNotExistsJoin(whereText)
		}

		if filter, err = parseDynamoDBWhere(whereText); err != nil {
			return err
		}
	}

	excluded := map[string]bool{}
	if join.Table != "" {
		if excluded, err = conn.loadDynamoDBKeySet(ctx, join); err != nil {
			return err
		}
	}

	pending := []map[string]ddbtypes.AttributeValue{}
	flush := func() (err error) {
		if len(pending) == 0 {
			return nil
		}
		if err = conn.batchWriteRequests(ctx, tableName, putRequests(pending)); err != nil {
			return g.Error(err, "could not update table %s", tableName)
		}
		pending = nil
		return nil
	}

	err = conn.scanDynamoDBItems(ctx, tableName, filter, func(item map[string]ddbtypes.AttributeValue) error {
		if len(join.Columns) > 0 && excluded[dynamoDBItemKey(item, join.Columns)] {
			return nil // the stream still holds this row, so keep it untouched
		}

		for col, assignment := range assignments {
			if assignment.remove {
				delete(item, col)
			} else {
				item[col] = assignment.value
			}
		}

		pending = append(pending, item)
		if len(pending) >= dynamoDBBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}

	return flush()
}

// deleteItems applies `delete from <table> [where <cond>]`. sling renders this
// shape to drop the rows that are missing from a stream
// (`delete_missing: hard`).
func (conn *DynamoDBConn) deleteItems(ctx context.Context, text string) (err error) {
	tableName := parseDynamoDBTableToken(text, "delete from")

	filter := map[string]any{}
	join := dynamoDBNotExistsJoin{}
	if whereLoc := dynamoDBWhereRegex.FindStringIndex(text); whereLoc != nil {
		whereText := text[whereLoc[0]:]
		join, err = parseDynamoDBNotExistsJoin(whereText)
		if err != nil {
			return err
		} else if join.Table != "" {
			whereText = stripDynamoDBNotExistsJoin(whereText)
		}

		if filter, err = parseDynamoDBWhere(whereText); err != nil {
			return err
		}
	}

	excluded := map[string]bool{}
	if join.Table != "" {
		if excluded, err = conn.loadDynamoDBKeySet(ctx, join); err != nil {
			return err
		}
	}

	desc, found, err := conn.describeTable(ctx, tableName)
	if err != nil {
		return err
	} else if !found {
		return nil
	}

	keyNames := []string{}
	for _, keySchema := range desc.KeySchema {
		keyNames = append(keyNames, *keySchema.AttributeName)
	}

	keys := []map[string]ddbtypes.AttributeValue{}
	flush := func() (err error) {
		if len(keys) == 0 {
			return nil
		}
		if err = conn.batchWriteRequests(ctx, tableName, deleteRequests(keys)); err != nil {
			return g.Error(err, "could not delete from table %s", tableName)
		}
		keys = nil
		return nil
	}

	err = conn.scanDynamoDBItems(ctx, tableName, filter, func(item map[string]ddbtypes.AttributeValue) error {
		if len(join.Columns) > 0 && excluded[dynamoDBItemKey(item, join.Columns)] {
			return nil // the stream still holds this row, so do not delete it
		}

		key := map[string]ddbtypes.AttributeValue{}
		for _, name := range keyNames {
			key[name] = item[name]
		}

		keys = append(keys, key)
		if len(keys) >= dynamoDBBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}

	return flush()
}

// dynamoDBNotExistsJoin describes the clause sling appends to its soft and hard
// delete statements, which keeps the rows that the stream still holds:
//
//	and not exists (select 1 from <temp_table> where <target>.<col> = <temp>.<col> ...)
type dynamoDBNotExistsJoin struct {
	Table   string   // table holding the keys of the current stream
	Columns []string // columns compared between that table and the target
}

// parseDynamoDBNotExistsJoin reads the `not exists (...)` clause, if present
func parseDynamoDBNotExistsJoin(text string) (join dynamoDBNotExistsJoin, err error) {
	lower := strings.ToLower(text)
	idx := strings.Index(lower, "not exists")
	if idx == -1 {
		return join, nil
	}

	openIdx := strings.Index(text[idx:], "(")
	if openIdx == -1 {
		return join, g.Error("could not parse `not exists` clause: %s", text)
	} else if closeIdx := matchDynamoDBParen(text[idx+openIdx:]); closeIdx == -1 {
		return join, g.Error("unbalanced parenthesis in `not exists` clause: %s", text)
	} else {
		clause := text[idx+openIdx+1 : idx+openIdx+closeIdx]

		fromIdx := indexDynamoDBKeyword(clause, "from")
		if fromIdx == -1 {
			return join, g.Error("could not find `from` in `not exists` clause: %s", clause)
		}

		rest := strings.TrimSpace(clause[fromIdx+len("from"):])
		if whereIdx := indexDynamoDBKeyword(rest, "where"); whereIdx != -1 {
			join.Table = unquoteDynamoDBIdentifier(rest[:whereIdx])
			rest = rest[whereIdx+len("where"):]
		} else {
			join.Table = unquoteDynamoDBIdentifier(rest)
			rest = ""
		}

		for _, condition := range strings.Split(rest, " and ") {
			sides := strings.Split(condition, "=")
			if len(sides) != 2 {
				continue
			}

			left := unquoteDynamoDBIdentifier(sides[0])
			right := unquoteDynamoDBIdentifier(sides[1])
			if left == "" || right == "" {
				continue
			} else if left == right {
				join.Columns = append(join.Columns, left)
				continue
			}

			// the clause compares `<temp>.<col> = <target>.<col>`, so the column
			// name is the same on both sides of a well-formed join
			return join, g.Error("unsupported `not exists` join condition: %s", strings.TrimSpace(condition))
		}
	}

	return join, nil
}

// indexDynamoDBKeyword returns the offset of a standalone keyword, which sling
// renders on its own line (with indentation) inside the delete statements.
func indexDynamoDBKeyword(text, keyword string) int {
	lower := strings.ToLower(text)
	offset := 0

	for {
		idx := strings.Index(lower[offset:], keyword)
		if idx == -1 {
			return -1
		}
		idx += offset

		end := idx + len(keyword)
		isBoundary := func(position int) bool {
			if position < 0 || position >= len(lower) {
				return true
			}
			switch lower[position] {
			case ' ', '\t', '\n', '\r', '(', ')', ',':
				return true
			}
			return false
		}

		if isBoundary(idx-1) && isBoundary(end) {
			return idx
		}
		offset = idx + 1
	}
}

// isDynamoDBIdentifier reports whether a name is a plain column name: DynamoDB
// has no functions or expressions to fold into a filter, but attribute names may
// carry dashes and dots
func isDynamoDBIdentifier(name string) bool {
	if name == "" {
		return false
	}

	for i, char := range name {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		case char >= '0' && char <= '9' && i > 0:
		case char == '-' || char == '.' || char == '$':
		default:
			return false
		}
	}

	return true
}

// stripDynamoDBNotExistsJoin removes the `not exists (...)` clause, so the
// remaining conditions can be parsed
func stripDynamoDBNotExistsJoin(text string) string {
	lower := strings.ToLower(text)
	idx := strings.Index(lower, "not exists")
	if idx == -1 {
		return text
	}

	openIdx := strings.Index(text[idx:], "(")
	if openIdx == -1 {
		return text
	}
	closeIdx := matchDynamoDBParen(text[idx+openIdx:])
	if closeIdx == -1 {
		return text
	}

	rest := text[:idx] + text[idx+openIdx+closeIdx+1:]
	rest = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), "and"))
	return strings.TrimSpace(rest)
}

// matchDynamoDBParen returns the index of the parenthesis closing the one at
// position 0 of text
func matchDynamoDBParen(text string) int {
	depth, quote := 0, byte(0)
	for i := 0; i < len(text); i++ {
		switch char := text[i]; {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"':
			quote = char
		case char == '(':
			depth++
		case char == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// dynamoDBItemKey renders the joined columns of an item as a signature
func dynamoDBItemKey(item map[string]ddbtypes.AttributeValue, columns []string) string {
	signature := strings.Builder{}
	for _, col := range columns {
		signature.WriteString(g.F("%s\x00%s\x00", col, dynamoDBKeyValue(item[col])))
	}
	return signature.String()
}

// loadDynamoDBKeySet reads the key signatures of the stream's temp table
func (conn *DynamoDBConn) loadDynamoDBKeySet(ctx context.Context, join dynamoDBNotExistsJoin) (keys map[string]bool, err error) {
	keys = map[string]bool{}
	if join.Table == "" {
		return keys, nil
	}

	err = conn.scanDynamoDBItems(ctx, join.Table, nil, func(item map[string]ddbtypes.AttributeValue) error {
		keys[dynamoDBItemKey(item, join.Columns)] = true
		return nil
	})
	if err != nil {
		return nil, g.Error(err, "could not read %s", join.Table)
	}

	return keys, nil
}

// scanDynamoDBItems pages through the items that match a filter
func (conn *DynamoDBConn) scanDynamoDBItems(ctx context.Context, tableName string, filter map[string]any, fn func(item map[string]ddbtypes.AttributeValue) error) (err error) {
	if err = conn.ensureClient(); err != nil {
		return err
	}

	f := newDynamoDBFilter()
	if len(filter) > 0 {
		if err = f.add(filter, iop.Columns{}); err != nil {
			return err
		}
	}

	var lastKey map[string]ddbtypes.AttributeValue
	for {
		input := &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ExclusiveStartKey: lastKey,
		}
		if expression := f.expression(); expression != "" {
			input.FilterExpression = aws.String(expression)
			input.ExpressionAttributeNames = f.names
			if len(f.values) > 0 {
				input.ExpressionAttributeValues = f.values
			}
		}

		out, err := conn.Client.Scan(ctx, input)
		if err != nil {
			return g.Error(err, "could not scan table %s", tableName)
		}

		for _, item := range out.Items {
			if err = fn(item); err != nil {
				return err
			}
		}

		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			return nil
		}
	}
}

// dynamoDBAssignment is a column assignment of an update statement
type dynamoDBAssignment struct {
	value  ddbtypes.AttributeValue
	remove bool // `set col = null` drops the attribute
}

// parseDynamoDBAssignments reads the `col = value` list of an update statement
func parseDynamoDBAssignments(text string) (assignments map[string]dynamoDBAssignment, err error) {
	assignments = map[string]dynamoDBAssignment{}

	for _, item := range splitDynamoDBTopLevel(text, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, g.Error("could not parse assignment %q", strings.TrimSpace(item))
		}

		col := strings.Trim(strings.TrimSpace(parts[0]), "`\"")
		expr := strings.TrimSpace(parts[1])

		if g.In(strings.ToLower(expr), dynamoDBNowExpressions...) {
			assignments[col] = dynamoDBAssignment{
				value: &ddbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
			}
			continue
		} else if strings.EqualFold(expr, "null") {
			assignments[col] = dynamoDBAssignment{remove: true}
			continue
		}

		literal, ok := parseDynamoDBLiteral(expr)
		if !ok {
			return nil, g.Error("unsupported value %q in update statement", expr)
		}

		value, err := dynamoDBFilterValue(literal, nil)
		if err != nil {
			return nil, err
		}
		assignments[col] = dynamoDBAssignment{value: value}
	}

	return assignments, nil
}

// parseDynamoDBWhere converts the conditions sling renders for missing records
// (`col is null`, `col is not null`, and `col <op> literal`, joined by AND) into
// the filter object the scan builder understands.
func parseDynamoDBWhere(text string) (filter map[string]any, err error) {
	filter = map[string]any{}

	conditions := strings.TrimSpace(text)
	if strings.HasPrefix(strings.ToLower(conditions), "where ") {
		conditions = strings.TrimSpace(conditions[len("where "):])
	}

	for _, condition := range splitDynamoDBTopLevel(conditions, " and ") {
		condition = strings.Trim(strings.TrimSpace(condition), "()")
		if condition == "" {
			continue
		}

		// an unfiltered update/delete renders `where 1=1`, which asks for nothing
		if strings.ReplaceAll(condition, " ", "") == "1=1" {
			continue
		}

		lower := strings.ToLower(condition)
		switch {
		case strings.HasSuffix(lower, " is null"):
			col := unquoteDynamoDBIdentifier(condition[:len(condition)-len(" is null")])
			if !isDynamoDBIdentifier(col) {
				return nil, g.Error("could not parse condition %q", condition)
			}
			filter[col] = map[string]any{"$exists": false}
		case strings.HasSuffix(lower, " is not null"):
			col := unquoteDynamoDBIdentifier(condition[:len(condition)-len(" is not null")])
			if !isDynamoDBIdentifier(col) {
				return nil, g.Error("could not parse condition %q", condition)
			}
			filter[col] = map[string]any{"$exists": true}
		default:
			col, op, literal, ok := parseDynamoDBComparison(condition)
			if !ok || !isDynamoDBIdentifier(col) {
				// a condition the connector cannot evaluate must not be dropped:
				// deleting rows the user excluded is worse than failing loudly
				return nil, g.Error("could not parse condition %q", condition)
			}
			filter[col] = map[string]any{op: literal}
		}
	}

	return filter, nil
}

// parseDynamoDBComparison reads a `col <op> literal` condition
func parseDynamoDBComparison(condition string) (col string, op string, literal any, ok bool) {
	operators := []string{"<=", ">=", "<>", "!=", "=", "<", ">"}
	for _, operator := range operators {
		idx := strings.Index(condition, operator)
		if idx == -1 {
			continue
		}

		col = unquoteDynamoDBIdentifier(condition[:idx])
		literalText := strings.TrimSpace(condition[idx+len(operator):])
		value, isLiteral := parseDynamoDBLiteral(literalText)
		if !isLiteral {
			return "", "", nil, false
		}

		switch operator {
		case "=":
			op = "$eq"
		case "<>", "!=":
			op = "$ne"
		case "<":
			op = "$lt"
		case "<=":
			op = "$lte"
		case ">":
			op = "$gt"
		case ">=":
			op = "$gte"
		}

		return col, op, value, true
	}

	return "", "", nil, false
}

// parseDynamoDBLiteral reads a SQL literal: a quoted string, a number, or a bool
func parseDynamoDBLiteral(expr string) (value any, ok bool) {
	text := strings.TrimSpace(expr)
	if len(text) >= 2 {
		quote := text[0]
		if (quote == '\'' || quote == '"') && text[len(text)-1] == quote {
			inner := text[1 : len(text)-1]
			return strings.ReplaceAll(inner, string([]byte{quote, quote}), string(quote)), true
		}
	}

	switch strings.ToLower(text) {
	case "true":
		return true, true
	case "false":
		return false, true
	}

	if number, err := strconv.ParseFloat(text, 64); err == nil {
		return number, true
	}

	return nil, false
}

// unquoteDynamoDBIdentifier strips the quoting and qualifier of a column name
func unquoteDynamoDBIdentifier(text string) string {
	name := strings.Trim(strings.TrimSpace(text), "`\"'")
	name = strings.TrimPrefix(name, "()")
	name = strings.Trim(name, "()")
	if parts := strings.Split(name, "."); len(parts) > 1 {
		name = parts[len(parts)-1]
	}
	return strings.Trim(name, "`\"'")
}

// splitDynamoDBTopLevel splits text on a separator that is not inside quotes or
// parenthesis
func splitDynamoDBTopLevel(text string, separator string) (items []string) {
	depth := 0
	quote := byte(0)
	start := 0

	for i := 0; i < len(text); i++ {
		switch char := text[i]; {
		case quote != 0:
			if char == quote {
				quote = 0
			}
		case char == '\'' || char == '"':
			quote = char
		case char == '(':
			depth++
		case char == ')':
			depth--
		case depth == 0 && strings.EqualFold(text[i:min(i+len(separator), len(text))], separator):
			items = append(items, text[start:i])
			i += len(separator) - 1
			start = i + 1
		}
	}

	return append(items, text[start:])
}

// batchWriteRequests sends write requests in batches of 25, retrying unprocessed items
func (conn *DynamoDBConn) batchWriteRequests(ctx context.Context, tableName string, requests []ddbtypes.WriteRequest) (err error) {
	if err = conn.ensureClient(); err != nil {
		return err
	}
	for len(requests) > 0 {
		batch := requests
		if len(batch) > dynamoDBBatchSize {
			batch = batch[:dynamoDBBatchSize]
		}
		requests = requests[len(batch):]

		pending := map[string][]ddbtypes.WriteRequest{tableName: batch}
		for attempt := 1; ; attempt++ {
			out, err := conn.Client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return g.Error(err, "batch write failed")
			}

			unprocessed := out.UnprocessedItems[tableName]
			if len(unprocessed) == 0 {
				break
			} else if attempt >= 10 {
				return g.Error("could not write %d item(s) after %d attempts (throttled)", len(unprocessed), attempt)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*50) * time.Millisecond):
			}
			pending = map[string][]ddbtypes.WriteRequest{tableName: unprocessed}
		}
	}

	return nil
}

// dedupeByKey keeps the last item seen for each key, preserving the order in
// which keys were first written. DynamoDB rejects a batch that carries the same
// key twice, and a repeated key means the later value wins anyway.
func dedupeByKey(items []map[string]ddbtypes.AttributeValue, keyNames []string) (unique []map[string]ddbtypes.AttributeValue) {
	if len(keyNames) == 0 {
		return items
	}

	positions := map[string]int{}
	for _, item := range items {
		sig := ""
		for _, name := range keyNames {
			sig += g.F("%s\x00%s\x00", name, dynamoDBKeyValue(item[name]))
		}

		if idx, ok := positions[sig]; ok {
			unique[idx] = item // keep the position, take the latest value
			continue
		}
		positions[sig] = len(unique)
		unique = append(unique, item)
	}
	return unique
}

// dynamoDBKeyValue renders a key attribute as text for comparison
func dynamoDBKeyValue(av ddbtypes.AttributeValue) string {
	switch v := av.(type) {
	case *ddbtypes.AttributeValueMemberS:
		return v.Value
	case *ddbtypes.AttributeValueMemberN:
		return v.Value
	case *ddbtypes.AttributeValueMemberB:
		return string(v.Value)
	}
	return g.Marshal(av)
}

// putRequests builds PutRequests (upsert by primary key)
func putRequests(items []map[string]ddbtypes.AttributeValue) (requests []ddbtypes.WriteRequest) {
	for _, item := range items {
		requests = append(requests, ddbtypes.WriteRequest{
			PutRequest: &ddbtypes.PutRequest{Item: item},
		})
	}
	return requests
}

// deleteRequests builds DeleteRequests from item keys
func deleteRequests(keys []map[string]ddbtypes.AttributeValue) (requests []ddbtypes.WriteRequest) {
	for _, key := range keys {
		requests = append(requests, ddbtypes.WriteRequest{
			DeleteRequest: &ddbtypes.DeleteRequest{Key: key},
		})
	}
	return requests
}

// dynamoDBSyntheticKeyValue returns a unique value for an added key column. A
// timestamp is not enough on its own: bulk writes can emit many rows in the
// same millisecond, so a sequence is appended.
func (conn *DynamoDBConn) dynamoDBSyntheticKeyValue() string {
	return g.F("%s%08d", g.NewTsID(""), atomic.AddUint64(&conn.syntheticKeySeq, 1))
}

// rowToItem converts a row into a DynamoDB item
func (conn *DynamoDBConn) rowToItem(columns iop.Columns, row []any, keyNames []string) (item map[string]ddbtypes.AttributeValue, err error) {
	item = map[string]ddbtypes.AttributeValue{}

	for i, col := range columns {
		if i >= len(row) || row[i] == nil {
			continue
		}

		av, err := goToDynamoDBValue(row[i], col.Type)
		if err != nil {
			return nil, g.Error(err, "could not convert column %s", col.Name)
		} else if av == nil {
			continue
		}
		item[col.Name] = av
	}

	for _, name := range keyNames {
		if _, ok := item[name]; ok {
			continue
		}

		if isDynamoDBSyntheticKey(name) {
			// the key column sling added: every row needs its own value
			item[name] = &ddbtypes.AttributeValueMemberS{Value: conn.dynamoDBSyntheticKeyValue()}
			continue
		}

		return nil, g.Error("key attribute %s is missing (DynamoDB requires it on every item)", name)
	}

	return item, nil
}

// goToDynamoDBValue converts a Go value into a DynamoDB attribute value
func goToDynamoDBValue(val any, colType iop.ColumnType) (ddbtypes.AttributeValue, error) {
	switch {
	case val == nil:
		return nil, nil
	case colType.IsBool():
		return &ddbtypes.AttributeValueMemberBOOL{Value: cast.ToBool(val)}, nil
	case colType.IsInteger():
		return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(cast.ToInt64(val), 10)}, nil
	case colType.IsNumber():
		return &ddbtypes.AttributeValueMemberN{Value: numberString(cast.ToFloat64(val))}, nil
	case colType.IsBinary():
		switch v := val.(type) {
		case []byte:
			return &ddbtypes.AttributeValueMemberB{Value: v}, nil
		default:
			return &ddbtypes.AttributeValueMemberB{Value: []byte(cast.ToString(val))}, nil
		}
	case colType == iop.JsonType:
		if parsed, ok := unmarshalDynamoJSON(val); ok {
			return parsed, nil
		}
		return &ddbtypes.AttributeValueMemberS{Value: cast.ToString(val)}, nil
	case colType.IsDate() || colType.IsDatetime():
		if err := trySetTime(val); err != nil {
			return nil, err
		}
		return &ddbtypes.AttributeValueMemberS{Value: cast.ToTime(val).Format(time.RFC3339Nano)}, nil
	}

	return &ddbtypes.AttributeValueMemberS{Value: cast.ToString(val)}, nil
}

// trySetTime validates that a value can be represented as a timestamp
func trySetTime(val any) error {
	if cast.ToTime(val).IsZero() {
		return g.Error("could not convert value %#v to a timestamp", val)
	}
	return nil
}

// numberString renders a float without an exponent, so numbers round-trip
func numberString(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

// unmarshalDynamoJSON converts JSON text into native DynamoDB attribute values
func unmarshalDynamoJSON(val any) (av ddbtypes.AttributeValue, ok bool) {
	text, isText := val.(string)
	if !isText {
		return nil, false
	}

	parsed := any(nil)
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return nil, false
	}
	return goValueToDynamoJSON(parsed), true
}

// goValueToDynamoJSON converts a decoded JSON value into an attribute value
func goValueToDynamoJSON(val any) ddbtypes.AttributeValue {
	switch v := val.(type) {
	case nil:
		return &ddbtypes.AttributeValueMemberNULL{Value: true}
	case bool:
		return &ddbtypes.AttributeValueMemberBOOL{Value: v}
	case float64:
		if v == float64(int64(v)) {
			return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(int64(v), 10)}
		}
		return &ddbtypes.AttributeValueMemberN{Value: numberString(v)}
	case string:
		return &ddbtypes.AttributeValueMemberS{Value: v}
	case []any:
		items := make([]ddbtypes.AttributeValue, len(v))
		for i, item := range v {
			items[i] = goValueToDynamoJSON(item)
		}
		return &ddbtypes.AttributeValueMemberL{Value: items}
	case map[string]any:
		m := map[string]ddbtypes.AttributeValue{}
		for k, item := range v {
			m[k] = goValueToDynamoJSON(item)
		}
		return &ddbtypes.AttributeValueMemberM{Value: m}
	}
	return &ddbtypes.AttributeValueMemberS{Value: cast.ToString(val)}
}

// AddMissingColumns is a no-op: DynamoDB attributes other than the key are schemaless
func (conn *DynamoDBConn) AddMissingColumns(table Table, newCols iop.Columns) (ok bool, err error) {
	return false, nil
}

// CompareChecksums is not supported by DynamoDB
func (conn *DynamoDBConn) CompareChecksums(tableName string, columns iop.Columns) (err error) {
	return nil
}

// dynamoDBFilter builds a DynamoDB filter expression from the JSON filter that
// sling renders for a stream. Supported shapes:
//
//	{ "id": 5 }                        -> #n0 = :v0
//	{ "id": { "$gt": 5 } }             -> #n0 > :v0
//	{ "id": { "$between": [1, 5] } }   -> #n0 BETWEEN :v0 AND :v1
//	{ "id": { "$in": [1, 2] } }        -> #n0 IN (:v0, :v1)
//	{ "name": { "$begins_with": "a" }} -> begins_with(#n0, :v0)
//	{ "name": { "$contains": "a" } }   -> contains(#n0, :v0)
//	{ "col": { "$exists": false } }    -> attribute_not_exists(#n0)
//
// Multiple columns and operators are joined with AND.
type dynamoDBFilter struct {
	parts   []string
	names   map[string]string // alias -> column
	aliases map[string]string // column -> alias
	values  map[string]ddbtypes.AttributeValue
}

func newDynamoDBFilter() *dynamoDBFilter {
	return &dynamoDBFilter{
		names:   map[string]string{},
		aliases: map[string]string{},
		values:  map[string]ddbtypes.AttributeValue{},
	}
}

func (f *dynamoDBFilter) expression() string {
	return strings.Join(f.parts, " AND ")
}

// name registers an attribute name and returns its expression placeholder
func (f *dynamoDBFilter) name(col string) string {
	if alias, ok := f.aliases[col]; ok {
		return alias // one alias per column, reused across its conditions
	}

	alias := g.F("#n%d", len(f.names))
	f.names[alias] = col
	f.aliases[col] = alias
	return alias
}

// value registers an attribute value and returns its expression placeholder
func (f *dynamoDBFilter) value(col string, raw any, columns iop.Columns) (placeholder string, err error) {
	av, err := dynamoDBFilterValue(raw, columns.GetColumn(col))
	if err != nil {
		return "", g.Error(err, "invalid filter value for %s", col)
	} else if av == nil {
		return "", g.Error("filter value for %s is null", col)
	}

	placeholder = g.F(":v%d", len(f.values))
	f.values[placeholder] = av
	return placeholder, nil
}

// add adds every condition of a filter object
func (f *dynamoDBFilter) add(filter any, columns iop.Columns) (err error) {
	m, ok := filter.(map[string]any)
	if !ok {
		return g.Error("filter must be a JSON object, got %T", filter)
	}

	for col, spec := range m {
		if err = f.addCondition(col, spec, columns); err != nil {
			return err
		}
	}
	return nil
}

// addCondition adds one column condition
func (f *dynamoDBFilter) addCondition(col string, spec any, columns iop.Columns) (err error) {
	operators, isOperatorMap := spec.(map[string]any)
	if !isOperatorMap {
		// a plain value is an equality check
		placeholder, err := f.value(col, spec, columns)
		if err != nil {
			return err
		}
		f.parts = append(f.parts, g.F("%s = %s", f.name(col), placeholder))
		return nil
	}

	comparisons := map[string]string{"$eq": "=", "$ne": "<>", "$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}
	for op, val := range operators {
		switch op {
		case "$eq", "$ne", "$lt", "$lte", "$gt", "$gte":
			placeholder, err := f.value(col, val, columns)
			if err != nil {
				return err
			}
			f.parts = append(f.parts, g.F("%s %s %s", f.name(col), comparisons[op], placeholder))
		case "$begins_with", "$contains":
			placeholder, err := f.value(col, val, columns)
			if err != nil {
				return err
			}
			function := strings.TrimPrefix(op, "$")
			f.parts = append(f.parts, g.F("%s(%s, %s)", function, f.name(col), placeholder))
		case "$in":
			items, ok := val.([]any)
			if !ok {
				return g.Error("$in expects an array for %s", col)
			}
			placeholders := []string{}
			for _, item := range items {
				placeholder, err := f.value(col, item, columns)
				if err != nil {
					return err
				}
				placeholders = append(placeholders, placeholder)
			}
			f.parts = append(f.parts, g.F("%s IN (%s)", f.name(col), strings.Join(placeholders, ", ")))
		case "$between":
			items, ok := val.([]any)
			if !ok || len(items) != 2 {
				return g.Error("$between expects [start, end] for %s", col)
			}
			start, err := f.value(col, items[0], columns)
			if err != nil {
				return err
			}
			end, err := f.value(col, items[1], columns)
			if err != nil {
				return err
			}
			f.parts = append(f.parts, g.F("%s BETWEEN %s AND %s", f.name(col), start, end))
		case "$exists":
			if cast.ToBool(val) {
				f.parts = append(f.parts, g.F("attribute_exists(%s)", f.name(col)))
			} else {
				f.parts = append(f.parts, g.F("attribute_not_exists(%s)", f.name(col)))
			}
		default:
			return g.Error("unsupported filter operator %s for %s", op, col)
		}
	}

	return nil
}

// dynamoDBFilterValue converts a filter value into an attribute value of the
// type stored in the table, so that comparisons match the stored attributes.
func dynamoDBFilterValue(raw any, col *iop.Column) (ddbtypes.AttributeValue, error) {
	switch val := raw.(type) {
	case nil:
		return nil, nil
	case bool:
		return &ddbtypes.AttributeValueMemberBOOL{Value: val}, nil
	case float64:
		if val == float64(int64(val)) {
			return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(int64(val), 10)}, nil
		}
		return &ddbtypes.AttributeValueMemberN{Value: numberString(val)}, nil
	case map[string]any, []any:
		return goValueToDynamoJSON(raw), nil
	case string:
		// values rendered from another connection (incremental max values) are
		// quoted as SQL literals, and possibly carrying doubled quotes
		val = normalizeDynamoDBFilterString(val)

		if col != nil {
			if col.Type.IsBool() {
				return &ddbtypes.AttributeValueMemberBOOL{Value: cast.ToBool(val)}, nil
			}
			if col.Type.IsNumber() {
				if col.Type.IsInteger() {
					if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
						return &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(parsed, 10)}, nil
					}
				} else if parsed, err := strconv.ParseFloat(val, 64); err == nil {
					return &ddbtypes.AttributeValueMemberN{Value: numberString(parsed)}, nil
				}
				return nil, g.Error("value %q is not a number for column %s (%s)", val, col.Name, col.Type)
			}
		}
		return &ddbtypes.AttributeValueMemberS{Value: val}, nil
	}

	return &ddbtypes.AttributeValueMemberS{Value: cast.ToString(raw)}, nil
}

// normalizeDynamoDBFilterString removes the quoting that FormatValue applies to
// string values rendered for another connection.
func normalizeDynamoDBFilterString(val string) string {
	if len(val) >= 2 && strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
		return strings.ReplaceAll(val[1:len(val)-1], "''", "'")
	}
	return val
}

var _ Connection = (*DynamoDBConn)(nil)
