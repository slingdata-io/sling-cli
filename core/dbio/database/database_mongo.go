package database

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// matches ISODate("...") / ObjectId("...").
// The inner `\(?` tolerates the doubled paren in the date_layout_str template.
var shellCtorRegex = regexp.MustCompile(`^(ISODate|ObjectId)\(\(?"([^"]*)"\)\)?$`)

// MongoDBConn is a Mongo connection
type MongoDBConn struct {
	BaseConn
	URL    string
	Client *mongo.Client
}

// Init initiates the object
func (conn *MongoDBConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbMongoDB

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Init initiates the object
func (conn *MongoDBConn) getNewClient(timeOut ...int) (client *mongo.Client, err error) {

	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}

	client, err = mongo.Connect(
		conn.BaseConn.Context().Ctx,
		options.Client().ApplyURI(conn.URL),
	)
	if err != nil {
		return nil, g.Error(err, "could not connect to MongoDB server")
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()

	opts := []*options.ClientOptions{
		options.Client().ApplyURI(conn.URL),
		options.Client().SetCompressors([]string{"zstd", "snappy", "zlib"}),
	}

	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return nil, g.Error(err)
	} else if tlsConfig != nil {
		opts[0].SetTLSConfig(tlsConfig)
	}

	return mongo.Connect(ctx, opts...)
}

// Connect connects to the database
func (conn *MongoDBConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, 5*time.Second)
	defer cancel()
	err = conn.Client.Ping(ctx, readpref.Primary())
	if err != nil {
		if strings.Contains(err.Error(), "server selection error") {
			g.Info(env.MagentaString("Try setting the `tls` key to 'true'. See https://docs.slingdata.io/connections/database-connections/mongodb"))
		}
		return g.Error(err, "Failed to ping mongo server")
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

func (conn *MongoDBConn) Close() error {
	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, 5*time.Second)
	defer cancel()
	err := conn.Client.Disconnect(ctx)
	if err != nil {
		return g.Error(err, "Failed to disconnect")
	}
	g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))

	return nil
}

// NewTransaction creates a new transaction
func (conn *MongoDBConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// does not support transaction
	return
}

// NewTransaction creates a new transaction
func (conn *MongoDBConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	tables, err := conn.GetTables(table.Schema)
	if err != nil {
		return columns, g.Error(err, "could not query to get tables")
	}

	found := false
	for _, tableRow := range tables.Rows {
		if strings.EqualFold(cast.ToString(tableRow[0]), table.Name) {
			found = true
		}
	}

	if !found {
		return nil, g.Error("did not find collection %s", table.FullName())
	}

	ds, err := conn.StreamRows(table.FullName(), g.M("limit", 10, "silent", true, "get_columns", true))
	if err != nil {
		return columns, g.Error("could not query to get columns")
	}

	data, err := ds.Collect(10)
	if err != nil {
		return columns, g.Error("could not collect to get columns")
	}

	for i := range data.Columns {
		data.Columns[i].Schema = table.Schema
		data.Columns[i].Table = table.Name
		data.Columns[i].DbType = "-"
	}

	return data.Columns, nil
}

func (conn *MongoDBConn) ExecContext(ctx context.Context, sql string, args ...any) (result sql.Result, err error) {
	return nil, g.Error("ExecContext not implemented on MongoConn")
}

func (conn *MongoDBConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	options, _ := g.UnmarshalMap(table.SQL)

	// add columns if present
	if len(table.Columns) > 0 {
		options["columns"] = table.Columns
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, table.FullName(), options)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

// processMongoFilter recursively processes filter values to convert ObjectID strings
func (conn *MongoDBConn) processMongoFilter(filter any) any {
	switch v := filter.(type) {
	case map[string]any:
		if val, ok := parseExtendedJSON(v); ok {
			return val
		}

		// Process map recursively
		result := make(map[string]any)
		for key, val := range v {
			if key == "_id" || strings.HasSuffix(key, "_id") {
				// Special handling for _id fields
				result[key] = conn.processObjectIDValue(val)
			} else {
				result[key] = conn.processMongoFilter(val)
			}
		}
		return result
	case map[any]any:
		normalized := make(map[string]any, len(v))
		for key, val := range v {
			normalized[cast.ToString(key)] = val
		}
		if val, ok := parseExtendedJSON(normalized); ok {
			return val
		}

		// Process map recursively
		result := make(map[string]any)
		for keyStr, val := range normalized {
			if keyStr == "_id" || strings.HasSuffix(keyStr, "_id") {
				result[keyStr] = conn.processObjectIDValue(val)
			} else {
				result[keyStr] = conn.processMongoFilter(val)
			}
		}
		return result
	case []any:
		// Process array recursively
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = conn.processMongoFilter(item)
		}
		return result
	case string:
		// Check if string is ObjectID format
		if conn.isObjectIDString(v) {
			oid, err := primitive.ObjectIDFromHex(v)
			if err == nil {
				return oid
			}
		}
		// Check if string is an ISO 8601 datetime (from ISODate() normalization)
		if t, ok := parseISODateString(v); ok {
			return t
		}
		return v
	default:
		return filter
	}
}

// normalizeFilterValue converts a template-rendered incremental/backfill value
// into a real BSON value. The templates wrap datetimes as ISODate("<iso>"),
// which is mongosh syntax the driver does not understand.
func (conn *MongoDBConn) normalizeFilterValue(key, value string) any {
	value = strings.Trim(value, "'")

	if m := shellCtorRegex.FindStringSubmatch(value); m != nil {
		value = m[2]
	}

	if key == "_id" || strings.HasSuffix(key, "_id") {
		return conn.processObjectIDValue(value)
	}

	return conn.processMongoFilter(value)
}

// processObjectIDValue handles ObjectID conversion for _id fields
func (conn *MongoDBConn) processObjectIDValue(val any) any {
	switch v := val.(type) {
	case string:
		// Direct ObjectID hex string
		if conn.isObjectIDString(v) {
			if oid, err := primitive.ObjectIDFromHex(v); err == nil {
				return oid
			}
		}
		// ObjectId("...") format
		if strings.HasPrefix(v, "ObjectId(") && strings.HasSuffix(v, ")") {
			hex := strings.TrimSuffix(strings.TrimPrefix(v, "ObjectId(\""), "\")")
			if oid, err := primitive.ObjectIDFromHex(hex); err == nil {
				return oid
			}
		}
		return v
	case map[string]any:
		if val, ok := parseExtendedJSON(v); ok {
			return val
		}

		// Handle operators like $gte, $lt
		result := make(map[string]any)
		for op, opVal := range v {
			if strings.HasPrefix(op, "$") {
				result[op] = conn.processObjectIDValue(opVal)
			} else {
				result[op] = conn.processMongoFilter(opVal)
			}
		}
		return result
	case map[any]any:
		normalized := make(map[string]any, len(v))
		for op, opVal := range v {
			normalized[cast.ToString(op)] = opVal
		}
		if val, ok := parseExtendedJSON(normalized); ok {
			return val
		}

		// Handle operators like $gte, $lt
		result := make(map[string]any)
		for opStr, opVal := range normalized {
			if strings.HasPrefix(opStr, "$") {
				result[opStr] = conn.processObjectIDValue(opVal)
			} else {
				result[opStr] = conn.processMongoFilter(opVal)
			}
		}
		return result
	default:
		return conn.processMongoFilter(val)
	}
}

// parseExtendedJSON converts an Extended JSON wrapper to its BSON value,
// e.g. {"$date": "2019-06-01T00:00:00Z"} => time.Time. Returns false if the
// map is not a single-key wrapper of a supported type.
func parseExtendedJSON(m map[string]any) (any, bool) {
	if len(m) != 1 {
		return nil, false
	}

	for key, val := range m {
		switch key {
		case "$date":
			switch v := val.(type) {
			case string:
				if t, ok := parseISODateString(v); ok {
					return t, true
				}
			case map[string]any:
				if inner, ok := v["$numberLong"]; ok && len(v) == 1 {
					if millis, err := cast.ToInt64E(inner); err == nil {
						return time.UnixMilli(millis).UTC(), true
					}
				}
			default: // epoch millis
				if millis, err := cast.ToInt64E(val); err == nil {
					return time.UnixMilli(millis).UTC(), true
				}
			}
		case "$oid":
			if s, ok := val.(string); ok {
				if oid, err := primitive.ObjectIDFromHex(s); err == nil {
					return oid, true
				}
			}
		case "$numberLong":
			if i, err := cast.ToInt64E(val); err == nil {
				return i, true
			}
		case "$numberInt":
			if i, err := cast.ToInt32E(val); err == nil {
				return i, true
			}
		case "$numberDouble":
			if f, err := cast.ToFloat64E(val); err == nil {
				return f, true
			}
		case "$numberDecimal":
			if s, ok := val.(string); ok {
				if d, err := primitive.ParseDecimal128(s); err == nil {
					return d, true
				}
			}
		}
	}

	return nil, false
}

// parseISODateString attempts to parse an ISO 8601 datetime string.
// Returns the parsed time and true if successful, zero time and false otherwise.
func parseISODateString(s string) (time.Time, bool) {
	// Only attempt parsing for strings that look like ISO 8601 datetimes
	// (at least 10 chars for "YYYY-MM-DD" and contains a '-')
	if len(s) < 10 || s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// isObjectIDString checks if a string is a valid ObjectID hex format
func (conn *MongoDBConn) isObjectIDString(s string) bool {
	// ObjectID is 24 hex characters
	if len(s) != 24 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func (conn *MongoDBConn) StreamRowsContext(ctx context.Context, collectionName string, Opts ...map[string]any) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(Opts)
	Limit := int64(0) // infinite
	if val := cast.ToInt64(opts["limit"]); val > 0 {
		Limit = val
	}

	findOpts := &options.FindOptions{Limit: &Limit}
	fields := cast.ToStringSlice(opts["fields"])
	if len(fields) > 0 {
		d := bson.D{}
		for _, field := range fields {
			d = append(d, bson.D{{Key: field, Value: 1}}...)
		}
		findOpts = options.Find().SetProjection(d)
	}

	updateKey := cast.ToString(opts["update_key"])
	incrementalValue := cast.ToString(opts["value"])
	startValue := cast.ToString(opts["start_value"])
	endValue := cast.ToString(opts["end_value"])

	filter := bson.D{}
	if filterOpt, ok := opts["filter"]; ok {
		// Process the filter to convert ObjectID strings and ISO date strings
		filterOpt = conn.processMongoFilter(filterOpt)

		// Convert processed filter to bson.D
		switch v := filterOpt.(type) {
		case map[any]any:
			// Simple filter format: {"field": "value", "field2": {"$gt": 100}}
			for key, val := range v {
				filter = append(filter, bson.E{Key: cast.ToString(key), Value: val})
			}
		case map[string]any:
			// Simple filter format: {"field": "value", "field2": {"$gt": 100}}
			for key, val := range v {
				filter = append(filter, bson.E{Key: key, Value: val})
			}
		case []any:
			// Complex filter format for $and/$or:
			// [{"$or": [{"field1": "value1"}, {"field2": "value2"}]}]
			var filterD bson.D
			g.JSONConvert(v, &filterD)
			filter = filterD
		default:
			return nil, g.Error("unsupported filter format: %#v", filterOpt)
		}
	}

	// Add incremental/backfill filters if specified. Values arrive as
	// template-rendered strings, so normalize them to real BSON values.
	// Otherwise a Date field is compared against a String and matches nothing.
	if updateKey != "" && incrementalValue != "" {
		// incremental mode
		value := conn.normalizeFilterValue(updateKey, incrementalValue)
		filter = append(filter, bson.E{Key: updateKey, Value: bson.D{{Key: "$gt", Value: value}}})
	} else if updateKey != "" && startValue != "" && endValue != "" {
		// backfill mode
		start := conn.normalizeFilterValue(updateKey, startValue)
		end := conn.normalizeFilterValue(updateKey, endValue)
		filter = append(filter, bson.E{Key: updateKey, Value: bson.D{{Key: "$gte", Value: start}}})
		filter = append(filter, bson.E{Key: updateKey, Value: bson.D{{Key: "$lte", Value: end}}})
	}

	if strings.TrimSpace(collectionName) == "" {
		g.Warn("Empty collection name")
		return ds, nil
	}

	queryContext := g.NewContext(ctx)

	table, _ := ParseTableName(collectionName, conn.Type)

	collection := conn.Client.Database(table.Schema).Collection(table.Name)

	if !cast.ToBool(opts["silent"]) {
		conn.LogSQL(g.Marshal(g.M("database", table.Schema, "collection", table.Name, "filter", filter, "options", g.M("limit", findOpts.Limit, "projection", findOpts.Projection))))
	}

	var cur *mongo.Cursor
	if pipeline, ok := opts["pipeline"]; ok {
		// https://www.mongodb.com/docs/manual/core/aggregation-pipeline/
		var aggPipeline []bson.D

		// check if pipeline is a slice of any
		switch v := pipeline.(type) {
		case []any:
			for _, item := range v {
				var aggItem bson.D
				g.JSONConvert(item, &aggItem)
				aggPipeline = append(aggPipeline, aggItem)
			}
		default:
			return nil, g.Error("unsupported input for aggregation pipeline: %#v", pipeline)
		}
		cur, err = collection.Aggregate(queryContext.Ctx, aggPipeline)
	} else {
		cur, err = collection.Find(queryContext.Ctx, filter, findOpts)
	}
	if err != nil {
		return ds, g.Error(err, "error querying collection")
	}

	ds = iop.NewDatastreamContext(queryContext.Ctx, nil)

	flatten := 0
	if val := conn.GetProp("flatten"); val != "" {
		flatten = cast.ToInt(val)
	}
	js := iop.NewJSONStream(ds, cur, flatten, conn.GetProp("jmespath"), conn.GetProp("jq"))
	js.HasMapPayload = true

	limit := cast.ToUint64(Limit)
	nextFunc := func(it *iop.Iterator) bool {
		if Limit > 0 && it.Counter >= limit {
			return false
		} else if it.Context.Err() != nil {
			return false
		}

		for cur.Next(queryContext.Ctx) {
			if js.NextFunc(it) {
				for i, val := range it.Row {
					switch vt := val.(type) {
					case primitive.ObjectID:
						// fix Object ID
						it.Row[i] = strings.TrimSuffix(
							strings.TrimPrefix(
								cast.ToString(it.Row[i]), `ObjectID("`,
							), `")`,
						)
					case primitive.DateTime:
						it.Row[i] = vt.Time()
					case primitive.Timestamp:
						it.Row[i] = time.Unix(cast.ToInt64(vt.T), 0)
					case primitive.A:
						// Array
						it.Row[i] = g.Marshal(vt)
					}
				}
				return true
			}
		}
		return false
	}

	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))
	ds.NoDebug = strings.Contains(collectionName, noDebugKey)
	if !cast.ToBool(opts["get_columns"]) {
		// only apply metadata, casing transformations when not fetching columns
		// this can pre-maturely transform the column names casing, returning wrong names
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}
	ds.Defer(func() { cur.Close(queryContext.Ctx) }) // close cursor when done

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	// unmarshal columns if none detected
	// otherwise this may error when creating a temp table with no columns
	if len(ds.Columns) == 0 {
		g.JSONConvert(opts["columns"], &ds.Columns)
	}

	return
}

// GetSchemas returns schemas
func (conn *MongoDBConn) GetSchemas() (data iop.Dataset, err error) {
	queryContext := g.NewContext(conn.Context().Ctx)
	res, err := conn.Client.ListDatabases(queryContext.Ctx, bson.D{})
	if err != nil {
		return data, g.Error(err, "could not list mongo databases")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	for _, db := range res.Databases {
		data.Append([]any{db.Name})
	}

	return data, nil
}

// GetSchemas returns schemas
func (conn *MongoDBConn) GetTables(schema string) (data iop.Dataset, err error) {
	queryContext := g.NewContext(conn.Context().Ctx)

	g.Trace("listing collections from database: %s", schema)
	names, err := conn.Client.Database(schema).ListCollectionNames(queryContext.Ctx, bson.D{})
	if err != nil {
		return data, g.Error(err, "could not list mongo collections in database %s", schema)
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))
	for _, name := range names {
		data.Append([]any{name})
	}

	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *MongoDBConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	currDatabase := dbio.TypeDbMongoDB.String()
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	schemaData, err := conn.GetSchemas()
	if err != nil {
		return schemata, g.Error(err, "Could not get databases")
	}

	schemas := map[string]Schema{}
	for _, schemaRow := range schemaData.Rows {
		schemaName := cast.ToString(schemaRow[0])

		tablesData, err := conn.GetTables(schemaName)
		if err != nil {
			return schemata, g.Error(err, "Could not get tables")
		}

		for _, tableRow := range tablesData.Rows {
			tableName := cast.ToString(tableRow[0])
			columnName := "data"
			dataType := "json"

			schema := Schema{
				Name:     schemaName,
				Database: currDatabase,
				Tables:   map[string]Table{},
			}

			if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
				schema = schemas[strings.ToLower(schema.Name)]
			}

			var table Table
			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				table = Table{
					Name:     tableName,
					Schema:   schemaName,
					Database: currDatabase,
					IsView:   false,
					Columns:  iop.Columns{},
					Dialect:  conn.GetType(),
				}

				if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
					table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
				}
			}

			if level == SchemataLevelColumn {
				column := iop.Column{
					Name:     columnName,
					Type:     NativeTypeToGeneral(columnName, dataType, conn),
					Table:    tableName,
					Schema:   schemaName,
					Database: currDatabase,
					Position: 1,
					DbType:   dataType,
				}

				table.Columns = append(table.Columns, column)

				if g.In(tableName, tableNames...) {
					columns, err := conn.GetSQLColumns(table)
					if err != nil {
						return schemata, g.Error(err, "could not get columns")
					}
					for i := range columns {
						columns[i].Database = table.Database
					}
					if len(columns) > 0 {
						table.Columns = columns
					}
				}
			}
			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				schema.Tables[strings.ToLower(tableName)] = table
			}
			schemas[strings.ToLower(schema.Name)] = schema
		}

		schemata.Databases[strings.ToLower(currDatabase)] = Database{
			Name:    currDatabase,
			Schemas: schemas,
		}
	}

	return schemata, nil
}
