package database

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

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

	var instance Connection
	instance = conn
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

	opts := options.Client().SetCompressors([]string{"zstd", "snappy", "zlib"})
	return mongo.Connect(ctx, options.Client().ApplyURI(conn.URL), opts)
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
		return g.Error(err, "Failed to ping mongo server")
	}

	return nil
}

func (conn *MongoDBConn) Close() error {
	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, 5*time.Second)
	defer cancel()
	err := conn.Client.Disconnect(ctx)
	if err != nil {
		return g.Error(err, "Failed to disconnect")
	}
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
		return columns, g.Error("could not query to get tables")
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

	ds, err := conn.StreamRows(table.FullName(), g.M("limit", 10))
	if err != nil {
		return columns, g.Error("could not query to get columns")
	}

	data, err := ds.Collect(10)
	if err != nil {
		return columns, g.Error("could not collect to get columns")
	}

	return data.Columns, nil
}

func (conn *MongoDBConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return nil, g.Error("ExecContext not implemented on MongoConn")
}

func (conn *MongoDBConn) BulkExportFlow(tables ...Table) (df *iop.Dataflow, err error) {
	if len(tables) == 0 {
		return
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, tables[0].FullName())
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

func (conn *MongoDBConn) StreamRowsContext(ctx context.Context, collectionName string, Opts ...map[string]interface{}) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(Opts)
	Limit := int64(0) // infinite
	if val := cast.ToInt64(opts["limit"]); val > 0 {
		Limit = val
	}

	if strings.TrimSpace(collectionName) == "" {
		g.Warn("Empty collection name")
		return ds, nil
	}

	queryContext := g.NewContext(ctx)

	table, _ := ParseTableName(collectionName, conn.Type)

	collection := conn.Client.Database(table.Schema).Collection(table.Name)

	cur, err := collection.Find(queryContext.Ctx, bson.D{}, &options.FindOptions{Limit: &Limit})
	if err != nil {
		return ds, g.Error(err, "error querying collection")
	}

	ds = iop.NewDatastreamContext(queryContext.Ctx, nil)
	// js := iop.NewJSONStream(ds, cur, true, conn.GetProp("jmespath"))
	js := iop.NewJSONStream(ds, cur, true, conn.GetProp("jmespath"))
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
				// fix Object ID
				it.Row[0] = strings.TrimSuffix(
					strings.TrimPrefix(
						cast.ToString(it.Row[0]), `ObjectID("`,
					), `")`,
				)

				return true
			}
		}
		return false
	}

	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))
	ds.NoDebug = strings.Contains(collectionName, noDebugKey)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
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
		data.Append([]interface{}{db.Name})
	}

	return data, nil
}

// GetSchemas returns schemas
func (conn *MongoDBConn) GetTables(schema string) (data iop.Dataset, err error) {
	queryContext := g.NewContext(conn.Context().Ctx)

	names, err := conn.Client.Database(schema).ListCollectionNames(queryContext.Ctx, bson.D{})
	if err != nil {
		return data, g.Error(err, "could not list mongo collections in database %s", schema)
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))
	for _, name := range names {
		data.Append([]interface{}{name})
	}

	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *MongoDBConn) GetSchemata(schemaName string, tableNames ...string) (Schemata, error) {
	currDatabase := "mongo"
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	schemaData, err := conn.GetSchemas()
	if err != nil {
		return schemata, g.Error(err, "Could not get databases")
	}

	for _, schemaRow := range schemaData.Rows {
		schemaName := cast.ToString(schemaRow[0])

		tablesData, err := conn.GetTables(schemaName)
		if err != nil {
			return schemata, g.Error(err, "Could not get tables")
		}

		schemas := map[string]Schema{}
		for _, tableRow := range tablesData.Rows {
			tableName := cast.ToString(tableRow[0])
			columnName := "data"
			dataType := "json"

			schema := Schema{
				Name:   schemaName,
				Tables: map[string]Table{},
			}

			table := Table{
				Name:     tableName,
				Schema:   schemaName,
				Database: currDatabase,
				IsView:   false,
				Columns:  iop.Columns{},
				Dialect:  conn.GetType(),
			}

			if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
				schema = schemas[strings.ToLower(schema.Name)]
			}

			if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
				table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
			}

			column := iop.Column{
				Name:     columnName,
				Type:     iop.ColumnType(conn.template.NativeTypeMap[dataType]),
				Table:    tableName,
				Schema:   schemaName,
				Database: currDatabase,
				Position: 1,
				DbType:   dataType,
			}

			table.Columns = append(table.Columns, column)
			schema.Tables[strings.ToLower(tableName)] = table
			schemas[strings.ToLower(schema.Name)] = schema
		}

		schemata.Databases[strings.ToLower(currDatabase)] = Database{
			Name:    currDatabase,
			Schemas: schemas,
		}
	}

	return schemata, nil
}
