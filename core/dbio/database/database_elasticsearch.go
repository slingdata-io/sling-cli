package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// ElasticsearchConn is a elasticsearch connection
type ElasticsearchConn struct {
	BaseConn
	URL    string
	Client *elasticsearch.Client
}

// Init initiates the object
func (conn *ElasticsearchConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbElasticsearch

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Init initiates the object
func (conn *ElasticsearchConn) getNewClient(timeOut ...int) (client *elasticsearch.Client, err error) {
	cfg := elasticsearch.Config{}

	// Handle Cloud ID + API Key auth
	if cloudID := conn.GetProp("cloud_id"); cloudID != "" {
		cfg.CloudID = cloudID
		if apiKey := conn.GetProp("api_key"); apiKey != "" {
			cfg.APIKey = apiKey
		}
	} else {
		// Handle HTTP URLs or default localhost
		if httpURLs := conn.GetProp("http_url"); httpURLs != "" {
			cfg.Addresses = strings.Split(httpURLs, ",")
		} else {
			host := conn.GetProp("host")
			port := conn.GetProp("port")
			cfg.Addresses = []string{g.F("http://%s:%s", host, port)}
		}

		// Handle Basic Auth
		if user := conn.GetProp("user", "username"); user != "" {
			cfg.Username = user
			cfg.Password = conn.GetProp("password")
		}

		// Handle Bearer Token
		if token := conn.GetProp("service_token"); token != "" {
			cfg.ServiceToken = token
		}
	}

	// Handle TLS config
	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return nil, g.Error(err)
	}
	if tlsConfig != nil {
		cfg.Transport = &http.Transport{TLSClientConfig: tlsConfig}
		// Update URLs to use HTTPS if using TLS
		for i, addr := range cfg.Addresses {
			if !strings.HasPrefix(addr, "https://") {
				cfg.Addresses[i] = strings.Replace(addr, "http://", "https://", 1)
			}
		}
	}

	client, err = elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, g.Error(err, "could not connect to Elasticsearch server")
	}

	// Test connection with timeout
	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(to)*time.Second)
	defer cancel()

	info, err := client.Info(
		client.Info.WithContext(ctx),
	)
	if err != nil {
		return nil, g.Error(err, "could not connect to Elasticsearch server")
	}
	defer info.Body.Close()

	return client, nil
}

// Connect connects to the database
func (conn *ElasticsearchConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

func (conn *ElasticsearchConn) Close() error {
	g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	return nil
}

// NewTransaction creates a new transaction
func (conn *ElasticsearchConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// Elasticsearch does not support transactions
	return nil, g.Error("transactions not supported in Elasticsearch")
}

// GetTableColumns returns columns for a table
func (conn *ElasticsearchConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	// Get mapping for the index
	mapping, err := conn.Client.Indices.GetMapping(
		conn.Client.Indices.GetMapping.WithIndex(table.Name),
	)
	if err != nil {
		return columns, g.Error(err, "could not get mapping for index %s", table.Name)
	}
	defer mapping.Body.Close()

	var mappingResponse map[string]interface{}
	if err := json.NewDecoder(mapping.Body).Decode(&mappingResponse); err != nil {
		return columns, g.Error(err, "could not decode mapping response")
	}

	// Navigate to properties
	indexMapping, ok := mappingResponse[table.Name].(map[string]interface{})
	if !ok {
		return columns, g.Error("unexpected mapping structure for index %s", table.Name)
	}

	mappings, ok := indexMapping["mappings"].(map[string]interface{})
	if !ok {
		return columns, g.Error("no mappings found for index %s", table.Name)
	}

	properties, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		// Try ES7+ structure where type is implicit
		if props, ok := mappings["_doc"].(map[string]interface{}); ok {
			properties = props["properties"].(map[string]interface{})
		} else {
			// If no properties found, return a single JSON column
			columns = append(columns, iop.Column{
				Name:     "data",
				Type:     iop.ColumnType("json"),
				Table:    table.Name,
				Schema:   table.Schema,
				Database: table.Database,
				Position: 1,
				DbType:   "json",
			})
			return columns, nil
		}
	}

	position := 1
	var processProperties func(prefix string, props map[string]interface{})
	processProperties = func(prefix string, props map[string]interface{}) {
		for fieldName, fieldDef := range props {
			fieldDefMap, ok := fieldDef.(map[string]interface{})
			if !ok {
				continue
			}

			fieldType, _ := fieldDefMap["type"].(string)

			// Handle nested objects
			if fieldType == "object" {
				if nestedProps, ok := fieldDefMap["properties"].(map[string]interface{}); ok {
					newPrefix := prefix
					if prefix != "" {
						newPrefix = prefix + "."
					}
					processProperties(newPrefix+fieldName, nestedProps)
				}
				continue
			}

			// Map ES types to general types
			var colType iop.ColumnType
			switch fieldType {
			case "text", "keyword", "string":
				colType = iop.TextType
			case "long", "integer":
				colType = iop.BigIntType
			case "float", "double":
				colType = iop.FloatType
			case "date":
				colType = iop.TimestampType
			case "boolean":
				colType = iop.BoolType
			case "binary":
				colType = iop.BinaryType
			default:
				colType = iop.TextType // default to text for unknown types
			}

			columnName := fieldName
			if prefix != "" {
				columnName = prefix + "." + fieldName
			}

			columns = append(columns, iop.Column{
				Name:     columnName,
				Type:     colType,
				Table:    table.Name,
				Schema:   table.Schema,
				Database: table.Database,
				Position: position,
				DbType:   fieldType,
			})
			position++
		}
	}

	processProperties("", properties)

	// If no columns were found, return a single JSON column
	if len(columns) == 0 {
		columns = append(columns, iop.Column{
			Name:     "data",
			Type:     iop.ColumnType("json"),
			Table:    table.Name,
			Schema:   table.Schema,
			Database: table.Database,
			Position: 1,
			DbType:   "json",
		})
	}

	return columns, nil
}

func (conn *ElasticsearchConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return nil, g.Error("ExecContext not implemented on ElasticSearch")
}

func (conn *ElasticsearchConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	options, _ := g.UnmarshalMap(table.SQL)
	ds, err := conn.StreamRowsContext(conn.Context().Ctx, table.Name, options)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return df, nil
}

func (conn *ElasticsearchConn) StreamRowsContext(ctx context.Context, tableName string, Opts ...map[string]interface{}) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(Opts)
	Limit := int64(0) // infinite
	if val := cast.ToInt64(opts["limit"]); val > 0 {
		Limit = val
	}

	// Handle incremental and backfill options
	var searchBody map[string]interface{}
	if updateKey := cast.ToString(opts["update_key"]); updateKey != "" {
		if incrementalValue := cast.ToString(opts["value"]); incrementalValue != "" {
			// Incremental mode
			searchBody = map[string]interface{}{
				"query": map[string]interface{}{
					"range": map[string]interface{}{
						updateKey: map[string]interface{}{
							"gt": incrementalValue,
						},
					},
				},
			}
		} else if startValue := cast.ToString(opts["start_value"]); startValue != "" {
			if endValue := cast.ToString(opts["end_value"]); endValue != "" {
				// Backfill mode
				searchBody = map[string]interface{}{
					"query": map[string]interface{}{
						"range": map[string]interface{}{
							updateKey: map[string]interface{}{
								"gte": startValue,
								"lte": endValue,
							},
						},
					},
				}
			}
		}
	}

	// If no specific query is provided, use match_all
	if searchBody == nil {
		searchBody = map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}

	// Add size if limit is specified
	if Limit > 0 {
		searchBody["size"] = Limit
	}

	// Create search request
	searchBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, g.Error(err, "could not marshal search body")
	}

	// Create the search request
	table, _ := ParseTableName(tableName, conn.Type)
	indexName := table.Name
	res, err := conn.Client.Search(
		conn.Client.Search.WithContext(ctx),
		conn.Client.Search.WithIndex(indexName),
		conn.Client.Search.WithBody(bytes.NewReader(searchBytes)),
		conn.Client.Search.WithScroll(time.Minute),
	)
	if err != nil {
		return nil, g.Error(err, "could not execute search")
	} else if res.StatusCode >= 400 {
		bytes, _ := io.ReadAll(res.Body)
		return nil, g.Error("could not execute search (status %s) => %s", res.StatusCode, string(bytes))
	}

	var searchResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, g.Error(err, "could not decode search response")
	}
	res.Body.Close()

	scrollID, ok := searchResponse["_scroll_id"].(string)
	if !ok {
		// no result, return empty datastream
		data := iop.NewDataset(iop.NewColumnsFromFields("data"))
		return data.Stream(), nil
	}

	// Create base datastream
	ds = iop.NewDatastreamContext(ctx, iop.Columns{})

	// Handle flattening option
	flatten := 0
	if val := conn.GetProp("flatten"); val != "" {
		flatten = cast.ToInt(val)
	}

	// Create a custom decoder for Elasticsearch scrolling
	decoder := &elasticDecoder{
		conn:           conn,
		ctx:            ctx,
		scrollID:       scrollID,
		searchResponse: searchResponse,
		limit:          cast.ToUint64(Limit),
		counter:        0,
	}

	// Create JSON stream iterator
	js := iop.NewJSONStream(ds, decoder, flatten, conn.GetProp("jmespath"))
	js.HasMapPayload = true

	// Set up the iterator
	it := ds.NewIterator(ds.Columns, js.NextFunc)
	ds.SetIterator(it)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	err = ds.Start()
	if err != nil {
		return ds, g.Error(err, "could not start datastream")
	}

	return ds, nil
}

// elasticDecoder implements the decoderLike interface for Elasticsearch scrolling
type elasticDecoder struct {
	conn           *ElasticsearchConn
	ctx            context.Context
	scrollID       string
	searchResponse map[string]interface{}
	hits           []interface{}
	currentHit     int
	limit          uint64
	counter        uint64
}

// Decode implements the decoderLike interface
func (d *elasticDecoder) Decode(obj interface{}) error {
	// Check context and limits
	if d.ctx.Err() != nil {
		return d.ctx.Err()
	}
	if d.limit > 0 && d.counter >= d.limit {
		return io.EOF
	}

	// Get next batch if needed
	if d.searchResponse == nil || (d.hits == nil || d.currentHit >= len(d.hits)) {
		if d.searchResponse == nil {
			// Get the next batch of results using the scroll ID
			res, err := d.conn.Client.Scroll(
				d.conn.Client.Scroll.WithContext(d.ctx),
				d.conn.Client.Scroll.WithScrollID(d.scrollID),
				d.conn.Client.Scroll.WithScroll(time.Minute),
			)
			if err != nil {
				return g.Error(err, "error scrolling results")
			}

			if err := json.NewDecoder(res.Body).Decode(&d.searchResponse); err != nil {
				res.Body.Close()
				return g.Error(err, "error decoding scroll response")
			}
			res.Body.Close()

			// Update scroll ID for next batch
			newScrollID, ok := d.searchResponse["_scroll_id"].(string)
			if !ok {
				return io.EOF
			}
			d.scrollID = newScrollID
		}

		// Get hits from response
		hits, ok := d.searchResponse["hits"].(map[string]interface{})
		if !ok {
			return g.Error("hits not found in response")
		}

		hitsArray, ok := hits["hits"].([]interface{})
		if !ok {
			return g.Error("hits array not found in response")
		}

		if len(hitsArray) == 0 {
			return io.EOF
		}

		d.hits = hitsArray
		d.currentHit = 0
		d.searchResponse = nil
	}

	// Get next hit
	hit, ok := d.hits[d.currentHit].(map[string]interface{})
	if !ok {
		d.currentHit++
		return d.Decode(obj) // skip invalid hit
	}

	source, ok := hit["_source"].(map[string]interface{})
	if !ok {
		d.currentHit++
		return d.Decode(obj) // skip invalid source
	}

	// Set the source as the object to decode
	objMap, ok := obj.(*map[string]interface{})
	if !ok {
		return g.Error("invalid object type for decoding")
	}
	*objMap = source

	d.currentHit++
	d.counter++
	return nil
}

// GetSchemas returns schemas
func (conn *ElasticsearchConn) GetSchemas() (data iop.Dataset, err error) {
	// In Elasticsearch, indices are similar to schemas/databases
	// We'll list all indices
	indices, err := conn.Client.Cat.Indices(
		conn.Client.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return data, g.Error(err, "could not list indices")
	}
	defer indices.Body.Close()

	var indicesResponse []map[string]interface{}
	if err := json.NewDecoder(indices.Body).Decode(&indicesResponse); err != nil {
		return data, g.Error(err, "could not decode indices response")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	for _, index := range indicesResponse {
		if indexName, ok := index["index"].(string); ok {
			data.Append([]interface{}{indexName})
		}
	}

	return data, nil
}

// GetTables returns tables
func (conn *ElasticsearchConn) GetTables(schema string) (data iop.Dataset, err error) {
	// In Elasticsearch, we can consider mappings as tables
	// For a given index (schema), get its mapping
	mapping, err := conn.Client.Indices.GetMapping(
		conn.Client.Indices.GetMapping.WithIndex(schema),
	)
	if err != nil {
		return data, g.Error(err, "could not get mapping for index %s", schema)
	}
	defer mapping.Body.Close()

	var mappingResponse map[string]interface{}
	if err := json.NewDecoder(mapping.Body).Decode(&mappingResponse); err != nil {
		return data, g.Error(err, "could not decode mapping response")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))
	// Each index has one mapping type in ES7+
	data.Append([]interface{}{"_doc"})

	return data, nil
}

// GetSchemata returns the database schemata
func (conn *ElasticsearchConn) GetSchemata(level SchemataLevel, schema string, tables ...string) (schemata Schemata, err error) {
	schemata = Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	// Get list of indices (schemas)
	indices, err := conn.Client.Cat.Indices(
		conn.Client.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return schemata, g.Error(err, "could not get indices")
	}
	defer indices.Body.Close()

	type indexInfo struct {
		Index string `json:"index"`
	}
	var indicesResponse []indexInfo
	if err := json.NewDecoder(indices.Body).Decode(&indicesResponse); err != nil {
		return schemata, g.Error(err, "could not decode indices response")
	}

	// Filter indices if schema is specified
	if schema != "" {
		filteredIndices := []indexInfo{}
		for _, idx := range indicesResponse {
			if idx.Index == schema {
				filteredIndices = append(filteredIndices, idx)
			}
		}
		indicesResponse = filteredIndices
	}

	// Create database entry
	database := Database{
		Name:    "default",
		Schemas: map[string]Schema{},
	}

	// Process each index
	for _, idx := range indicesResponse {
		// Skip system indices
		if strings.HasPrefix(idx.Index, ".") {
			continue
		}

		// Create schema entry
		schema := Schema{
			Name:     idx.Index,
			Database: database.Name,
			Tables:   map[string]Table{},
		}

		// Create table entry (in ES, index is both schema and table)
		table := Table{
			Name:     idx.Index,
			Schema:   idx.Index,
			Database: database.Name,
			Dialect:  dbio.TypeDbElasticsearch,
		}

		// Get columns if requested
		if level == SchemataLevelColumn {
			table.Columns, err = conn.GetTableColumns(&table)
			if err != nil {
				g.Warn("could not get columns for index %s: %s", idx.Index, err)
				continue
			}
		}

		schema.Tables[strings.ToLower(table.Name)] = table
		database.Schemas[strings.ToLower(schema.Name)] = schema
	}

	schemata.Databases[strings.ToLower(database.Name)] = database
	return schemata, nil
}
