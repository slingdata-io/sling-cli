package database

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// PrometheusConn is a Prometheus connection
type PrometheusConn struct {
	BaseConn
	URL    string
	Client v1.API
}

// Init initiates the object
func (conn *PrometheusConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbPrometheus

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Init initiates the client
func (conn *PrometheusConn) getNewClient(timeOut ...int) (client v1.API, err error) {

	rt := api.DefaultRoundTripper

	// get tls
	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return nil, g.Error(err)
	} else if tlsConfig != nil {
		rt = &http.Transport{TLSClientConfig: tlsConfig}
	}

	if token := conn.GetProp("token"); token != "" {
		rt = config.NewAuthorizationCredentialsRoundTripper("Bearer", config.NewInlineSecret(token), rt)
	}

	if user := conn.GetProp("user"); user != "" {
		rt = config.NewBasicAuthRoundTripper(config.NewInlineSecret(user), config.NewInlineSecret(conn.GetProp("password")), rt)
	}

	if tenant := conn.GetProp("tenant"); tenant != "" {
		rt = config.NewHeadersRoundTripper(&config.Headers{Headers: map[string]config.Header{"X-Scope-OrgID": {Values: []string{tenant}}}}, rt)
	}

	c, err := api.NewClient(api.Config{
		Address:      conn.GetProp("http_url"),
		RoundTripper: rt,
	})
	if err != nil {
		return nil, g.Error(err, "could not connect to Prometheus server")
	}

	client = v1.NewAPI(c)

	return
}

// Connect connects to the database
func (conn *PrometheusConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to get client")
	}

	_, err = conn.Client.Buildinfo(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))

	return nil
}

func (conn *PrometheusConn) Close() error {
	g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	return nil
}

// NewTransaction creates a new transaction
func (conn *PrometheusConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// does not support transaction
	return
}
func (conn *PrometheusConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	// For Prometheus, we need to examine the query to determine expected columns
	// Default columns for time series data
	columns = iop.Columns{
		{Name: "__name__", Type: iop.StringType, Position: 1},
		{Name: "job", Type: iop.StringType, Position: 2},
		{Name: "instance", Type: iop.StringType, Position: 3},
		{Name: "timestamp", Type: iop.BigIntType, Position: 4},
		{Name: "value", Type: iop.DecimalType, Position: 5},
	}

	// Try to get actual columns by querying with a very small time range
	if table.SQL != "" {
		// Extract the base query without options
		baseQuery := table.SQL
		if idx := strings.Index(baseQuery, "#"); idx != -1 {
			baseQuery = strings.TrimSpace(baseQuery[:idx])
		}

		// Query with a minimal time range to get column structure
		testOpts := g.M(
			"start", "now-1m",
			"end", "now",
			"step", "1m",
			"limit", 1,
			"get_columns", true,
		)

		ds, err := conn.StreamRowsContext(conn.Context().Ctx, baseQuery, testOpts)
		if err == nil && ds != nil {
			// Wait for columns to be initialized
			ds.WaitReady()
			if len(ds.Columns) > 0 {
				columns = ds.Columns
			}
			ds.Close()
		}
	}

	return columns, nil
}

// NewTransaction creates a new transaction
func (conn *PrometheusConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
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

	ds, err := conn.StreamRows(table.FullName(), g.M("limit", 10, "silent", true, "get_columns", true))
	if err != nil {
		return columns, g.Error("could not query to get columns")
	}

	data, err := ds.Collect(10)
	if err != nil {
		return columns, g.Error("could not collect to get columns")
	}

	return data.Columns, nil
}

func (conn *PrometheusConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return nil, g.Error("ExecContext not implemented on PrometheusConn")
}

func (conn *PrometheusConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	// parse options
	options := g.M()
	if parts := strings.Split(table.SQL, `#`); len(parts) > 1 {
		lastPart := parts[len(parts)-1]
		g.Unmarshal(lastPart, &options)
		g.Debug("query options: %s", g.Marshal(options))
	}

	// Pass table columns for fallback when no data is returned
	if len(table.Columns) > 0 {
		options["columns"] = table.Columns
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, table.SQL, options)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

func (conn *PrometheusConn) StreamRowsContext(ctx context.Context, query string, Opts ...map[string]interface{}) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(Opts)
	Limit := int(0) // infinite
	if val := cast.ToInt(opts["limit"]); val > 0 {
		Limit = val
	}

	if strings.TrimSpace(query) == "" {
		g.Warn("Empty query")
		return ds, nil
	}

	queryContext := g.NewContext(ctx)

	// Check if we should use chunked streaming for large time ranges
	if strings.Contains(query, "#") {
		queryOpts := map[string]string{}
		if queryOptArr := strings.Split(query, "#"); len(queryOptArr) == 2 {
			err = g.Unmarshal(queryOptArr[1], &queryOpts)
			if err == nil {
				// Parse time range to determine if chunking is needed
				start := time.Now().Add(-30 * time.Minute)
				if val, ok := queryOpts["start"]; ok {
					if strings.HasPrefix(val, "now") && strings.Contains(val, "-") {
						duration, _ := time.ParseDuration(strings.TrimPrefix(val, "now"))
						start = time.Now().Add(duration)
					} else if !strings.HasPrefix(val, "now") {
						start, _ = time.Parse(time.RFC3339, val)
					}
				}

				end := time.Now()
				if val, ok := queryOpts["end"]; ok {
					if strings.HasPrefix(val, "now") && strings.Contains(val, "-") {
						duration, _ := time.ParseDuration(strings.TrimPrefix(val, "now"))
						end = time.Now().Add(duration)
					} else if !strings.HasPrefix(val, "now") {
						end, _ = time.Parse(time.RFC3339, val)
					}
				}

				// If time range is more than 1 hour, use chunked streaming
				if end.Sub(start) > time.Hour {
					g.Debug("using chunked streaming for Prometheus query (range: %s)", end.Sub(start))
					return conn.StreamRowsChunked(queryContext, query, opts)
				}
			}
		}
	}

	start := time.Now().Add(-24 * 30 * time.Hour)
	end := time.Now()

	toHourDuration := func(duration string) string {
		switch {
		case strings.HasSuffix(duration, "d"):
			num := cast.ToInt(strings.TrimSuffix(duration, "d"))
			duration = g.F("%dh", num*24)
		case strings.HasSuffix(duration, "w"):
			num := cast.ToInt(strings.TrimSuffix(duration, "w"))
			duration = g.F("%dh", num*24*7)
		case strings.HasSuffix(duration, "M"):
			num := cast.ToInt(strings.TrimSuffix(duration, "M"))
			duration = g.F("%dh", num*24*31)
		}
		return duration
	}

	if startVal := cast.ToString(opts["start"]); strings.HasPrefix(startVal, "now-") {
		duration := strings.TrimPrefix(startVal, "now-")
		delta, err := time.ParseDuration(toHourDuration(duration))
		if err != nil {
			return nil, g.Error(err, "could not parse duration from start: %s", startVal)
		}
		start = time.Now().Add(-1 * delta)
	} else if opts["start"] != nil && opts["start"] != "now" {
		start, err = cast.ToTimeE(opts["start"])
		if err != nil {
			return nil, g.Error(err, "could not parse start value: %s", opts["start"])
		}
	}

	if endVal := cast.ToString(opts["end"]); strings.HasPrefix(endVal, "now-") {
		duration := strings.TrimPrefix(endVal, "now-")
		delta, err := time.ParseDuration(toHourDuration(duration))
		if err != nil {
			return nil, g.Error(err, "could not parse duration from end: %s", endVal)
		}
		end = time.Now().Add(-1 * delta)
	} else if opts["end"] != nil && opts["end"] != "now" {
		end, err = cast.ToTimeE(opts["end"])
		if err != nil {
			return nil, g.Error(err, "could not parse end value: %s", opts["end"])
		}
	}

	step := time.Hour
	if opts["step"] != nil {
		step, err = time.ParseDuration(toHourDuration(cast.ToString(opts["step"])))
		if err != nil {
			return nil, g.Error(err, "could not parse step duration: %s", opts["step"])
		}
	}

	// Initialize datastream
	ds = iop.NewDatastreamContext(queryContext.Ctx, iop.Columns{})
	ds.SetConfig(conn.Props())

	// State variables for the iterator closure
	var result model.Value
	var resultFetched bool
	var warnings []string
	var matrix model.Matrix
	var vector model.Vector
	var matrixIndex, valueIndex, histogramIndex, bucketIndex int
	var currentSample *model.SampleStream
	var columnsInitialized bool
	var fieldMap map[string]int
	limit := uint64(Limit)

	// Create the nextFunc closure
	nextFunc := func(it *iop.Iterator) bool {
		// Check context cancellation
		if it.Context.Err() != nil {
			return false
		}

		// Check limit
		if Limit > 0 && it.Counter >= limit {
			return false
		}

		// Initial query execution (only once)
		if !resultFetched {
			Range := v1.Range{
				Start: start,
				End:   end,
				Step:  step,
			}
			g.Debug("using range %s", g.Marshal(Range))

			result, warnings, err = conn.Client.QueryRange(queryContext.Ctx, query, Range)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "Error querying Prometheus: %s", query))
				return false
			}

			for _, warning := range warnings {
				g.Warn(warning)
			}

			resultFetched = true

			// Type switch to initialize the appropriate variables
			switch v := result.(type) {
			case model.Matrix:
				matrix = v
				matrixIndex = 0
			case model.Vector:
				vector = v
				matrixIndex = 0
			case *model.Scalar:
				// Handle scalar - single row
				if !columnsInitialized {
					ds.Columns = iop.Columns{
						{Name: "timestamp", Type: iop.BigIntType, Position: 1},
						{Name: "value", Type: iop.DecimalType, Position: 2},
					}
					it.Row = make([]any, len(ds.Columns))
					columnsInitialized = true
				}
				it.Row[0] = v.Timestamp.Unix()
				it.Row[1] = cast.ToFloat64(v.Value)
				result = nil // Mark as processed
				return true
			case *model.String:
				// Handle string - single row
				if !columnsInitialized {
					ds.Columns = iop.Columns{
						{Name: "timestamp", Type: iop.BigIntType, Position: 1},
						{Name: "value", Type: iop.StringType, Position: 2},
					}
					it.Row = make([]any, len(ds.Columns))
					columnsInitialized = true
				}
				it.Row[0] = v.Timestamp.Unix()
				it.Row[1] = cast.ToString(v.Value)
				result = nil // Mark as processed
				return true
			default:
				it.Context.CaptureErr(g.Error("invalid result: %#v", result))
				return false
			}
		}

		// Process Matrix results
		if matrix != nil {
			// Find next value to return
			for matrixIndex < len(matrix) {
				if currentSample == nil {
					currentSample = matrix[matrixIndex]
					valueIndex = 0
					histogramIndex = 0
					bucketIndex = 0

					// Initialize columns on first sample
					if !columnsInitialized {
						metricMap := map[string]string{}
						g.Unmarshal(g.Marshal(currentSample.Metric), &metricMap)

						labels := lo.Keys(metricMap)
						sort.Strings(labels)
						columns := iop.NewColumnsFromFields(labels...)

						if len(currentSample.Histograms) > 0 {
							// Add histogram columns
							columns = append(columns,
								iop.Column{Name: "timestamp", Type: iop.BigIntType, Position: len(columns) + 1},
								iop.Column{Name: "count", Type: iop.DecimalType, Position: len(columns) + 1},
								iop.Column{Name: "sum", Type: iop.DecimalType, Position: len(columns) + 1},
								iop.Column{Name: "bucket_boundaries", Type: iop.IntegerType, Position: len(columns) + 1},
								iop.Column{Name: "bucket_count", Type: iop.DecimalType, Position: len(columns) + 1},
								iop.Column{Name: "bucket_lower", Type: iop.DecimalType, Position: len(columns) + 1},
								iop.Column{Name: "bucket_upper", Type: iop.DecimalType, Position: len(columns) + 1},
							)
						} else {
							columns = append(columns,
								iop.Column{Name: "timestamp", Type: iop.BigIntType, Position: len(columns) + 1},
								iop.Column{Name: "value", Type: iop.DecimalType, Position: len(columns) + 1},
							)
						}

						ds.Columns = columns
						it.Row = make([]any, len(columns))
						fieldMap = ds.Columns.FieldMap(true)
						columnsInitialized = true
					}
				}

				metricMap := map[string]string{}
				g.Unmarshal(g.Marshal(currentSample.Metric), &metricMap)

				// Process regular values
				if valueIndex < len(currentSample.Values) {
					value := currentSample.Values[valueIndex]

					// Fill row
					for k, v := range metricMap {
						if idx, ok := fieldMap[strings.ToLower(k)]; ok {
							it.Row[idx] = v
						}
					}
					it.Row[fieldMap["timestamp"]] = value.Timestamp.Unix()
					it.Row[fieldMap["value"]] = float64(value.Value)

					valueIndex++
					return true
				}

				// Process histogram values
				if histogramIndex < len(currentSample.Histograms) {
					hist := currentSample.Histograms[histogramIndex]

					if bucketIndex < len(hist.Histogram.Buckets) {
						bucket := hist.Histogram.Buckets[bucketIndex]

						// Fill row
						for k, v := range metricMap {
							if idx, ok := fieldMap[strings.ToLower(k)]; ok {
								it.Row[idx] = v
							}
						}
						it.Row[fieldMap["timestamp"]] = hist.Timestamp.Unix()
						it.Row[fieldMap["count"]] = cast.ToFloat64(hist.Histogram.Count)
						it.Row[fieldMap["sum"]] = cast.ToFloat64(hist.Histogram.Sum)
						it.Row[fieldMap["bucket_boundaries"]] = cast.ToInt(bucket.Boundaries)
						it.Row[fieldMap["bucket_count"]] = cast.ToFloat64(bucket.Count)
						it.Row[fieldMap["bucket_lower"]] = cast.ToFloat64(bucket.Lower)
						it.Row[fieldMap["bucket_upper"]] = cast.ToFloat64(bucket.Upper)

						bucketIndex++
						return true
					}

					bucketIndex = 0
					histogramIndex++
					if histogramIndex < len(currentSample.Histograms) {
						continue // Process next histogram
					}
				}

				// Move to next sample
				currentSample = nil
				matrixIndex++
			}

			// No more data in matrix
			matrix = nil
		}

		// Process Vector results
		if vector != nil {
			if matrixIndex < len(vector) {
				sample := vector[matrixIndex]

				// Initialize columns on first sample
				if !columnsInitialized {
					metricMap := map[string]string{}
					g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

					labels := lo.Keys(metricMap)
					sort.Strings(labels)
					columns := iop.NewColumnsFromFields(labels...)

					if sample.Histogram != nil {
						// Add histogram columns
						columns = append(columns,
							iop.Column{Name: "timestamp", Type: iop.BigIntType, Position: len(columns) + 1},
							iop.Column{Name: "count", Type: iop.DecimalType, Position: len(columns) + 1},
							iop.Column{Name: "sum", Type: iop.DecimalType, Position: len(columns) + 1},
							iop.Column{Name: "bucket_boundaries", Type: iop.IntegerType, Position: len(columns) + 1},
							iop.Column{Name: "bucket_count", Type: iop.DecimalType, Position: len(columns) + 1},
							iop.Column{Name: "bucket_lower", Type: iop.DecimalType, Position: len(columns) + 1},
							iop.Column{Name: "bucket_upper", Type: iop.DecimalType, Position: len(columns) + 1},
						)
					} else {
						columns = append(columns,
							iop.Column{Name: "timestamp", Type: iop.BigIntType, Position: len(columns) + 1},
							iop.Column{Name: "value", Type: iop.DecimalType, Position: len(columns) + 1},
						)
					}

					ds.Columns = columns
					it.Row = make([]any, len(columns))
					fieldMap = ds.Columns.FieldMap(true)
					columnsInitialized = true
				}

				metricMap := map[string]string{}
				g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

				// Fill row
				for k, v := range metricMap {
					if idx, ok := fieldMap[strings.ToLower(k)]; ok {
						it.Row[idx] = v
					}
				}

				if sample.Histogram != nil {
					it.Row[fieldMap["timestamp"]] = sample.Timestamp.Unix()
					it.Row[fieldMap["count"]] = cast.ToFloat64(sample.Histogram.Count)
					it.Row[fieldMap["sum"]] = cast.ToFloat64(sample.Histogram.Sum)

					// Note: simplified - only processing first bucket for vector
					if len(sample.Histogram.Buckets) > 0 {
						bucket := sample.Histogram.Buckets[0]
						it.Row[fieldMap["bucket_boundaries"]] = cast.ToInt(bucket.Boundaries)
						it.Row[fieldMap["bucket_count"]] = cast.ToFloat64(bucket.Count)
						it.Row[fieldMap["bucket_lower"]] = cast.ToFloat64(bucket.Lower)
						it.Row[fieldMap["bucket_upper"]] = cast.ToFloat64(bucket.Upper)
					}
				} else {
					it.Row[fieldMap["timestamp"]] = sample.Timestamp.Unix()
					it.Row[fieldMap["value"]] = cast.ToFloat64(sample.Value)
				}

				matrixIndex++
				return true
			}

			// No more data in vector
			vector = nil
		}

		// Handle no data case - use fallback columns
		if !columnsInitialized && opts["columns"] != nil {
			switch cols := opts["columns"].(type) {
			case iop.Columns:
				ds.Columns = cols
			default:
				g.JSONConvert(opts["columns"], &ds.Columns)
			}
			if len(ds.Columns) > 0 {
				it.Row = make([]any, len(ds.Columns))
				columnsInitialized = true
			}
		}

		return false // No more data
	}

	// Set the iterator
	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))

	// Start the stream
	err = ds.Start()
	if err != nil {
		return ds, g.Error(err, "could not start datastream")
	}

	return ds, nil
}

// GetSchemas returns schemas
func (conn *PrometheusConn) GetSchemas() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]interface{}{"prometheus"})
	return data, nil
}

// GetSchemas returns schemas
func (conn *PrometheusConn) GetTables(schema string) (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))
	data.Append([]interface{}{"prometheus"})
	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *PrometheusConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	database := conn.Type.String()
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	queryContext := g.NewContext(conn.Context().Ctx)
	metadataMap, err := conn.Client.Metadata(queryContext.Ctx, "", "")
	if err != nil {
		return schemata, g.Error(err, "Could not get metadata")
	}

	_ = schemaName

	schemas := map[string]Schema{}
	schemaName = conn.Type.String()
	tableName := conn.Type.String()

	schema := Schema{
		Name:   schemaName,
		Tables: map[string]Table{},
	}

	table := Table{
		Name:     tableName,
		Schema:   schemaName,
		Database: database,
		IsView:   false,
		Columns:  iop.Columns{},
		Dialect:  conn.GetType(),
	}

	if level == SchemataLevelColumn {
		for key, metadata := range metadataMap {
			columnName := key
			colType := ""
			colDescription := ""
			if len(metadata) > 0 {
				colType = string(metadata[0].Type)
				colDescription = string(metadata[0].Help)
			}

			column := iop.Column{
				Name:        columnName,
				Type:        iop.BigIntType,
				Table:       tableName,
				Schema:      schemaName,
				Database:    database,
				Position:    len(table.Columns) + 1,
				DbType:      colType,
				Description: colDescription,
			}

			table.Columns = append(table.Columns, column)
		}

		sort.Slice(table.Columns, func(i, j int) bool {
			return table.Columns[i].Name < table.Columns[j].Name
		})

		// set position after sorting
		for i := range table.Columns {
			table.Columns[i].Position = i + 1
		}
	}

	if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
		schema.Tables[strings.ToLower(tableName)] = table
	}
	schemas[strings.ToLower(schema.Name)] = schema

	schemata.Databases[strings.ToLower(database)] = Database{
		Name:    database,
		Schemas: schemas,
	}

	return schemata, nil
}

// StreamRowsChunked implements chunked streaming for Prometheus to avoid memory issues
func (conn *PrometheusConn) StreamRowsChunked(queryContext *g.Context, query string, opts map[string]interface{}) (ds *iop.Datastream, err error) {
	// Parse options
	queryOpts := map[string]string{}
	if queryOptArr := strings.Split(query, "#"); len(queryOptArr) == 2 {
		err = g.Unmarshal(queryOptArr[1], &queryOpts)
		if err != nil {
			return nil, g.Error(err, "could not parse query options")
		}
		query = queryOptArr[0]
	}

	// Parse time range
	start := time.Now().Add(-30 * time.Minute)
	if val, ok := queryOpts["start"]; ok {
		if strings.HasPrefix(val, "now") {
			if strings.Contains(val, "-") {
				duration, err := time.ParseDuration(strings.TrimPrefix(val, "now"))
				if err != nil {
					return nil, g.Error(err, "could not parse start duration: %s", val)
				}
				start = time.Now().Add(duration)
			} else {
				start = time.Now()
			}
		} else {
			start, err = time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, g.Error(err, "could not parse start time: %s", val)
			}
		}
	}

	end := time.Now()
	if val, ok := queryOpts["end"]; ok {
		if strings.HasPrefix(val, "now") {
			if strings.Contains(val, "-") {
				duration, err := time.ParseDuration(strings.TrimPrefix(val, "now"))
				if err != nil {
					return nil, g.Error(err, "could not parse end duration: %s", val)
				}
				end = time.Now().Add(duration)
			} else {
				end = time.Now()
			}
		} else {
			end, err = time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, g.Error(err, "could not parse end time: %s", val)
			}
		}
	}

	step := 1 * time.Minute
	if val, ok := queryOpts["step"]; ok {
		step, err = time.ParseDuration(val)
		if err != nil {
			return nil, g.Error(err, "could not parse step duration: %s", val)
		}
	}

	// Calculate chunk size - max 1 hour per chunk to avoid memory issues
	chunkDuration := 1 * time.Hour
	if totalDuration := end.Sub(start); totalDuration < chunkDuration {
		// If total duration is less than chunk size, use original method
		return conn.StreamRowsContext(queryContext.Ctx, query, opts)
	}

	// Initialize datastream
	ds = iop.NewDatastreamContext(queryContext.Ctx, iop.Columns{})
	ds.SetConfig(conn.Props())

	// State variables for chunked iteration
	chunkStart := start
	chunkNum := 0
	var currentChunkResult model.Value
	var currentMatrix model.Matrix
	var currentVector model.Vector
	var matrixIndex, valueIndex, histogramIndex, bucketIndex int
	var currentSample *model.SampleStream
	var columnsInitialized bool
	var fieldMap map[string]int

	// Create the nextFunc closure for chunked streaming
	nextFunc := func(it *iop.Iterator) bool {
		// Check context cancellation
		if it.Context.Err() != nil {
			return false
		}

		// Main processing loop
		for {
			// Need to fetch a new chunk?
			if currentChunkResult == nil && chunkStart.Before(end) {
				chunkEnd := chunkStart.Add(chunkDuration)
				if chunkEnd.After(end) {
					chunkEnd = end
				}

				chunkNum++
				g.Debug("Processing chunk %d: %s to %s", chunkNum, chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))

				chunkRange := v1.Range{
					Start: chunkStart,
					End:   chunkEnd,
					Step:  step,
				}

				result, warnings, err := conn.Client.QueryRange(queryContext.Ctx, query, chunkRange)
				if err != nil {
					it.Context.CaptureErr(g.Error(err, "Error querying Prometheus chunk: %s", query))
					return false
				}

				for _, warning := range warnings {
					g.Warn(warning)
				}

				// Initialize based on result type
				switch v := result.(type) {
				case model.Matrix:
					currentMatrix = v
					matrixIndex = 0
					currentChunkResult = result
				case model.Vector:
					currentVector = v
					matrixIndex = 0
					currentChunkResult = result
				default:
					// For scalar/string types, handle immediately
					it.Context.CaptureErr(g.Error("unexpected result type in chunk: %#v", result))
					return false
				}

				// Move chunk window forward
				chunkStart = chunkEnd

				// Add small delay between chunks
				time.Sleep(100 * time.Millisecond)
			}

			// Process current chunk's Matrix data
			if currentMatrix != nil {
				for matrixIndex < len(currentMatrix) {
					if currentSample == nil {
						currentSample = currentMatrix[matrixIndex]
						valueIndex = 0
						histogramIndex = 0
						bucketIndex = 0

						// Initialize columns on first data
						if !columnsInitialized {
							metricMap := map[string]string{}
							g.Unmarshal(g.Marshal(currentSample.Metric), &metricMap)

							columns := createPrometheusColumns(metricMap, currentSample)
							ds.Columns = columns
							it.Row = make([]any, len(columns))
							fieldMap = columns.FieldMap(true)
							columnsInitialized = true
						}
					}

					metricMap := map[string]string{}
					g.Unmarshal(g.Marshal(currentSample.Metric), &metricMap)

					// Process values
					if valueIndex < len(currentSample.Values) {
						value := currentSample.Values[valueIndex]

						// Fill row
						for k, v := range metricMap {
							if idx, ok := fieldMap[strings.ToLower(k)]; ok {
								it.Row[idx] = v
							}
						}
						it.Row[fieldMap["timestamp"]] = value.Timestamp.Unix()
						it.Row[fieldMap["value"]] = float64(value.Value)

						valueIndex++
						return true
					}

					// Process histograms
					if histogramIndex < len(currentSample.Histograms) {
						hist := currentSample.Histograms[histogramIndex]

						if bucketIndex < len(hist.Histogram.Buckets) {
							bucket := hist.Histogram.Buckets[bucketIndex]

							// Fill row
							for k, v := range metricMap {
								if idx, ok := fieldMap[strings.ToLower(k)]; ok {
									it.Row[idx] = v
								}
							}
							it.Row[fieldMap["timestamp"]] = hist.Timestamp.Unix()
							it.Row[fieldMap["count"]] = hist.Histogram.Count
							it.Row[fieldMap["sum"]] = hist.Histogram.Sum
							it.Row[fieldMap["bucket_boundaries"]] = bucketIndex
							it.Row[fieldMap["bucket_count"]] = bucket.Count
							it.Row[fieldMap["bucket_lower"]] = bucket.Lower
							it.Row[fieldMap["bucket_upper"]] = bucket.Upper

							bucketIndex++
							return true
						}

						bucketIndex = 0
						histogramIndex++
						continue // Process next histogram
					}

					// Move to next sample in matrix
					currentSample = nil
					matrixIndex++
				}

				// Finished current matrix chunk
				currentMatrix = nil
				currentChunkResult = nil
				continue // Fetch next chunk
			}

			// Process current chunk's Vector data
			if currentVector != nil {
				if matrixIndex < len(currentVector) {
					sample := currentVector[matrixIndex]

					// Initialize columns on first data
					if !columnsInitialized {
						metricMap := map[string]string{}
						g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

						columns := createPrometheusColumnsFromVector(metricMap, sample)
						ds.Columns = columns
						it.Row = make([]any, len(columns))
						fieldMap = columns.FieldMap(true)
						columnsInitialized = true
					}

					metricMap := map[string]string{}
					g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

					// Fill row
					for k, v := range metricMap {
						if idx, ok := fieldMap[strings.ToLower(k)]; ok {
							it.Row[idx] = v
						}
					}

					if sample.Histogram != nil {
						it.Row[fieldMap["timestamp"]] = sample.Timestamp.Unix()
						it.Row[fieldMap["count"]] = cast.ToFloat64(sample.Histogram.Count)
						it.Row[fieldMap["sum"]] = cast.ToFloat64(sample.Histogram.Sum)

						if len(sample.Histogram.Buckets) > 0 {
							bucket := sample.Histogram.Buckets[0]
							it.Row[fieldMap["bucket_boundaries"]] = cast.ToInt(bucket.Boundaries)
							it.Row[fieldMap["bucket_count"]] = cast.ToFloat64(bucket.Count)
							it.Row[fieldMap["bucket_lower"]] = cast.ToFloat64(bucket.Lower)
							it.Row[fieldMap["bucket_upper"]] = cast.ToFloat64(bucket.Upper)
						}
					} else {
						it.Row[fieldMap["timestamp"]] = sample.Timestamp.Unix()
						it.Row[fieldMap["value"]] = cast.ToFloat64(sample.Value)
					}

					matrixIndex++
					return true
				}

				// Finished current vector chunk
				currentVector = nil
				currentChunkResult = nil
				continue // Fetch next chunk
			}

			// No more chunks and no current data
			if chunkStart.After(end) || chunkStart.Equal(end) {
				// Handle empty result with fallback columns
				if !columnsInitialized && opts["columns"] != nil {
					switch cols := opts["columns"].(type) {
					case iop.Columns:
						ds.Columns = cols
					default:
						g.JSONConvert(opts["columns"], &ds.Columns)
					}
					if len(ds.Columns) > 0 {
						columnsInitialized = true
						it.Row = make([]any, len(ds.Columns))
					}
				}
				return false // No more data
			}
		}
	}

	// Set the iterator
	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))

	// Start the stream
	err = ds.Start()
	if err != nil {
		return ds, g.Error(err, "could not start datastream")
	}

	return ds, nil
}

// Helper function to create columns for matrix data
func createPrometheusColumns(metricMap map[string]string, sample *model.SampleStream) iop.Columns {
	labels := lo.Keys(metricMap)
	sort.Strings(labels)

	columns := iop.NewColumnsFromFields(labels...)

	if len(sample.Histograms) > 0 {
		columns = append(columns, iop.Column{
			Name:     "timestamp",
			Type:     iop.BigIntType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "count",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "sum",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_boundaries",
			Type:     iop.IntegerType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_count",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_lower",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_upper",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
	} else {
		columns = append(columns, iop.Column{
			Name:     "timestamp",
			Type:     iop.BigIntType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "value",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
	}

	return columns
}

// Helper function to create columns for vector data
func createPrometheusColumnsFromVector(metricMap map[string]string, sample *model.Sample) iop.Columns {
	labels := lo.Keys(metricMap)
	sort.Strings(labels)

	columns := iop.NewColumnsFromFields(labels...)

	if sample.Histogram != nil {
		columns = append(columns, iop.Column{
			Name:     "timestamp",
			Type:     iop.BigIntType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "count",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "sum",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_boundaries",
			Type:     iop.IntegerType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_count",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_lower",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "bucket_upper",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
	} else {
		columns = append(columns, iop.Column{
			Name:     "timestamp",
			Type:     iop.BigIntType,
			Position: len(columns) + 1,
		})
		columns = append(columns, iop.Column{
			Name:     "value",
			Type:     iop.DecimalType,
			Position: len(columns) + 1,
		})
	}

	return columns
}
