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

	Range := v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	}
	g.Debug("using range %s", g.Marshal(Range))
	result, warnings, err := conn.Client.QueryRange(queryContext.Ctx, query, Range)
	if err != nil {
		return nil, g.Error(err, "Error querying Prometheus: %s", query)
	}

	for _, warning := range warnings {
		g.Warn(warning)
	}

	data := iop.NewDataset(iop.Columns{})
	fieldMap := data.Columns.FieldMap(true)

	index := func(k string) int { return fieldMap[strings.ToLower(k)] }

	if matrix, ok := result.(model.Matrix); ok {
		for _, sample := range matrix {

			metricMap := map[string]string{}
			g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

			if len(data.Columns) == 0 {
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

				data = iop.NewDataset(columns)
				fieldMap = data.Columns.FieldMap(true)
			}

			for _, value := range sample.Values {
				row := make([]any, len(data.Columns))
				for k, v := range metricMap {
					row[index(k)] = v
				}
				row[index("timestamp")] = value.Timestamp.Unix()
				row[index("value")] = value.Value
				data.Append(row)
			}

			for _, value := range sample.Histograms {
				for _, bucket := range value.Histogram.Buckets {
					row := make([]any, len(data.Columns))
					for k, v := range metricMap {
						row[index(k)] = v
					}
					row[index("timestamp")] = value.Timestamp.Unix()
					row[index("count")] = cast.ToFloat64(value.Histogram.Count)
					row[index("sum")] = cast.ToFloat64(value.Histogram.Sum)
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = cast.ToFloat64(bucket.Count)
					row[index("bucket_lower")] = cast.ToFloat64(bucket.Lower)
					row[index("bucket_upper")] = cast.ToFloat64(bucket.Upper)
					data.Append(row)
				}
			}

			if Limit > 0 && len(data.Rows) >= Limit {
				break
			}
		}
	} else if vector, ok := result.(model.Vector); ok {
		for _, sample := range vector {
			metricMap := map[string]string{}
			g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

			if len(data.Columns) == 0 {
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

				data = iop.NewDataset(columns)
				fieldMap = data.Columns.FieldMap(true)
			}

			row := make([]any, len(data.Columns))
			for k, v := range metricMap {
				row[index(k)] = v
			}

			if sample.Histogram != nil {
				row[index("timestamp")] = sample.Timestamp.Unix()
				row[index("count")] = cast.ToFloat64(sample.Histogram.Count)
				row[index("sum")] = cast.ToFloat64(sample.Histogram.Sum)

				for _, bucket := range sample.Histogram.Buckets {
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = cast.ToFloat64(bucket.Count)
					row[index("bucket_lower")] = cast.ToFloat64(bucket.Lower)
					row[index("bucket_upper")] = cast.ToFloat64(bucket.Upper)
				}
			} else {
				row[index("timestamp")] = sample.Timestamp.Unix()
				row[index("value")] = cast.ToFloat64(sample.Value)
			}

			data.Append(row)
			if Limit > 0 && len(data.Rows) >= Limit {
				break
			}
		}
	} else if scalar, ok := result.(*model.Scalar); ok {
		data.Columns = iop.Columns{
			{
				Name:     "timestamp",
				Type:     iop.BigIntType,
				Position: 1,
			},
			{
				Name:     "value",
				Type:     iop.DecimalType,
				Position: 2,
			},
		}
		data.Append([]any{scalar.Timestamp.Unix(), cast.ToFloat64(scalar.Value)})
	} else if str, ok := result.(*model.String); ok {
		data.Columns = iop.Columns{
			{
				Name:     "timestamp",
				Type:     iop.BigIntType,
				Position: 1,
			},
			{
				Name:     "value",
				Type:     iop.StringType,
				Position: 2,
			},
		}
		data.Append([]any{str.Timestamp.Unix(), cast.ToFloat64(str.Value)})
	} else {
		return nil, g.Error("invalid result: %#v", result)
	}

	// If no columns were detected (no data), use columns from options
	if len(data.Columns) == 0 && opts["columns"] != nil {
		switch cols := opts["columns"].(type) {
		case iop.Columns:
			data.Columns = cols
		default:
			// Try to convert from options
			g.JSONConvert(opts["columns"], &data.Columns)
		}
	}

	ds = data.Stream(conn.Props())

	return
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

	// Initialize columns and datastream
	var columns iop.Columns
	columnsInitialized := false

	// Create the datastream
	ds = iop.NewDatastreamContext(queryContext.Ctx, iop.Columns{})
	ds.SetConfig(conn.Props())

	// Process in chunks
	go func() {
		defer ds.Close()

		chunkStart := start
		chunkNum := 0
		for chunkStart.Before(end) {
			chunkEnd := chunkStart.Add(chunkDuration)
			if chunkEnd.After(end) {
				chunkEnd = end
			}

			chunkNum++
			g.Debug("Processing chunk %d: %s to %s", chunkNum, chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))

			// Query this chunk
			chunkRange := v1.Range{
				Start: chunkStart,
				End:   chunkEnd,
				Step:  step,
			}

			result, warnings, err := conn.Client.QueryRange(queryContext.Ctx, query, chunkRange)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error querying Prometheus chunk: %s", query))
				return
			}

			for _, warning := range warnings {
				g.Warn(warning)
			}

			// Process chunk results
			if matrix, ok := result.(model.Matrix); ok {
				for _, sample := range matrix {
					metricMap := map[string]string{}
					g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

					// Initialize columns on first data
					if !columnsInitialized {
						columns = createPrometheusColumns(metricMap, sample)
						ds.Columns = columns
						columnsInitialized = true
						ds.SetReady() // Signal that datastream is ready
					}

					fieldMap := columns.FieldMap(true)
					index := func(k string) int { return fieldMap[strings.ToLower(k)] }

					// Stream values
					for _, value := range sample.Values {
						row := make([]any, len(columns))
						for k, v := range metricMap {
							row[index(k)] = v
						}
						row[index("timestamp")] = value.Timestamp.Unix()
						row[index("value")] = value.Value

						select {
						case <-queryContext.Ctx.Done():
							return
						default:
							ds.Push(row)
						}
					}

					// Stream histogram data if present
					for _, hist := range sample.Histograms {
						for i, bucket := range hist.Histogram.Buckets {
							row := make([]any, len(columns))
							for k, v := range metricMap {
								row[index(k)] = v
							}
							row[index("timestamp")] = hist.Timestamp.Unix()
							row[index("count")] = hist.Histogram.Count
							row[index("sum")] = hist.Histogram.Sum
							row[index("bucket_boundaries")] = i
							row[index("bucket_count")] = bucket.Count
							row[index("bucket_lower")] = bucket.Lower
							row[index("bucket_upper")] = bucket.Upper

							select {
							case <-queryContext.Ctx.Done():
								return
							default:
								ds.Push(row)
							}
						}
					}
				}
			} else if vector, ok := result.(model.Vector); ok {
				// Handle vector results similarly
				for _, sample := range vector {
					metricMap := map[string]string{}
					g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

					if !columnsInitialized {
						columns = createPrometheusColumnsFromVector(metricMap, sample)
						ds.Columns = columns
						columnsInitialized = true
						ds.SetReady() // Signal that datastream is ready
					}

					fieldMap := columns.FieldMap(true)
					index := func(k string) int { return fieldMap[strings.ToLower(k)] }

					row := make([]any, len(columns))
					for k, v := range metricMap {
						row[index(k)] = v
					}

					if sample.Histogram != nil {
						row[index("timestamp")] = sample.Timestamp.Unix()
						row[index("count")] = cast.ToFloat64(sample.Histogram.Count)
						row[index("sum")] = cast.ToFloat64(sample.Histogram.Sum)

						for _, bucket := range sample.Histogram.Buckets {
							row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
							row[index("bucket_count")] = cast.ToFloat64(bucket.Count)
							row[index("bucket_lower")] = cast.ToFloat64(bucket.Lower)
							row[index("bucket_upper")] = cast.ToFloat64(bucket.Upper)
						}
					} else {
						row[index("timestamp")] = sample.Timestamp.Unix()
						row[index("value")] = cast.ToFloat64(sample.Value)
					}

					select {
					case <-queryContext.Ctx.Done():
						return
					default:
						ds.Push(row)
					}
				}
			}

			// Move to next chunk
			chunkStart = chunkEnd

			// Add a small delay between chunks to avoid overwhelming the API
			time.Sleep(100 * time.Millisecond)
		}

		// If no columns were initialized (no data), use columns from options if available
		if !columnsInitialized {
			if opts["columns"] != nil {
				switch cols := opts["columns"].(type) {
				case iop.Columns:
					ds.Columns = cols
				default:
					// Try to convert from options
					g.JSONConvert(opts["columns"], &ds.Columns)
				}
			}
			ds.SetReady()
		}
	}()

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
