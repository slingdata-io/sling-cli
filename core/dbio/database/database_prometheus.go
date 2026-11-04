package database

import (
	"context"
	"database/sql"
	"net/http"
	"slices"
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

func (conn *PrometheusConn) toFloat(val any) any {
	switch v := val.(type) {
	case model.SampleValue:
		val = strings.ReplaceAll(v.String(), `"`, "")
	case model.FloatString:
		val = strings.ReplaceAll(v.String(), `"`, "")
	case string:
		val = strings.ReplaceAll(v, `"`, "")
	default:
		val = strings.ReplaceAll(cast.ToString(val), `"`, "")
	}
	newVal, err := cast.ToFloat64E(val)
	if err == nil {
		return newVal
	}
	return val
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

	// Pass transforms configuration to datastream
	if transformStr := conn.GetProp("transforms"); transformStr != "" {
		options["transforms"] = transformStr
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

// toHourDuration converts the d (day), w (week) and M (month) suffixes, which
// time.ParseDuration does not understand, into hours.
func toHourDuration(duration string) string {
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

// parseRelativeOffset parses the offset of a "now-<duration>" value (e.g. "-2M",
// "-7d", "-30m"), accepting the same d/w/M suffixes as the non-chunked path.
func parseRelativeOffset(offset string) (time.Duration, error) {
	if strings.HasPrefix(offset, "-") {
		d, err := time.ParseDuration(toHourDuration(strings.TrimPrefix(offset, "-")))
		return -d, err
	}
	return time.ParseDuration(toHourDuration(offset))
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
						duration, _ := parseRelativeOffset(strings.TrimPrefix(val, "now"))
						start = time.Now().Add(duration)
					} else if !strings.HasPrefix(val, "now") {
						start, _ = time.Parse(time.RFC3339, val)
					}
				}

				end := time.Now()
				if val, ok := queryOpts["end"]; ok {
					if strings.HasPrefix(val, "now") && strings.Contains(val, "-") {
						duration, _ := parseRelativeOffset(strings.TrimPrefix(val, "now"))
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
				row[index("value")] = conn.toFloat(value.Value)
				data.Append(row)
			}

			for _, value := range sample.Histograms {
				for _, bucket := range value.Histogram.Buckets {
					row := make([]any, len(data.Columns))
					for k, v := range metricMap {
						row[index(k)] = v
					}
					row[index("timestamp")] = value.Timestamp.Unix()
					row[index("count")] = conn.toFloat(value.Histogram.Count)
					row[index("sum")] = conn.toFloat(value.Histogram.Sum)
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = conn.toFloat(bucket.Count)
					row[index("bucket_lower")] = conn.toFloat(bucket.Lower)
					row[index("bucket_upper")] = conn.toFloat(bucket.Upper)
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
				row[index("count")] = conn.toFloat(sample.Histogram.Count)
				row[index("sum")] = conn.toFloat(sample.Histogram.Sum)

				for _, bucket := range sample.Histogram.Buckets {
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = conn.toFloat(bucket.Count)
					row[index("bucket_lower")] = conn.toFloat(bucket.Lower)
					row[index("bucket_upper")] = conn.toFloat(bucket.Upper)
				}
			} else {
				row[index("timestamp")] = sample.Timestamp.Unix()
				row[index("value")] = conn.toFloat(sample.Value)
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
		data.Append([]any{scalar.Timestamp.Unix(), conn.toFloat(scalar.Value)})
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
		data.Append([]any{str.Timestamp.Unix(), conn.toFloat(str.Value)})
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

	// Create the datastream with connection properties
	props := conn.Props()

	// Pass transforms through to the datastream configuration
	if transformsOpt := opts["transforms"]; transformsOpt != nil {
		props["transforms"] = cast.ToString(transformsOpt)
	}

	// Create a temporary datastream to initialize transforms before streaming the data
	tempDs := iop.NewDatastream(data.Columns)
	tempDs.SetConfig(props)

	// If transforms are configured, apply them manually to the existing data
	// since Prometheus bypasses the normal iterator pattern
	if transformsOpt := opts["transforms"]; transformsOpt != nil && len(data.Rows) > 0 && !cast.ToBool(opts["get_columns"]) {
		err := conn.applyTransformsToData(tempDs, &data)
		if err != nil {
			return nil, err
		}
	}

	ds = data.Stream(props)

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
				duration, err := parseRelativeOffset(strings.TrimPrefix(val, "now"))
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
				duration, err := parseRelativeOffset(strings.TrimPrefix(val, "now"))
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
		step, err = time.ParseDuration(toHourDuration(val))
		if err != nil {
			return nil, g.Error(err, "could not parse step duration: %s", val)
		}
	}
	if step <= 0 {
		return nil, g.Error("step must be positive: %s", step)
	}

	reader := newPromChunkReader(queryContext.Ctx, conn, query, start, end, step)

	// read up to the first rows, to get the columns
	var rows [][]any
	for len(rows) == 0 && !reader.Done() {
		if rows, err = reader.Read(); err != nil {
			return nil, err
		}
	}

	columns := reader.columns
	if len(columns) == 0 && opts["columns"] != nil {
		switch cols := opts["columns"].(type) {
		case iop.Columns:
			columns = cols
		default:
			g.JSONConvert(opts["columns"], &columns)
		}
	}

	props := conn.Props()
	if transformsOpt := opts["transforms"]; transformsOpt != nil {
		props["transforms"] = cast.ToString(transformsOpt)
	}

	// ds.Start reads the first rows through the transforms before SetReady,
	// so the column types and the buffer are final when the dataflow gets them
	i := 0
	nextFunc := func(it *iop.Iterator) bool {
		for i >= len(rows) {
			if reader.Done() || it.Context.Err() != nil {
				return false
			}
			next, err := reader.Read()
			if err != nil {
				it.Context.CaptureErr(err)
				return false
			}
			rows, i = next, 0
		}
		it.Row = rows[i]
		i++
		return true
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, slices.Clone(columns), nextFunc) // the reader keeps its own copy
	ds.SetConfig(props)

	// a cast transform sets the types, as in applyTransformsToData
	if t := ds.Sp.Config.Transforms; t != nil && t.Casted() {
		ds.Inferred = true
	}

	if err = ds.Start(); err != nil {
		return nil, g.Error(err, "could not start prometheus datastream")
	}

	return ds, nil
}

// promChunkReader reads a Prometheus range query in chunks of about 60 points per series.
type promChunkReader struct {
	ctx     context.Context
	conn    *PrometheusConn
	query   string
	step    time.Duration
	size    time.Duration
	delay   time.Duration // between chunks, to not overwhelm the API
	next    time.Time
	end     time.Time
	num     int
	columns iop.Columns // from the first sample
}

func newPromChunkReader(ctx context.Context, conn *PrometheusConn, query string, start, end time.Time, step time.Duration) *promChunkReader {
	// a multiple of step keeps the evaluation times of one full range query
	points := max(60, int64((time.Hour+step-1)/step))
	return &promChunkReader{
		ctx:   ctx,
		conn:  conn,
		query: query,
		step:  step,
		size:  step * time.Duration(points),
		delay: 100 * time.Millisecond,
		next:  start,
		end:   end,
	}
}

// Done is true when no chunk is left. Prometheus includes both range ends,
// so a chunk that starts at end is still read.
func (r *promChunkReader) Done() bool {
	return r.next.After(r.end)
}

// Read queries the next chunk and returns its rows.
func (r *promChunkReader) Read() (rows [][]any, err error) {
	if r.num > 0 && r.delay > 0 {
		time.Sleep(r.delay)
	}

	// end one step before the next chunk, so that boundary points do not repeat
	chunk := v1.Range{Start: r.next, End: r.next.Add(r.size - r.step), Step: r.step}
	if chunk.End.After(r.end) {
		chunk.End = r.end
	}
	r.next = r.next.Add(r.size)
	r.num++

	g.Debug("Processing chunk %d: %s to %s", r.num, chunk.Start.Format(time.RFC3339), chunk.End.Format(time.RFC3339))

	result, warnings, err := r.conn.Client.QueryRange(r.ctx, r.query, chunk)
	if err != nil {
		return nil, g.Error(err, "Error querying Prometheus chunk: %s", r.query)
	}

	for _, warning := range warnings {
		g.Warn(warning)
	}

	switch result := result.(type) {
	case model.Matrix:
		for _, sample := range result {
			metricMap := map[string]string{}
			g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

			if len(r.columns) == 0 {
				r.columns = createPrometheusColumns(metricMap, sample)
			}

			fieldMap := r.columns.FieldMap(true)
			index := func(k string) int { return fieldMap[strings.ToLower(k)] }

			for _, value := range sample.Values {
				row := make([]any, len(r.columns))
				for k, v := range metricMap {
					row[index(k)] = v
				}
				row[index("timestamp")] = value.Timestamp.Unix()
				row[index("value")] = r.conn.toFloat(value.Value)
				rows = append(rows, row)
			}

			for _, hist := range sample.Histograms {
				for i, bucket := range hist.Histogram.Buckets {
					row := make([]any, len(r.columns))
					for k, v := range metricMap {
						row[index(k)] = v
					}
					row[index("timestamp")] = hist.Timestamp.Unix()
					row[index("count")] = r.conn.toFloat(hist.Histogram.Count)
					row[index("sum")] = r.conn.toFloat(hist.Histogram.Sum)
					row[index("bucket_boundaries")] = i
					row[index("bucket_count")] = r.conn.toFloat(bucket.Count)
					row[index("bucket_lower")] = r.conn.toFloat(bucket.Lower)
					row[index("bucket_upper")] = r.conn.toFloat(bucket.Upper)
					rows = append(rows, row)
				}
			}
		}
	case model.Vector:
		for _, sample := range result {
			metricMap := map[string]string{}
			g.Unmarshal(g.Marshal(sample.Metric), &metricMap)

			if len(r.columns) == 0 {
				r.columns = createPrometheusColumnsFromVector(metricMap, sample)
			}

			fieldMap := r.columns.FieldMap(true)
			index := func(k string) int { return fieldMap[strings.ToLower(k)] }

			row := make([]any, len(r.columns))
			for k, v := range metricMap {
				row[index(k)] = v
			}

			if sample.Histogram != nil {
				row[index("timestamp")] = sample.Timestamp.Unix()
				row[index("count")] = r.conn.toFloat(sample.Histogram.Count)
				row[index("sum")] = r.conn.toFloat(sample.Histogram.Sum)

				for _, bucket := range sample.Histogram.Buckets {
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = r.conn.toFloat(bucket.Count)
					row[index("bucket_lower")] = r.conn.toFloat(bucket.Lower)
					row[index("bucket_upper")] = r.conn.toFloat(bucket.Upper)
				}
			} else {
				row[index("timestamp")] = sample.Timestamp.Unix()
				row[index("value")] = r.conn.toFloat(sample.Value)
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
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

// applyTransformsToData applies transforms to the collected Prometheus data
func (conn *PrometheusConn) applyTransformsToData(ds *iop.Datastream, data *iop.Dataset) error {
	if ds.Sp == nil {
		return nil
	}
	if ds.Sp.Config.Transforms == nil {
		return nil
	}

	transforms := ds.Sp.Config.Transforms

	// Apply transforms to each row in the collected data
	for i, row := range data.Rows {
		newRow, err := transforms.Evaluate(row)
		if err != nil {
			return g.Error(err)
		}
		data.Rows[i] = newRow
	}

	// set columns
	data.Columns = ds.Columns

	// set inferred if casting
	// so that the datastream doesn't re-infer
	if transforms.Casted() {
		data.Inferred = true
	}

	return nil
}
