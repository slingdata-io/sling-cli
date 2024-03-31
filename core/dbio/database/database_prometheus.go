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

// Init initiates the object
func (conn *PrometheusConn) getNewClient(timeOut ...int) (client v1.API, err error) {

	var rt http.RoundTripper
	if token := conn.GetProp("token"); token != "" {
		rt = config.NewAuthorizationCredentialsRoundTripper("Bearer", config.Secret(token), api.DefaultRoundTripper)
	}

	if user := conn.GetProp("user"); user != "" {
		rt = config.NewBasicAuthRoundTripper(user, config.Secret(conn.GetProp("password")), "", "", api.DefaultRoundTripper)
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

	return nil
}

func (conn *PrometheusConn) Close() error {
	return nil
}

// NewTransaction creates a new transaction
func (conn *PrometheusConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// does not support transaction
	return
}
func (conn *PrometheusConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	return iop.Columns{{Name: "metric"}}, nil
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

func (conn *PrometheusConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return nil, g.Error("ExecContext not implemented on PrometheusConn")
}

func (conn *PrometheusConn) BulkExportFlow(tables ...Table) (df *iop.Dataflow, err error) {
	if len(tables) == 0 {
		return
	}

	// parse options
	options := g.M()
	if parts := strings.Split(tables[0].SQL, `#`); len(parts) > 1 {
		lastPart := parts[len(parts)-1]
		g.Unmarshal(lastPart, &options)
		g.Debug("query options: %s", g.Marshal(options))
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, tables[0].SQL, options)
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
						Type:     iop.TimestampType,
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
						Type:     iop.TimestampType,
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

			for _, value := range sample.Values {
				row[index("timestamp")] = value.Timestamp.Time()
				row[index("value")] = cast.ToFloat64(value.Value)
			}

			for _, value := range sample.Histograms {
				row[index("timestamp")] = value.Timestamp.Time()
				row[index("count")] = cast.ToFloat64(value.Histogram.Count)
				row[index("sum")] = cast.ToFloat64(value.Histogram.Sum)

				for _, bucket := range value.Histogram.Buckets {
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = cast.ToFloat64(bucket.Count)
					row[index("bucket_lower")] = cast.ToFloat64(bucket.Lower)
					row[index("bucket_upper")] = cast.ToFloat64(bucket.Upper)
				}
			}

			data.Append(row)
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
						Type:     iop.TimestampType,
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
						Type:     iop.TimestampType,
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
				row[index("timestamp")] = sample.Timestamp.Time()
				row[index("count")] = cast.ToFloat64(sample.Histogram.Count)
				row[index("sum")] = cast.ToFloat64(sample.Histogram.Sum)

				for _, bucket := range sample.Histogram.Buckets {
					row[index("bucket_boundaries")] = cast.ToInt(bucket.Boundaries)
					row[index("bucket_count")] = cast.ToFloat64(bucket.Count)
					row[index("bucket_lower")] = cast.ToFloat64(bucket.Lower)
					row[index("bucket_upper")] = cast.ToFloat64(bucket.Upper)
				}
			} else {
				row[index("timestamp")] = sample.Timestamp.Time()
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
				Type:     iop.TimestampType,
				Position: 1,
			},
			{
				Name:     "value",
				Type:     iop.DecimalType,
				Position: 2,
			},
		}
		data.Append([]any{scalar.Timestamp, cast.ToFloat64(scalar.Value)})
	} else if str, ok := result.(*model.String); ok {
		data.Columns = iop.Columns{
			{
				Name:     "timestamp",
				Type:     iop.TimestampType,
				Position: 1,
			},
			{
				Name:     "value",
				Type:     iop.StringType,
				Position: 2,
			},
		}
		data.Append([]any{str.Timestamp, cast.ToFloat64(str.Value)})
	} else {
		return nil, g.Error("invalid result: %#v", result)
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
func (conn *PrometheusConn) GetSchemata(schemaName string, tableNames ...string) (Schemata, error) {
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

	schema.Tables[strings.ToLower(tableName)] = table
	schemas[strings.ToLower(schema.Name)] = schema

	schemata.Databases[strings.ToLower(database)] = Database{
		Name:    database,
		Schemas: schemas,
	}

	return schemata, nil
}
