package sling

import (
	"os"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// The arrow lane moves Arrow record batches from an ADBC source to an ADBC or
// staged-parquet target without building a []any row. This file decides
// whether a stream may take the lane; it never moves data. The source and the
// sink only see the result: a lane plus the stage 2 check on the source
// connection, or nothing at all.

// ArrowLaneEnvVar turns the lane off, or forces it on for tests (D14).
const ArrowLaneEnvVar = "SLING_ARROW_LANE"

// arrowLaneSwitch is the parsed value of SLING_ARROW_LANE.
type arrowLaneSwitch string

const (
	// arrowLaneAuto runs the lane when every condition holds.
	arrowLaneAuto arrowLaneSwitch = "auto"
	// arrowLaneFalse disables the lane before the engine is asked, so a user
	// who opted out never gets the token warning.
	arrowLaneFalse arrowLaneSwitch = "false"
	// arrowLaneForce turns a decline into an error, for tests that must run on
	// the lane.
	arrowLaneForce arrowLaneSwitch = "force"
)

// newArrowLaneFn returns the closed engine. Tests swap it for a fake lane.
var newArrowLaneFn = iop.NewArrowLane

// getArrowLaneSwitch reads SLING_ARROW_LANE. Anything that is not an explicit
// false or force is auto: "true" turns the lane on, it does not fail a run
// that declines.
func getArrowLaneSwitch() arrowLaneSwitch {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(ArrowLaneEnvVar))) {
	case "false", "0", "off", "no", "disable", "disabled":
		return arrowLaneFalse
	case "force":
		return arrowLaneForce
	}
	return arrowLaneAuto
}

// Log levels for a decline (D23).
const (
	arrowLaneInfo  = "info"  // the user's config is the reason
	arrowLaneDebug = "debug" // the connections, the env switch or a default is the reason
	arrowLaneError = "error" // forced: the run fails
)

// arrowLaneSource describes what the lane reads: an ADBC query, or a local
// cache file (CDC Phase B), which brings its own schema.
type arrowLaneSource struct {
	conn   database.Connection // nil for a cache file
	schema *arrow.Schema       // cache file schema; nil for a query (stage 2 reads it)
	file   bool                // parquet/arrow file source: the footers are read later
	stream string              // stream name for the log lines
}

// arrowLaneTargetKind is where the lane's records land.
type arrowLaneTargetKind string

const (
	arrowLaneToADBC  arrowLaneTargetKind = "adbc"
	arrowLaneToStage arrowLaneTargetKind = "stage"
	arrowLaneToFile  arrowLaneTargetKind = "file"
)

// arrowLaneDecision is stage 1's verdict. A nil lane means the row path, and
// reason says why. check is stage 2, built when the lane is on.
type arrowLaneDecision struct {
	lane   iop.ArrowLane
	check  database.LaneSchemaCheck
	level  string
	reason string
	stream string
	label  string // e.g. "postgres -> snowflake/stage"
	sw     arrowLaneSwitch
	logged bool // stage 2 already logged its one line (cache file)
}

// forced reports whether SLING_ARROW_LANE=force turned a decline into a
// failure.
func (d arrowLaneDecision) forced() bool {
	return d.sw == arrowLaneForce && d.lane == nil
}

// enabled reports whether the stream takes the lane.
func (d arrowLaneDecision) enabled() bool {
	return d.lane != nil
}

// forcedErr returns the error that fails a forced run, nil otherwise.
func (d arrowLaneDecision) forcedErr() error {
	if !d.forced() {
		return nil
	}
	return g.Error(arrowLaneForceText(d.stream, d.reason))
}

// Log writes the one line this stream gets (D23). A decline from the user's
// config is info: the row path is the correct current behavior, not a
// degradation. A decline from the connections, the env switch or a default is
// debug.
func (d arrowLaneDecision) Log() {
	if d.reason == "" || d.logged {
		return
	}

	switch d.level {
	case arrowLaneInfo:
		g.Info("%s", arrowLaneFallbackText(d.stream, d.reason))
	case arrowLaneError:
		g.Error("%s", arrowLaneForceText(d.stream, d.reason))
	default:
		g.Debug("arrow lane: not used for stream %q. Reason: %s", d.stream, d.reason)
	}
}

func arrowLaneFallbackText(stream, reason string) string {
	return g.F("arrow lane: falling back to row-based processing for stream %q. Reason: %s", stream, reason)
}

func arrowLaneForceText(stream, reason string) string {
	return g.F("arrow lane: forced but not eligible for stream %q. Reason: %s", stream, reason)
}

func arrowLaneEnabledText(label string) string {
	return g.F("arrow lane: enabled (%s)", label)
}

// decline builds a decision for the row path. A forced run always logs at
// error, because the run fails.
func arrowLaneDecline(sw arrowLaneSwitch, level, reason string) arrowLaneDecision {
	if sw == arrowLaneForce {
		level = arrowLaneError
	}
	return arrowLaneDecision{sw: sw, level: level, reason: reason}
}

// decideArrowLane is stage 1 of the gate: config, connections, token, and the
// target columns. It runs after both connections are open and before
// ReadFromDB, so a stream never changes mode after it starts (D26).
func (t *TaskExecution) decideArrowLane(src arrowLaneSource, tgtConn database.Connection) arrowLaneDecision {
	cfg := t.Config
	sw := getArrowLaneSwitch()

	if src.stream == "" {
		src.stream = cfg.Source.Stream
		if src.stream == "" {
			src.stream = cfg.Target.Object
		}
	}

	decline := func(level, reason string) arrowLaneDecision {
		d := arrowLaneDecline(sw, level, reason)
		d.stream = src.stream
		return d
	}

	// the env switch comes first, so an opted-out user never warns about a token
	if sw == arrowLaneFalse {
		return decline(arrowLaneDebug, "SLING_ARROW_LANE=false")
	}

	// the engine. The token check (D19) lives behind this call.
	lane, laneReason := newArrowLaneFn()
	if lane == nil {
		return decline(arrowLaneDebug, laneReason)
	}

	// mode
	if cfg.Mode == DefinitionOnlyMode {
		return decline(arrowLaneDebug, "mode definition-only has no rows to move")
	}
	if cfg.Mode == ChangeCaptureMode && src.conn != nil {
		// CDC changes go through the change cache, which is a file source
		return decline(arrowLaneDebug, "change-capture reads through the change cache")
	}

	// source: an ADBC database, or a parquet/arrow file (the CDC cache is a
	// file source too, with its schema already known)
	srcType, srcReason := "", ""
	switch {
	case src.file || src.schema != nil:
		srcType, srcReason = arrowLaneSourceFile(cfg, src)
	default:
		srcType, srcReason = arrowLaneSourceConn(src.conn)
	}
	if srcReason != "" {
		return decline(arrowLaneDebug, srcReason)
	}

	// target
	tgtKind, tgtReason, tgtLevel := arrowLaneTargetConn(cfg, tgtConn)
	if tgtReason != "" {
		return decline(tgtLevel, tgtReason)
	}

	// config
	if reason, level := arrowLaneConfigReason(t, src, lane); reason != "" {
		return decline(level, reason)
	}
	if reason, level := cfg.checkArrowLaneSourceOptions(); reason != "" {
		return decline(level, reason)
	}

	// stage 2 input: the target table columns, when the table exists
	tgtCols := iop.Columns{}
	if tgtConn != nil {
		tgtCols, _ = pullTargetTableColumns(cfg, tgtConn, false)
	}

	// the update key drives the incremental state. A change-capture stream has
	// its own state (the change cache), and its key is often a string.
	updateKey := ""
	if t.hasStateWithUpdateKey() && cfg.Mode != ChangeCaptureMode {
		updateKey = cfg.Source.UpdateKey
	}

	label := g.F("%s -> %s/%s", srcType, cfg.TgtConn.Type, tgtKind)
	jsonOK := arrowLaneCarriesJSON(tgtConn)
	check := t.newArrowLaneCheck(lane, src.stream, label, tgtCols, updateKey, jsonOK, cfg.TransformsPrepared())

	decision := arrowLaneDecision{
		lane:   lane,
		check:  check,
		stream: src.stream,
		label:  label,
		sw:     sw,
	}

	// the cache file carries its schema, so stage 2 runs here and the verdict
	// is final: there is no query to read the schema from later
	if src.schema != nil {
		ok, reason, _ := check(src.schema, iop.ArrowSchemaToColumns(src.schema))
		if !ok {
			d := arrowLaneDecline(sw, arrowLaneDebug, reason)
			d.stream = src.stream
			d.logged = true // the check logged its own line
			return d
		}
		decision.check = nil
		decision.logged = true
	}

	return decision
}

// arrowLaneSourceFile applies the file-source rules: a parquet or arrow file
// read straight into records, never through duckdb, with a plain select.
func arrowLaneSourceFile(cfg *Config, src arrowLaneSource) (label, reason string) {
	// the CDC cache file carries its footer schema, and its source connection
	// is the change reader, not a file: the format, duckdb and select rows
	// below only apply to a real file source
	if src.schema != nil && !src.file {
		return "arrow file", ""
	}

	if cfg.SrcConn.Type.Kind() != dbio.KindFile {
		return "", "source is not a file connection"
	}

	var format dbio.FileType
	if cfg.Source.Options.Format != nil {
		format = *cfg.Source.Options.Format
	} else {
		format = filesys.InferFileFormat(cfg.SrcConn.URL())
	}
	if !g.In(format, dbio.FileTypeParquet, dbio.FileTypeArrow) {
		return "", g.F("file source format %s is not parquet or arrow", format)
	}

	// duckdb compute runs its own scan, so the lane cannot read the files
	fsCfg := iop.FileStreamConfig{Format: format, Select: cfg.Source.Select, SQL: cfg.Source.Query}
	if fsCfg.ShouldUseDuckDB() {
		return "", "the file source would use duckdb compute"
	}

	// an exclude or a glob in the select is row-path value work
	for _, sel := range cfg.Source.Select {
		if strings.HasPrefix(sel, "-") || strings.Contains(sel, "*") {
			return "", g.F("select %q is not a plain column list", sel)
		}
	}

	return "file", ""
}

// setArrowLane runs the gate and installs its verdict on the source
// connection. It returns the error of a forced run.
func (t *TaskExecution) setArrowLane(src arrowLaneSource, tgtConn database.Connection) error {
	decision := t.decideArrowLane(src, tgtConn)
	decision.Log()

	if decision.enabled() {
		g.Trace("arrow lane: candidate for stream %q (%s)", decision.stream, decision.label)
		src.conn.SetProp("arrow_lane", "candidate")
		src.conn.SetArrowLane(decision.lane, decision.check)
	} else {
		// clear state a previous stream (or a pooled connection) may have left
		src.conn.SetProp("arrow_lane", "")
		src.conn.SetArrowLane(nil, nil)
	}

	return decision.forcedErr()
}

// arrowLaneTargetFile classifies a file target. The sink is
// WriteDataflowReady, which writes records straight to the file when the
// format is parquet or arrow. A csv target is a decline: debug when the
// format is only inferred from the object name, info when the user set it.
func arrowLaneTargetFile(cfg *Config) (kind arrowLaneTargetKind, reason, level string) {
	if cfg.TgtConn.Type.Kind() != dbio.KindFile {
		return "", "no target connection", arrowLaneDebug
	}

	// use_bulk is a target option the file sink shares with the database one:
	// false means the user asked for the row path
	if cfg.Target.Options != nil && cfg.Target.Options.UseBulk != nil && !*cfg.Target.Options.UseBulk {
		return "", "use_bulk is false", arrowLaneInfo
	}

	format := cfg.Target.Options.Format
	explicit := format != ""
	if !explicit {
		format = filesys.InferFileFormat(cfg.Target.Object)
	}

	if g.In(format, dbio.FileTypeParquet, dbio.FileTypeArrow) {
		// a partitioned target goes through the temp DuckDB writer, which
		// reads rows the lane does not have
		if len(iop.ExtractPartitionFields(cfg.Target.Object)) > 0 {
			return "", "the target object has partition fields", arrowLaneInfo
		}
		return arrowLaneToFile, "", ""
	}

	reason = g.F("format: %s on the target", format)
	if explicit {
		return "", reason, arrowLaneInfo
	}
	return "", reason, arrowLaneDebug
}

// arrowLaneSourceConn applies the D15 source list.
func arrowLaneSourceConn(conn database.Connection) (label string, reason string) {
	if conn == nil {
		return "", "source is not a database connection"
	}
	if !conn.UseADBC() {
		return "", g.F("source connection %s is not ADBC", conn.GetType())
	}
	if !g.In(conn.GetType(), arrowLaneSourceTypes...) {
		return "", g.F("source driver %s is not in the arrow lane list", conn.GetType())
	}
	return string(conn.GetType()), ""
}

// arrowLaneTargetConn applies the D15 target list and the staged rules. A nil
// target connection is a file target: the sink writes records when the format
// is parquet or arrow.
func arrowLaneTargetConn(cfg *Config, tgtConn database.Connection) (kind arrowLaneTargetKind, reason, level string) {
	if tgtConn == nil {
		return arrowLaneTargetFile(cfg)
	}

	if tgtConn.UseADBC() {
		if !g.In(tgtConn.GetType(), arrowLaneTargetTypes...) {
			return "", g.F("target driver %s is not in the arrow lane list", tgtConn.GetType()), arrowLaneDebug
		}
		if cfg.Target.Options.UseBulk != nil && !*cfg.Target.Options.UseBulk {
			return "", "use_bulk is false", arrowLaneInfo
		}
		return arrowLaneToADBC, "", ""
	}

	if !g.In(tgtConn.GetType(), arrowLaneStageTypes...) {
		return "", g.F("target driver %s is not in the arrow lane list", tgtConn.GetType()), arrowLaneDebug
	}

	if reason := arrowLaneStageReason(tgtConn); reason != "" {
		return "", reason, arrowLaneInfo
	}

	return arrowLaneToStage, "", ""
}

// arrowLaneStageReason checks that the staged loader this target picks is the
// parquet one. A target with no stage, or with a copy method that has its own
// arrow path, stays on the row path.
func arrowLaneStageReason(conn database.Connection) (reason string) {
	if conn.GetProp("use_bulk") == "false" {
		return "use_bulk is false"
	}

	switch conn.GetType() {
	case dbio.TypeDbSnowflake:
		switch conn.GetProp("copy_method") {
		case "AWS", "AZURE":
			return "copy_method " + conn.GetProp("copy_method") + " uses its own parquet path"
		}
		// an empty internal_stage is fine: the write path creates the
		// default SLING_STAGING stage before the COPY runs
	case dbio.TypeDbDatabricks:
		if conn.GetProp("copy_method") == "zerobus" {
			return "copy_method: zerobus has its own arrow path"
		}
	case dbio.TypeDbRedshift:
		if conn.GetProp("AWS_BUCKET") == "" {
			return "no AWS_BUCKET is configured"
		}
	}

	if format := strings.ToLower(conn.GetProp("format")); format != "" && format != string(dbio.FileTypeParquet) {
		return "format: " + format + " on the target"
	}
	return ""
}

// arrowLaneConfigReason covers the gate rows that the source options do not:
// transforms, columns, metadata and the like. It returns the reason and the
// level D23 gives it.
func arrowLaneConfigReason(t *TaskExecution, src arrowLaneSource, lane iop.ArrowLane) (reason, level string) {
	cfg := t.Config

	if cfg.Transforms != nil {
		stages := cfg.TransformsPrepared()
		if len(stages) == 0 {
			return "transforms are set (the lane does not evaluate value work)", arrowLaneInfo
		}
		// every stage must classify, or the whole stream falls back: the lane
		// never evaluates part of a stage list. The schema is unknown here, so
		// stage 2 re-runs the check with the real columns.
		if reason := lane.ClassifyTransform(stages, nil); reason != "" {
			return reason, arrowLaneInfo
		}
	}

	// a constraint is evaluated per value by the row path
	if cols, err := cfg.ColumnsPrepared(); err == nil {
		for _, col := range cols {
			if col.Constraint != nil {
				return g.F("a constraint is set on column %q", col.Name), arrowLaneInfo
			}
		}
	}

	if cfg.Target.Columns != nil {
		return "columns is set (the lane does not apply column value casting)", arrowLaneInfo
	}
	if cfg.Target.Options.ColumnTyping != nil {
		return "column_typing is set (it changes decimal, bool or json types)", arrowLaneInfo
	}
	if cfg.Target.Options.DirectInsert != nil && *cfg.Target.Options.DirectInsert {
		return "direct_insert is set", arrowLaneInfo
	}
	if cast.ToUint64(os.Getenv("SLING_CHECKSUM_ROWS")) > 0 {
		return "row checksum is requested (SLING_CHECKSUM_ROWS)", arrowLaneInfo
	}
	if database.NewSchemaMigrator(nil).IsEnabled() {
		return "the schema migrator is enabled", arrowLaneInfo
	}

	if val := cfg.Source.Options.DatetimeFormat; !arrowLaneDatetimeFormatDefault(val) {
		return "source datetime_format is set", arrowLaneInfo
	}
	if val := cfg.Target.Options.DatetimeFormat; !arrowLaneDatetimeFormatDefault(val) {
		return "target datetime_format is set", arrowLaneInfo
	}

	return "", ""
}

// arrowLaneDatetimeFormatDefault reports whether a datetime_format option is
// at its default. The row path only uses it for text formats, so the lane
// keeps the simple rule.
func arrowLaneDatetimeFormatDefault(val string) bool {
	return g.In(strings.ToUpper(strings.TrimSpace(val)), "", "AUTO")
}

// arrowLaneSourceOptionClass says how one source option interacts with the
// lane (D13). Every field of SourceOptions is classified, and
// TestArrowLane_OptionClasses fails when a new field is not.
type arrowLaneSourceOptionClass string

const (
	// arrowOptSQL: the option is compiled into the source SQL, so the lane
	// reads the same rows.
	arrowOptSQL arrowLaneSourceOptionClass = "sql"
	// arrowOptNames: the option only changes the names or the types sling
	// derives. The values the lane reads are the same.
	arrowOptNames arrowLaneSourceOptionClass = "schema-only"
	// arrowOptInert: the option has no effect on a database read.
	arrowOptInert arrowLaneSourceOptionClass = "inert"
	// arrowOptDefault: eligible at its default value only.
	arrowOptDefault arrowLaneSourceOptionClass = "default-only"
	// arrowOptDeclines: a user value always declines the lane.
	arrowOptDeclines arrowLaneSourceOptionClass = "declines"
)

// arrowLaneSourceOptionClasses is the classification of every SourceOptions
// field.
var arrowLaneSourceOptionClasses = map[string]arrowLaneSourceOptionClass{
	"EmptyAsNull":    arrowOptDefault,
	"Header":         arrowOptDefault,
	"Flatten":        arrowOptDefault,
	"FieldsPerRec":   arrowOptDefault,
	"Compression":    arrowOptDefault,
	"Format":         arrowOptDefault,
	"NullIf":         arrowOptDefault,
	"DatetimeFormat": arrowOptDefault,
	"SkipBlankLines": arrowOptDefault,
	"SkipLines":      arrowOptDefault,
	"Delimiter":      arrowOptDefault,
	"Escape":         arrowOptDefault,
	"Quote":          arrowOptDefault,
	"MaxDecimals":    arrowOptDefault,
	"JmesPath":       arrowOptDefault,
	"Jq":             arrowOptDefault,
	"Sheet":          arrowOptDefault,
	"Range":          arrowOptSQL,
	"Limit":          arrowOptSQL,
	"Offset":         arrowOptSQL,
	// a chunked read compiles into one stream per chunk, each with its own
	// range in the source SQL. Every chunk stream decides the lane on its own,
	// so the options only have to be honest about what they change.
	"ChunkSize":  arrowOptSQL,
	"ChunkCount": arrowOptSQL,
	"ChunkExpr":  arrowOptSQL,
	"Encoding":   arrowOptDefault,
	"Columns":    arrowOptDeclines, // legacy: it becomes target columns
	"Transforms": arrowOptDeclines, // legacy: it becomes cfg.Transforms
}

// arrowLaneTargetOptionClasses classifies every TargetOptions field, for the
// coverage test and as documentation. The gate reads the few that matter
// through arrowLaneConfigReason.
var arrowLaneTargetOptionClasses = map[string]arrowLaneSourceOptionClass{
	"Header":           arrowOptInert,
	"Compression":      arrowOptDefault,
	"Concurrency":      arrowOptInert,
	"BatchLimit":       arrowOptInert,
	"BatchMaxDuration": arrowOptInert,
	"DatetimeFormat":   arrowOptDefault,
	"Delimiter":        arrowOptInert,
	"FileMaxRows":      arrowOptInert,
	"FileMaxBytes":     arrowOptInert,
	"Format":           arrowOptDefault,
	"MaxDecimals":      arrowOptDefault,
	"UseBulk":          arrowOptDefault,
	"IgnoreExisting":   arrowOptInert,
	"DeleteMissing":    arrowOptSQL,
	"AddNewColumns":    arrowOptInert,
	"AdjustColumnType": arrowOptInert,
	"ColumnCasing":     arrowOptNames,
	"ColumnTyping":     arrowOptDeclines,
	"Encoding":         arrowOptInert,
	"DirectInsert":     arrowOptDeclines,
	"TableKeys":        arrowOptInert,
	"TableTmp":         arrowOptSQL,
	"TableDDL":         arrowOptInert,
	"PreSQL":           arrowOptSQL,
	"PostSQL":          arrowOptSQL,
	"IsolationLevel":   arrowOptInert,
	"MergeStrategy":    arrowOptSQL,
}

// checkArrowLaneSourceOptions reports the first source option the lane cannot
// honor. Each value is compared against the default of its source kind, so
// only a value the user moved away from can decline.
func (cfg *Config) checkArrowLaneSourceOptions() (reason, level string) {
	opts := cfg.Source.Options
	if opts == nil {
		return "", ""
	}

	defaults := cfg.arrowLaneSourceDefaults()
	val := reflect.ValueOf(*opts)
	def := reflect.ValueOf(defaults)
	typ := val.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		class, ok := arrowLaneSourceOptionClasses[field.Name]
		if !ok {
			// the coverage test guards this list. At runtime an unclassified
			// option declines rather than risk a silent value change.
			if !val.Field(i).IsZero() {
				return arrowLaneDeclineText(snakeCase(field.Name)), arrowLaneInfo
			}
			continue
		}

		if field.Name == "Flatten" && arrowLaneFlattenDefault(val.Field(i)) {
			// getSourceOptionsMap rewrites flatten to an int before the gate
			// runs, so an unset option arrives as -1, not nil
			continue
		}

		switch class {
		case arrowOptSQL, arrowOptNames, arrowOptInert:
			continue
		case arrowOptDeclines:
			if !val.Field(i).IsZero() {
				return arrowLaneDeclineText(snakeCase(field.Name)), arrowLaneInfo
			}
		case arrowOptDefault:
			if !sameOptionValue(val.Field(i), def.Field(i)) {
				return arrowLaneDeclineText(snakeCase(field.Name)), arrowLaneInfo
			}
		}
	}

	return "", ""
}

// arrowLaneSourceDefaults returns the option defaults of the source kind, the
// same struct SetDefault applies.
func (cfg *Config) arrowLaneSourceDefaults() SourceOptions {
	if cfg.SrcConn.Type.Kind() == dbio.KindFile {
		return SourceFileOptionsDefault
	}
	return SourceDBOptionsDefault
}

// arrowLaneFlattenDefault reports whether a flatten option is at its default.
// Source.Flatten returns -1 for nil, false and "false", and getSourceOptionsMap
// stores that int on the options struct before the gate runs.
func arrowLaneFlattenDefault(v reflect.Value) bool {
	val := v.Interface()
	if g.IsNil(val) {
		return true
	}
	switch val {
	case false, "false":
		return true
	}
	return cast.ToInt(val) == -1
}

// sameOptionValue compares two option values, following pointers and
// interfaces.
func sameOptionValue(a, b reflect.Value) bool {
	if a.Kind() == reflect.Ptr || a.Kind() == reflect.Interface {
		if a.IsNil() || b.IsNil() {
			return a.IsNil() && b.IsNil()
		}
		return sameOptionValue(a.Elem(), b.Elem())
	}
	if b.Kind() == reflect.Ptr || b.Kind() == reflect.Interface {
		if b.IsNil() {
			return false
		}
		return sameOptionValue(a, b.Elem())
	}
	return reflect.DeepEqual(a.Interface(), b.Interface())
}

// arrowLaneDeclineText names the option in the decline line. The substrings
// are what the CLI suite asserts on.
func arrowLaneDeclineText(name string) string {
	switch name {
	case "columns", "transforms":
		return name + " are set (legacy, at the stream level)"
	}
	return name + " is set"
}

// snakeCase converts a Go field name to the option name in a replication file.
func snakeCase(name string) string {
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			if prev := rune(name[i-1]); !(prev >= 'A' && prev <= 'Z') {
				b.WriteByte('_')
			}
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

// arrowLaneSourceTypes is D15 for sources: the ADBC drivers whose Arrow schema
// the lane carries. A driver lands here after its schema dump is in the type
// matrix.
var arrowLaneSourceTypes = []dbio.Type{
	dbio.TypeDbPostgres,
	dbio.TypeDbMySQL,
	dbio.TypeDbDuckDb,
	dbio.TypeDbSQLite,
	dbio.TypeDbSnowflake,
	dbio.TypeDbBigQuery,
}

// arrowLaneTargetTypes is D15 for ADBC targets.
var arrowLaneTargetTypes = []dbio.Type{
	dbio.TypeDbPostgres,
	dbio.TypeDbClickhouse,
	dbio.TypeDbDuckDb,
	dbio.TypeDbSQLite,
	dbio.TypeDbSnowflake,
}

// arrowLaneStageTypes are the staged-parquet targets.
var arrowLaneStageTypes = []dbio.Type{
	dbio.TypeDbSnowflake,
	dbio.TypeDbDatabricks,
	dbio.TypeDbRedshift,
}

// arrowLaneCarryableTypes are the column types ColumnsToArrowSchema maps to an
// Arrow type that maps back to the same column type (the D3 round trip).
// Anything else (uuid, geometry, ...) would arrive as utf8, so a target column
// of that type declines the lane.
var arrowLaneCarryableTypes = map[iop.ColumnType]bool{
	iop.BoolType:       true,
	iop.SmallIntType:   true,
	iop.IntegerType:    true,
	iop.BigIntType:     true,
	iop.FloatType:      true,
	iop.DecimalType:    true,
	iop.DateType:       true,
	iop.DatetimeType:   true,
	iop.TimestampType:  true,
	iop.TimestampzType: true,
	iop.TimeType:       true,
	iop.TimezType:      true,
	iop.StringType:     true,
	iop.TextType:       true,
	iop.JsonType:       true,
	iop.BinaryType:     true,
}

// arrowLaneCarriesJSON reports whether a json column keeps its type on the
// target. Snowflake types json as VARIANT, and it loads a parquet string into
// a VARIANT column as a string, not as parsed json. Its stage and its ADBC
// ingest both load parquet.
func arrowLaneCarriesJSON(tgtConn database.Connection) bool {
	return tgtConn == nil || tgtConn.GetType() != dbio.TypeDbSnowflake
}

// newArrowLaneCheck builds stage 2. It runs on the real reader schema of the
// final SQL, once per stream, before the datastream starts, and returns the
// update-key field index for the incremental state (or -1).
func (t *TaskExecution) newArrowLaneCheck(lane iop.ArrowLane, stream, label string, tgtCols iop.Columns, updateKey string, jsonOK bool, stages []map[string]string) database.LaneSchemaCheck {
	// a stream can run more than once (retries), so the line is logged once
	logged := false

	return func(src *arrow.Schema, cols iop.Columns) (ok bool, reason string, maxCol int) {
		maxCol = -1

		decline := func(level, reason string) (bool, string, int) {
			if !logged {
				logged = true
				d := arrowLaneDecline(arrowLaneAuto, level, reason)
				d.stream = stream
				d.Log()
			}
			return false, reason, -1
		}

		// the stage targets are only known here, so this is where a stage that
		// names a new column declines
		if reason := lane.ClassifyTransform(stages, cols); reason != "" {
			return decline(arrowLaneInfo, reason)
		}

		for _, col := range cols {
			// a type the driver labels (uuid) is a column type decline, as
			// in the target check below
			if !arrowLaneCarryableTypes[col.Type] {
				return decline(arrowLaneInfo, g.F("cannot cast column %q to %s: the lane carries Arrow types only",
					col.Name, col.Type))
			}
			if col.Type == iop.JsonType && !jsonOK {
				return decline(arrowLaneInfo, g.F("json column %q would load as a string on this target", col.Name))
			}
		}

		derived := iop.ColumnsToArrowSchema(cols)

		if len(src.Fields()) != len(derived.Fields()) {
			return decline(arrowLaneDebug, g.F("source has %d fields, the derived schema has %d",
				len(src.Fields()), len(derived.Fields())))
		}

		// every source field must reach the derived type unchanged (D3)
		for i, field := range src.Fields() {
			dField := derived.Field(i)
			if arrow.TypeEqual(field.Type, dField.Type) {
				continue
			}
			if ok, castReason := lane.CastSupported(field.Type, dField.Type); !ok {
				return decline(arrowLaneDebug, g.F("cannot cast column %q from %s to %s: %s",
					field.Name, field.Type, dField.Type, castReason))
			}
		}

		// the target table, when it exists. The temp table is created from the
		// same columns, so a gap here means the target cannot hold the record
		// type.
		if len(tgtCols) > 0 {
			derivedByName := map[string]arrow.Field{}
			for _, field := range derived.Fields() {
				derivedByName[strings.ToLower(field.Name)] = field
			}

			for _, col := range tgtCols {
				srcField, ok := derivedByName[strings.ToLower(col.Name)]
				if !ok {
					// a target column the source does not provide
					continue
				}
				if !arrowLaneCarryableTypes[col.Type] {
					return decline(arrowLaneInfo, g.F("cannot cast column %q to %s: the lane carries Arrow types only",
						col.Name, col.Type))
				}
				tgtField := iop.ColumnsToArrowSchema(iop.Columns{col}).Field(0)
				// ColumnsToArrowSchema labels a zone-less Sling timestamp with
				// UTC, while the reader reports the same column with no zone.
				// The column type decides the wall clock, so the target keeps
				// the source's zone label for the zone-less types.
				if tgtTs, ok := tgtField.Type.(*arrow.TimestampType); ok &&
					(col.Type == iop.DatetimeType || col.Type == iop.TimestampType) {
					if srcTs, ok := srcField.Type.(*arrow.TimestampType); ok {
						tgtField.Type = &arrow.TimestampType{Unit: tgtTs.Unit, TimeZone: srcTs.TimeZone}
					}
				}
				if arrow.TypeEqual(srcField.Type, tgtField.Type) {
					continue
				}
				if ok, castReason := lane.CastSupported(srcField.Type, tgtField.Type); !ok {
					return decline(arrowLaneInfo, g.F("cannot cast column %q from %s to %s: %s",
						col.Name, srcField.Type, tgtField.Type, castReason))
				}
			}
		}

		// the update key drives the incremental state (D7)
		if updateKey != "" {
			idx := -1
			for i, field := range src.Fields() {
				if strings.EqualFold(field.Name, updateKey) {
					idx = i
					break
				}
			}
			if idx < 0 {
				return decline(arrowLaneInfo, g.F("update_key %q is not in the source schema", updateKey))
			} else if reason := arrowLaneUpdateKeyReason(src.Field(idx).Type); reason != "" {
				return decline(arrowLaneInfo, reason)
			}
			maxCol = idx
		}

		if !logged {
			logged = true
			g.Info("%s", arrowLaneEnabledText(label))
		}

		return true, "", maxCol
	}
}

// arrowLaneUpdateKeyReason allows the types the lane can track a maximum for.
func arrowLaneUpdateKeyReason(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.DATE32,
		arrow.TIMESTAMP,
		// a string key is tracked by its own loop (RecordStream.MaxString)
		arrow.STRING, arrow.LARGE_STRING:
		return ""
	}
	return g.F("update_key type is not tracked yet (%s)", dt)
}
