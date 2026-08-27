package assist

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

func TestSignError_NoStreamColumns(t *testing.T) {
	errText := `--- task_run.go:140 func2 ---
--- task_run.go:830 runDbToDb ---
~ Could not WriteToDb
--- task_run_write.go:168 WriteToDb ---
no stream columns detected`
	meta := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	sig := SignError(errText, meta)

	if sig.Algorithm != "v1" {
		t.Fatalf("algorithm: got %q", sig.Algorithm)
	}
	if len(sig.ID) != CompositeIDLen {
		t.Fatalf("id len: got %q (want %d hex)", sig.ID, CompositeIDLen)
	}
	if len(sig.PatternID) != PartIDLen || len(sig.EdgeID) != PartIDLen {
		t.Fatalf("parts: pattern=%q edge=%q", sig.PatternID, sig.EdgeID)
	}
	if sig.ID != sig.PatternID+sig.EdgeID {
		t.Fatalf("composite != pattern||edge: %s vs %s%s", sig.ID, sig.PatternID, sig.EdgeID)
	}
	// Stack frames must not appear in skeleton.
	if strings.Contains(sig.Skeleton, "task_run") || strings.Contains(sig.Skeleton, ".go:") {
		t.Fatalf("skeleton still has frames:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "could not writetodb") {
		t.Fatalf("skeleton missing context msg:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "no stream columns detected") {
		t.Fatalf("skeleton missing leaf:\n%s", sig.Skeleton)
	}
	// Deterministic
	sig2 := SignError(errText, meta)
	if sig.ID != sig2.ID || sig.PatternID != sig2.PatternID || sig.EdgeID != sig2.EdgeID {
		t.Fatalf("not deterministic: %v vs %v", sig, sig2)
	}
	// Line numbers must not change signature.
	errAlt := strings.ReplaceAll(errText, "140", "999")
	errAlt = strings.ReplaceAll(errAlt, "830", "1")
	errAlt = strings.ReplaceAll(errAlt, "168", "42")
	if SignError(errAlt, meta).ID != sig.ID {
		t.Fatalf("line numbers changed signature")
	}
	// Different target type → different composite / edge, same pattern
	other := SignError(errText, SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbSnowflake})
	if other.ID == sig.ID {
		t.Fatalf("different meta should change composite id")
	}
	if other.PatternID != sig.PatternID {
		t.Fatalf("same skeleton should share pattern_id: %s vs %s", sig.PatternID, other.PatternID)
	}
	if other.EdgeID == sig.EdgeID {
		t.Fatalf("different target should change edge_id")
	}
}

func TestSignError_ConnectionRefusedScrubsIP(t *testing.T) {
	errText := `--- proc.go:283 main ---
--- sling_cli.go:517 main ---
~ could not connect to database(try adding ` + "`sslmode=require`" + ` or ` + "`sslmode=disable`" + `)
dial tcp 192.168.176.200:5432: connect: connection refused`
	meta := SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbPostgres}
	sig := SignError(errText, meta)
	if strings.Contains(sig.Skeleton, "192.168") {
		t.Fatalf("IP not scrubbed:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "<ip>") {
		t.Fatalf("expected <ip> placeholder:\n%s", sig.Skeleton)
	}
	err2 := strings.ReplaceAll(errText, "192.168.176.200", "10.0.0.5")
	if SignError(err2, meta).ID != sig.ID {
		t.Fatalf("different IPs should cluster")
	}
}

func TestSignError_AuthFailedExtractsCode(t *testing.T) {
	errText := `--- task_run.go:142 func2 ---
~ Could not initialize target connection
--- database_clickhouse.go:74 Connect ---
~ could not connect to database
clickhouse [execute]:: 403 code: Code: 516. DB::Exception: bcdata: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED) (version 25.11.2.24 (official build))
`
	meta := SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbClickhouse}
	sig := SignError(errText, meta)
	if strings.Contains(sig.Skeleton, "25.11") {
		t.Fatalf("version banner not scrubbed:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "code:authentication_failed") {
		t.Fatalf("expected AUTHENTICATION_FAILED code:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "code:ch_516") {
		t.Fatalf("expected CH code:\n%s", sig.Skeleton)
	}
	t.Logf("skeleton:\n%s\nid=%s dashed=%s label=%s", sig.Skeleton, sig.ID, sig.IDDashed(), sig.ShortLabel)
}

func TestSignError_URLAndPathScrub(t *testing.T) {
	errText := `--- database.go:709 Connect ---
Post "https://clickhouse.bcstuff.dev:443?database=bc_clickhouse_db&default_format=Native": dial tcp 136.41.64.93:443: i/o timeout
unable to open database "/root/.duckdb/extensions/v1.4.2/linux_amd64/motherduck.duckdb_extension"
`
	meta := SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbClickhouse}
	sig := SignError(errText, meta)
	if strings.Contains(sig.Skeleton, "bcstuff") || strings.Contains(sig.Skeleton, "clickhouse.bc") {
		t.Fatalf("URL host not scrubbed:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "<url>") {
		t.Fatalf("expected <url>:\n%s", sig.Skeleton)
	}
	if strings.Contains(sig.Skeleton, "/root/") {
		t.Fatalf("path not scrubbed:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "<path>") {
		t.Fatalf("expected <path>:\n%s", sig.Skeleton)
	}
}

func TestSignError_DatabricksArityCode(t *testing.T) {
	errText := `--- task_run.go:142 func2 ---
~ Could not WriteToDb
--- database_databricks.go:184 BulkImportFlow ---
~ could not insert into ` + "`safenet`.`approval_plan_ship_tmp`" + `
databricks: execution error: failed to execute query: unexpected operation state ERROR_STATE: [COPY_INTO_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to ` + "`abl_analytics_ws_prd`.`safenet`.`approval_plan_ship_tmp`" + `, the reason is not enough data columns compared to specified columns:
Specified columns: ` + "`approval_plan_id`" + `, ` + "`ship_id`" + `, ` + "`_sling_loaded_at`" + `.
Data columns: .`
	meta := SignMeta{SourceType: dbio.TypeDbMySQL, TargetType: dbio.TypeDbDatabricks}
	sig := SignError(errText, meta)
	if !strings.Contains(sig.Skeleton, "code:copy_into_column_arity_mismatch.not_enough_data_columns") {
		t.Fatalf("expected arity code:\n%s", sig.Skeleton)
	}
	if strings.Contains(sig.Skeleton, "safenet") || strings.Contains(sig.Skeleton, "approval_plan") {
		t.Fatalf("idents not scrubbed:\n%s", sig.Skeleton)
	}
}

func TestSignError_IdentAndTempTable(t *testing.T) {
	errText := `~ could not prepare Tx: COPY "public"."lxp_cpu_throttled_percentage_tmp" ("app") FROM STDIN
pq: relation "public.lxp_cpu_throttled_percentage_tmp" does not exist
~ Error executing: create unique index if not exists tempSipc0_idx on tempSipc0 ("timestamp")
`
	meta := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	sig := SignError(errText, meta)
	if strings.Contains(sig.Skeleton, "tempSipc0") {
		t.Fatalf("temp table not scrubbed:\n%s", sig.Skeleton)
	}
	if strings.Contains(sig.Skeleton, "lxp_cpu") {
		t.Fatalf("quoted ident not scrubbed:\n%s", sig.Skeleton)
	}
	if !strings.Contains(sig.Skeleton, "<temp>") || !strings.Contains(sig.Skeleton, "<ident>") {
		t.Fatalf("expected placeholders:\n%s", sig.Skeleton)
	}
}

func TestSignError_Empty(t *testing.T) {
	sig := SignError("", SignMeta{})
	if sig.Skeleton != "unknown_error" {
		t.Fatalf("got skeleton %q", sig.Skeleton)
	}
	if sig.PatternMaterial != "v1p|unknown_error" {
		t.Fatalf("pattern material: %q", sig.PatternMaterial)
	}
	if sig.EdgeMaterial != "v1e|-|-|unknown_error" {
		t.Fatalf("edge material: %q", sig.EdgeMaterial)
	}
	if len(sig.ID) != CompositeIDLen {
		t.Fatalf("id: %q", sig.ID)
	}
	// Known vector: empty meta + unknown_error
	if sig.ID != "fb2398c014bc4249" {
		t.Fatalf("empty id vector: got %s want fb2398c014bc4249", sig.ID)
	}
}

func TestSignError_MetaCaseInsensitive(t *testing.T) {
	errText := "no stream columns detected"
	a := SignError(errText, SignMeta{SourceType: dbio.Type("Postgres"), TargetType: dbio.Type("SNOWFLAKE")})
	b := SignError(errText, SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbSnowflake})
	if a.ID != b.ID {
		t.Fatalf("meta case should not matter: %s vs %s", a.ID, b.ID)
	}
}

func TestComposite_KnownVector(t *testing.T) {
	skel := "could not writetodb\nno stream columns detected"
	meta := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	// Materials
	pMat := PatternMaterial(skel)
	eMat := EdgeMaterial(skel, meta)
	if pMat != "v1p|"+skel {
		t.Fatalf("pattern material: %q", pMat)
	}
	if eMat != "v1e|prometheus|postgres|"+skel {
		t.Fatalf("edge material: %q", eMat)
	}
	// Known digests (python/sha256 vectors)
	if HashPart(pMat) != "97d84811" {
		t.Fatalf("pattern part: got %s want 97d84811", HashPart(pMat))
	}
	if HashPart(eMat) != "5aede62c" {
		t.Fatalf("edge part: got %s want 5aede62c", HashPart(eMat))
	}
	// Full SignError on framed error yields same skeleton materials
	errText := `--- task_run.go:140 func2 ---
~ Could not WriteToDb
--- task_run_write.go:168 WriteToDb ---
no stream columns detected`
	sig := SignError(errText, meta)
	if sig.PatternID != "97d84811" || sig.EdgeID != "5aede62c" {
		t.Fatalf("parts: pattern=%s edge=%s", sig.PatternID, sig.EdgeID)
	}
	if sig.ID != "97d848115aede62c" {
		t.Fatalf("composite: got %s", sig.ID)
	}
	if sig.IDDashed() != "97d84811-5aede62c" {
		t.Fatalf("dashed: %s", sig.IDDashed())
	}
}

func TestShortLabel(t *testing.T) {
	lab := ShortLabel("could not writetodb\nno stream columns detected")
	if lab != "no_stream_columns_detected" {
		t.Fatalf("label: %q", lab)
	}
	lab2 := ShortLabel("unknown_error")
	if lab2 != "unknown_error" {
		t.Fatalf("label: %q", lab2)
	}
}

func TestDisplay(t *testing.T) {
	sig := SignError("no stream columns detected", SignMeta{
		SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres,
	})
	d := sig.Display()
	if !strings.Contains(d, sig.IDDashed()) || !strings.Contains(d, "prometheus→postgres") {
		t.Fatalf("display: %q", d)
	}
	if !strings.Contains(d, "-") {
		t.Fatalf("display should use dashed composite: %q", d)
	}
}

func TestParseSignatureID(t *testing.T) {
	compact, patternOnly, err := ParseSignatureID("97d848115aede62c")
	if err != nil || compact != "97d848115aede62c" || patternOnly {
		t.Fatalf("16hex: %q %v %v", compact, patternOnly, err)
	}
	compact, patternOnly, err = ParseSignatureID("97d84811-5aede62c")
	if err != nil || compact != "97d848115aede62c" || patternOnly {
		t.Fatalf("dashed: %q %v %v", compact, patternOnly, err)
	}
	compact, patternOnly, err = ParseSignatureID("97d84811  (prometheus→postgres · no_stream_columns)")
	if err != nil || compact != "97d84811" || !patternOnly {
		t.Fatalf("pattern from display: %q %v %v", compact, patternOnly, err)
	}
	// Display line with dashed id
	compact, patternOnly, err = ParseSignatureID("97d84811-5aede62c  (prometheus→postgres · x)")
	if err != nil || compact != "97d848115aede62c" || patternOnly {
		t.Fatalf("dashed display: %q %v %v", compact, patternOnly, err)
	}
	if _, _, err := ParseSignatureID("not-a-sig"); err == nil {
		t.Fatal("expected error")
	}
	if _, _, err := ParseSignatureID("abcd"); err == nil {
		t.Fatal("expected error for short hex")
	}
}

func TestSkeleton_DropsSectionBanners(t *testing.T) {
	errText := `~ failure running replication
--------------------------- lxp_app_oomkilled_count ---------------------------
~ Could not WriteToDb
no stream columns detected
--------------------------- lxp_app_middleware_pod_oomkilled_count ---------------------------
~ Could not WriteToDb
no stream columns detected`
	skel := Skeleton(errText)
	if strings.Contains(skel, "lxp_app_oomkilled") {
		t.Fatalf("section banner leaked:\n%s", skel)
	}
	if strings.Count(skel, "no stream columns detected") < 1 {
		t.Fatalf("missing leaf:\n%s", skel)
	}
}

func TestVariousErrorShapes(t *testing.T) {
	cases := []struct {
		name string
		err  string
		meta SignMeta
		want []string
		deny []string
	}{
		{
			name: "ssl_not_enabled",
			err:  "--- sling_run.go:442 runTask ---\n~ could not connect to database\npq: SSL is not enabled on the server",
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbPostgres},
			want: []string{"ssl is not enabled"},
			deny: []string{"sling_run.go"},
		},
		{
			name: "update_key_missing",
			err:  "--- task_run_read.go:149 ReadFromDB ---\ndid not find update_key: modified_at",
			meta: SignMeta{SourceType: dbio.TypeDbMySQL, TargetType: dbio.TypeDbSnowflake},
			want: []string{"did not find update_key"},
			deny: []string{"task_run_read"},
		},
		{
			name: "table_not_found_sqlserver",
			err:  "--- database_sqlserver.go:543 GetTableColumns ---\ndid not find table or synonym: \"V12PROD\".\"XL\"",
			meta: SignMeta{SourceType: dbio.TypeDbSQLServer, TargetType: dbio.TypeDbClickhouse},
			want: []string{"did not find table or synonym", "<ident>"},
			deny: []string{"V12PROD", "database_sqlserver"},
		},
		{
			name: "not_enough_space",
			err:  "clickhouse [execute]:: 500 code: Code: 243. DB::Exception: Cannot reserve 1.00 MiB, not enough space: While executing WaitForAsyncInsert. (NOT_ENOUGH_SPACE) (version 26.6.2.81 (official build))",
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbClickhouse},
			want: []string{"code:not_enough_space", "code:ch_243", "not enough space"},
			deny: []string{"26.6.2"},
		},
		{
			name: "motherduck_auth",
			err: `~ Failed to execute SQL
Error: unable to open database "md:warehouse": Invalid Input Error: Initialization function "motherduck_duckdb_cpp_init" from file "/root/.duckdb/extensions/v1.4.2/linux_amd64/motherduck.duckdb_extension" threw an exception: "Invalid Error: Request failed: Your request is not authenticated. Please check your MotherDuck token."`,
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbMotherDuck},
			want: []string{"not authenticated", "<path>"},
			deny: []string{"/root/.duckdb"},
		},
		{
			name: "eof_connect",
			err:  "~ could not connect to database(try adding `sslmode=require` or `sslmode=disable`)\nEOF",
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbPostgres},
			want: []string{"could not connect", "eof"},
			deny: []string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := SignError(tc.err, tc.meta)
			if len(sig.ID) != CompositeIDLen {
				t.Fatalf("bad id %q", sig.ID)
			}
			for _, w := range tc.want {
				if !strings.Contains(sig.Skeleton, w) {
					t.Fatalf("skeleton missing %q:\n%s", w, sig.Skeleton)
				}
			}
			for _, d := range tc.deny {
				if strings.Contains(sig.Skeleton, d) {
					t.Fatalf("skeleton has denied %q:\n%s", d, sig.Skeleton)
				}
			}
			if !strings.HasPrefix(sig.PatternMaterial, "v1p|") {
				t.Fatalf("pattern material: %q", sig.PatternMaterial)
			}
			if !strings.HasPrefix(sig.EdgeMaterial, "v1e|") {
				t.Fatalf("edge material: %q", sig.EdgeMaterial)
			}
		})
	}
}

func TestInferredTaskType(t *testing.T) {
	m := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	if m.InferredTaskType() != "db-db" {
		t.Fatalf("got %q", m.InferredTaskType())
	}
	m = SignMeta{SourceType: dbio.TypeFileS3, TargetType: dbio.TypeDbSnowflake}
	if m.InferredTaskType() != "file-db" {
		t.Fatalf("got %q", m.InferredTaskType())
	}
	m = SignMeta{SourceType: dbio.TypeApi, TargetType: dbio.TypeFileLocal}
	if m.InferredTaskType() != "api-file" {
		t.Fatalf("got %q", m.InferredTaskType())
	}
	if (SignMeta{}).InferredTaskType() != "" {
		t.Fatal("empty meta should not infer task type")
	}
}

// --- ClickHouse live parity ----------------------------------------------------

func chQuery(t *testing.T, sql string) []map[string]any {
	t.Helper()
	entry := connection.GetLocalConns().Get("clickhouse_top")
	if entry.Name == "" {
		t.Skip("clickhouse_top connection not configured")
	}
	db, err := entry.Connection.AsDatabase()
	if err != nil {
		t.Skipf("AsDatabase: %v", err)
	}
	if err := db.Connect(); err != nil {
		t.Skipf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	data, err := db.Query(sql)
	if err != nil {
		t.Fatalf("query failed: %v\nsql=%s", err, truncate(sql, 400))
	}
	return data.Records(true)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// TestClickHouseHashMatchesGo proves SHA-256 first-8 hex is identical for the
// same material bytes in Go and ClickHouse (pattern and edge materials).
func TestClickHouseHashMatchesGo(t *testing.T) {
	skel := Skeleton(`--- task_run.go:140 func2 ---
~ Could not WriteToDb
no stream columns detected`)
	meta := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	materials := []string{
		PatternMaterial(skel),
		EdgeMaterial(skel, meta),
		"v1p|unknown_error",
		"v1e|-|-|unknown_error",
		EdgeMaterial(Skeleton("dial tcp 10.0.0.1:5432: connection refused"), SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbMySQL}),
		PatternMaterial(Skeleton(`[COPY_INTO_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] bad columns`)),
	}

	for i, mat := range materials {
		t.Run(fmt.Sprintf("mat_%d", i), func(t *testing.T) {
			goSig := HashPart(mat)
			sql := "SELECT " + chSQLHashPart(mat) + " AS sig"
			rows := chQuery(t, sql)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			chSig := cast.ToString(rows[0]["sig"])
			if chSig != goSig {
				t.Fatalf("hash mismatch\n  go=%s\n  ch=%s\n  material=%q", goSig, chSig, mat)
			}
		})
	}
}

// TestClickHouseSignErrorMatchesGo compares full composite SignError against CH SQL.
func TestClickHouseSignErrorMatchesGo(t *testing.T) {
	cases := []struct {
		name string
		err  string
		meta SignMeta
	}{
		{
			name: "no_stream_columns",
			err: `--- task_run.go:140 func2 ---
--- task_run.go:830 runDbToDb ---
~ Could not WriteToDb
--- task_run_write.go:168 WriteToDb ---
no stream columns detected`,
			meta: SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres},
		},
		{
			name: "connection_refused",
			err: `--- proc.go:283 main ---
~ could not connect to database(try adding sslmode=require or sslmode=disable)
dial tcp 192.168.176.200:5432: connect: connection refused`,
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbPostgres},
		},
		{
			name: "ssl_not_enabled",
			err: `~ could not connect to database
pq: SSL is not enabled on the server`,
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbClickhouse},
		},
		{
			name: "url_timeout",
			err: `~ could not connect to database
Post "https://clickhouse.example.com:443?database=db&default_format=Native": dial tcp 136.41.64.93:443: i/o timeout`,
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbClickhouse},
		},
		{
			name: "empty_meta",
			err:  "no stream columns detected",
			meta: SignMeta{},
		},
		{
			name: "path_scrub",
			err:  `unable to open database "/root/.duckdb/extensions/v1.4.2/linux_amd64/motherduck.duckdb_extension"`,
			meta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbMotherDuck},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goSig := SignError(tc.err, tc.meta)

			// Pattern + edge material hash parity
			for _, part := range []struct {
				name, mat, want string
			}{
				{"pattern", goSig.PatternMaterial, goSig.PatternID},
				{"edge", goSig.EdgeMaterial, goSig.EdgeID},
			} {
				rows := chQuery(t, "SELECT "+chSQLHashPart(part.mat)+" AS sig")
				if cast.ToString(rows[0]["sig"]) != part.want {
					t.Fatalf("%s hash mismatch go=%s ch=%s mat=%q", part.name, part.want, rows[0]["sig"], part.mat)
				}
			}

			// Full composite from CH normalizer
			sqlFull := fmt.Sprintf(
				"SELECT %s AS sig, %s AS pattern_id, %s AS edge_id, %s AS skel",
				chSQLCompositeExpr(chQuote(tc.err), chQuote(string(tc.meta.SourceType)), chQuote(string(tc.meta.TargetType))),
				chSQLPatternExpr(chQuote(tc.err)),
				chSQLEdgeExpr(chQuote(tc.err), chQuote(string(tc.meta.SourceType)), chQuote(string(tc.meta.TargetType))),
				chSkeletonExpr(chQuote(tc.err)),
			)
			rows2 := chQuery(t, sqlFull)
			chSig := cast.ToString(rows2[0]["sig"])
			chPat := cast.ToString(rows2[0]["pattern_id"])
			chEdge := cast.ToString(rows2[0]["edge_id"])
			chSkel := cast.ToString(rows2[0]["skel"])

			if chSkel != goSig.Skeleton {
				t.Fatalf("skeleton mismatch\n--- go ---\n%s\n--- ch ---\n%s", goSig.Skeleton, chSkel)
			}
			if chPat != goSig.PatternID || chEdge != goSig.EdgeID {
				t.Fatalf("parts mismatch go=%s/%s ch=%s/%s", goSig.PatternID, goSig.EdgeID, chPat, chEdge)
			}
			if chSig != goSig.ID {
				t.Fatalf("composite mismatch go=%s ch=%s", goSig.ID, chSig)
			}
		})
	}
}

// TestClickHouseLiveErrors_GoAndCHMatch pulls real plausible_events rows.
func TestClickHouseLiveErrors_GoAndCHMatch(t *testing.T) {
	sql := `
SELECT
  toString(meta_json.error) AS err,
  JSONExtractString(ifNull(task_string, ''), 'source_type') AS source_type,
  JSONExtractString(ifNull(task_string, ''), 'target_type') AS target_type
FROM analytics.plausible_events
WHERE timestamp > now() - INTERVAL 14 DAY
  AND toString(meta_json.error) != ''
  AND length(toString(meta_json.error)) BETWEEN 40 AND 2500
  AND JSONExtractString(ifNull(task_string, ''), 'source_type') != ''
LIMIT 40
`
	rows := chQuery(t, sql)
	if len(rows) == 0 {
		t.Skip("no recent error rows in plausible_events")
	}

	var (
		hashMatches int
		fullMatches int
		fullChecked int
	)

	seenPattern := map[string]struct{}{}
	seenComposite := map[string]struct{}{}
	for i, row := range rows {
		errText := cast.ToString(row["err"])
		meta := SignMeta{
			SourceType: dbio.Type(cast.ToString(row["source_type"])),
			TargetType: dbio.Type(cast.ToString(row["target_type"])),
		}
		goSig := SignError(errText, meta)

		// Always: CH SHA256 of pattern/edge materials == Go parts
		prows := chQuery(t, "SELECT "+chSQLHashPart(goSig.PatternMaterial)+" AS sig")
		erows := chQuery(t, "SELECT "+chSQLHashPart(goSig.EdgeMaterial)+" AS sig")
		if cast.ToString(prows[0]["sig"]) != goSig.PatternID || cast.ToString(erows[0]["sig"]) != goSig.EdgeID {
			t.Errorf("row %d material hash mismatch go=%s/%s ch=%s/%s",
				i, goSig.PatternID, goSig.EdgeID, prows[0]["sig"], erows[0]["sig"])
			continue
		}
		hashMatches++
		seenPattern[goSig.PatternID] = struct{}{}
		seenComposite[goSig.ID] = struct{}{}

		if strings.Contains(goSig.Skeleton, "code:") {
			continue
		}
		fullChecked++
		fullSQL := fmt.Sprintf(
			"SELECT %s AS sig",
			chSQLCompositeExpr(chQuote(errText), chQuote(string(meta.SourceType)), chQuote(string(meta.TargetType))),
		)
		frows := chQuery(t, fullSQL)
		chSig := cast.ToString(frows[0]["sig"])
		if chSig != goSig.ID {
			skelSQL := "SELECT " + chSkeletonExpr(chQuote(errText)) + " AS skel"
			srows := chQuery(t, skelSQL)
			t.Errorf("row %d full sig mismatch go=%s ch=%s\n--- go skel ---\n%s\n--- ch skel ---\n%s\nerr_prefix=%q",
				i, goSig.ID, chSig, goSig.Skeleton, cast.ToString(srows[0]["skel"]), truncate(errText, 160))
			continue
		}
		fullMatches++
	}

	t.Logf("live sample: rows=%d material_hash_ok=%d full_checked=%d full_ok=%d unique_patterns=%d unique_composites=%d",
		len(rows), hashMatches, fullChecked, fullMatches, len(seenPattern), len(seenComposite))

	if hashMatches != len(rows) {
		t.Fatalf("material hash parity failed for %d/%d rows", len(rows)-hashMatches, len(rows))
	}
	if fullChecked > 0 && fullMatches < fullChecked {
		t.Fatalf("full CH normalizer parity failed for %d/%d non-code rows", fullChecked-fullMatches, fullChecked)
	}
}

// TestClickHouseLiveTopPatternStable: same skeleton, renumbered frames, same composite;
// different target shares pattern only.
func TestClickHouseLiveTopPatternStable(t *testing.T) {
	errA := `--- task_run.go:140 func2 ---
--- task_run.go:830 runDbToDb ---
~ Could not WriteToDb
--- task_run_write.go:168 WriteToDb ---
no stream columns detected`
	errB := `--- task_run.go:125 func2 ---
--- task_run.go:881 runDbToDb ---
~ Could not WriteToDb
--- task_run_write.go:450 WriteToDb ---
no stream columns detected`
	meta := SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres}
	sa := SignError(errA, meta)
	sb := SignError(errB, meta)
	if sa.ID != sb.ID || sa.PatternID != sb.PatternID {
		t.Fatalf("line renumber should not split: %s vs %s", sa.ID, sb.ID)
	}

	// Different target → same pattern, different edge/composite
	sc := SignError(errA, SignMeta{SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbSnowflake})
	if sc.PatternID != sa.PatternID {
		t.Fatalf("pattern should match across targets: %s vs %s", sa.PatternID, sc.PatternID)
	}
	if sc.ID == sa.ID || sc.EdgeID == sa.EdgeID {
		t.Fatalf("edge/composite should differ by target")
	}

	// CH agrees on both composites
	for _, errText := range []string{errA, errB} {
		sql := fmt.Sprintf("SELECT %s AS sig, %s AS pattern_id",
			chSQLCompositeExpr(chQuote(errText), chQuote(string(meta.SourceType)), chQuote(string(meta.TargetType))),
			chSQLPatternExpr(chQuote(errText)))
		rows := chQuery(t, sql)
		if cast.ToString(rows[0]["sig"]) != sa.ID {
			t.Fatalf("CH composite: got %s want %s", rows[0]["sig"], sa.ID)
		}
		if cast.ToString(rows[0]["pattern_id"]) != sa.PatternID {
			t.Fatalf("CH pattern: got %s want %s", rows[0]["pattern_id"], sa.PatternID)
		}
	}
	t.Logf("stable composite for no_stream_columns: %s (%s) pattern=%s", sa.ID, sa.Display(), sa.PatternID)
}

// --- ClickHouse SQL helpers (test-only parity with Go SignError) ---------------

func chSQLHashPart(material string) string {
	return fmt.Sprintf("lower(substring(hex(SHA256(%s)), 1, %d))", chQuote(material), PartIDLen)
}

func chSQLPatternExpr(errExpr string) string {
	skel := chSkeletonExpr(errExpr)
	mat := fmt.Sprintf("concat(%s, '|', %s)", chQuote(PatternMaterialPrefix), skel)
	return fmt.Sprintf("lower(substring(hex(SHA256(%s)), 1, %d))", mat, PartIDLen)
}

func chSQLEdgeExpr(errExpr, sourceExpr, targetExpr string) string {
	skel := chSkeletonExpr(errExpr)
	src := fmt.Sprintf("if(empty(trimBoth(%s)), '-', lower(trimBoth(%s)))", sourceExpr, sourceExpr)
	tgt := fmt.Sprintf("if(empty(trimBoth(%s)), '-', lower(trimBoth(%s)))", targetExpr, targetExpr)
	mat := fmt.Sprintf(
		"concat(%s, '|', %s, '|', %s, '|', %s)",
		chQuote(EdgeMaterialPrefix), src, tgt, skel,
	)
	return fmt.Sprintf("lower(substring(hex(SHA256(%s)), 1, %d))", mat, PartIDLen)
}

func chSQLCompositeExpr(errExpr, sourceExpr, targetExpr string) string {
	return fmt.Sprintf("concat(%s, %s)",
		chSQLPatternExpr(errExpr),
		chSQLEdgeExpr(errExpr, sourceExpr, targetExpr),
	)
}

func chSkeletonExpr(errExpr string) string {
	steps := []struct {
		pat, repl string
	}{
		{`(?m)^---\s+\S+\.go:\d+\s+.*\n?`, ``},
		{`(?m)^-{3,}[^-].*-{3,}\s*\n?`, ``},
		{`(?m)^~\s*`, ``},
		{`(?i)"(?:https?|s3|gs|file|azure|abfs|abfss)://[^"]*"`, `<url>`},
		{`"(?:/|~/)[^"]*"`, `<path>`},
		{`(?i)\b(?:https?|s3|gs|file|azure|abfs|abfss)://[^\s"'<>]+`, `<url>`},
		{`\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b`, `<ts>`},
		{`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`, `<id>`},
		{`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b`, `<ip>`},
		{`\(version\s+[^)]+\)`, ``},
		{`\btemp[A-Za-z0-9]{3,}\b`, `<temp>`},
		{`\b(?:exec_[A-Za-z0-9]+|[0-9A-Za-z]{24,}|[0-9a-fA-F]{16,})\b`, `<id>`},
		{`"[^"\s]{1,256}"`, `<ident>`},
		{"`[^`\\s]{1,256}`", `<ident>`},
		{`(^|[\s"'=(])(/[^\s"'<>]+)`, `\1<path>`},
		{`\b\d{3,}\b`, `<n>`},
	}

	expr := errExpr
	for _, s := range steps {
		expr = fmt.Sprintf("replaceRegexpAll(%s, %s, %s)", expr, chQuote(s.pat), chQuote(s.repl))
	}

	lineMap := fmt.Sprintf(
		`arrayMap(x -> trimBoth(replaceRegexpAll(lower(x), %s, ' ')), splitByChar('\n', %s))`,
		chQuote(`[ \t]+`),
		expr,
	)
	filtered := fmt.Sprintf(`arrayFilter(x -> x != '', %s)`, lineMap)
	joined := fmt.Sprintf(`arrayStringConcat(%s, '\n')`, filtered)
	return fmt.Sprintf(`if(%s = '', 'unknown_error', %s)`, joined, joined)
}

func chQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func TestWriteFailureSnapshotReadableByInvestigate(t *testing.T) {
	withTempHomeDir(t)

	WriteFailureSnapshot(FailureSnapshot{
		ExecID:     "exec_test123",
		ErrMsg:     "column missing: email_verified",
		ConfigPath: "./r.yaml",
	})

	dir := findLocalExecDir("exec_test123")
	if dir == "" {
		t.Fatal("findLocalExecDir returned empty after WriteFailureSnapshot")
	}
	wantDir := filepath.Join(ExecutionsDir(), "exec_test123")
	if dir != wantDir {
		t.Fatalf("snapshot dir = %q want %q", dir, wantDir)
	}
	errBytes, err := os.ReadFile(filepath.Join(dir, "error.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(errBytes), "email_verified") {
		t.Fatalf("error.txt = %q", errBytes)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
		t.Fatal(err)
	}

	execs, err := ListLocalExecs()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range execs {
		if e.ID == "exec_test123" {
			found = true
			if e.Status != "err" {
				t.Fatalf("status = %q, want err", e.Status)
			}
			if e.ConfigPath != "./r.yaml" {
				t.Fatalf("config_path = %q", e.ConfigPath)
			}
		}
	}
	if !found {
		t.Fatal("ListLocalExecs did not include written exec")
	}
}

func TestPrintFailureFooterRespectsEnv(t *testing.T) {
	t.Setenv("SLING_ASSIST_HINT", "false")
	PrintFailureFooter(FailureFooterOpts{ExecID: "exec_x", ErrMsg: "boom"})
	MaybePrintErrorHint("exec_x")
}

func TestPrintFailureFooterErrorFlag(t *testing.T) {
	withTempHomeDir(t)
	t.Setenv("SLING_ASSIST_HINT", "true")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	PrintFailureFooter(FailureFooterOpts{ExecID: "exec_x", ErrMsg: "boom"})
	_ = w.Close()
	os.Stderr = old

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "sling assist --id exec_x") {
		t.Fatalf("missing --id hint in footer: %q", out)
	}
	// The signature is agent context (meta.json), never user-facing output.
	if strings.Contains(out, "error_signature") || strings.Contains(out, "sling assist error ") {
		t.Fatalf("signature leaked into footer: %q", out)
	}
	if strings.Contains(out, "--exec ") {
		t.Fatalf("stale --exec flag in footer: %q", out)
	}
	// One hint line, plus one leading blank.
	lines := []string{}
	for _, ln := range strings.Split(strings.Trim(out, "\n"), "\n") {
		if strings.TrimSpace(ln) != "" {
			lines = append(lines, ln)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("footer must be one line, got %d: %q", len(lines), out)
	}
	if !strings.HasPrefix(lines[0], "  ") {
		t.Fatalf("footer line must be indented: %q", lines[0])
	}
}

func TestShortExecIDTrims(t *testing.T) {
	long := "3IGpCdEfUbXfaOlpYjrXXy2uKbL"
	if got := ShortExecID(long); got != "3IGpCdEf" {
		t.Fatalf("ShortExecID = %q", got)
	}
	if got := ShortExecID("abc"); got != "abc" {
		t.Fatalf("short id must pass through, got %q", got)
	}
}

func TestWriteFailureSnapshotConnName(t *testing.T) {
	withTempHomeDir(t)
	WriteFailureSnapshot(FailureSnapshot{
		ExecID:   "exec_conntest",
		ErrMsg:   "connection refused",
		ConnName: "MY_PG",
	})
	dir := findLocalExecDir("exec_conntest")
	if dir == "" {
		t.Fatal("missing snapshot dir")
	}
	if _, err := os.Stat(filepath.Join(dir, "config.snapshot.yaml")); !os.IsNotExist(err) {
		t.Fatal("conns test snapshot must not include config.snapshot.yaml")
	}
	le, err := ResolveLocalExec("exec_conntest")
	if err != nil {
		t.Fatal(err)
	}
	if le.ConnName != "MY_PG" {
		t.Fatalf("ConnName=%q", le.ConnName)
	}
	if le.ConfigPath != "" {
		t.Fatalf("ConfigPath should be empty, got %q", le.ConfigPath)
	}
	if le.displayObject() != "MY_PG" {
		t.Fatalf("displayObject=%q", le.displayObject())
	}
}

func TestWriteFailureSnapshotIncludesSignature(t *testing.T) {
	withTempHomeDir(t)
	errMsg := `--- task_run.go:140 func2 ---
~ Could not WriteToDb
no stream columns detected`
	WriteFailureSnapshot(FailureSnapshot{
		ExecID: "sigExec1",
		ErrMsg: errMsg,
		SignMeta: SignMeta{
			SourceType: dbio.TypeDbPrometheus,
			TargetType: dbio.TypeDbPostgres,
		},
	})
	dir := findLocalExecDir("sigExec1")
	if dir == "" {
		t.Fatal("exec dir missing")
	}
	body, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{}
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatal(err)
	}
	wantSig := SignError(errMsg, SignMeta{
		SourceType: dbio.TypeDbPrometheus, TargetType: dbio.TypeDbPostgres,
	})
	if cast.ToString(meta["error_signature"]) != wantSig.ID {
		t.Fatalf("error_signature = %v want %s", meta["error_signature"], wantSig.ID)
	}
	if cast.ToString(meta["error_pattern_id"]) != wantSig.PatternID {
		t.Fatalf("pattern_id = %v", meta["error_pattern_id"])
	}
	if cast.ToString(meta["error_edge_id"]) != wantSig.EdgeID {
		t.Fatalf("edge_id = %v", meta["error_edge_id"])
	}
	if cast.ToString(meta["error_algorithm"]) != "v1" {
		t.Fatalf("algorithm = %v", meta["error_algorithm"])
	}
}

func TestLookupError(t *testing.T) {
	// Pattern-only (8 hex)
	r, err := LookupError("97d84811")
	if err != nil {
		t.Fatal(err)
	}
	if r.Signature != "97d84811" || !r.PatternOnly || r.PatternID != "97d84811" {
		t.Fatalf("%+v", r)
	}
	// Full composite
	r2, err := LookupError("97d84811-5aede62c")
	if err != nil || r2.Signature != "97d848115aede62c" || r2.PatternOnly {
		t.Fatalf("dashed composite: %+v err=%v", r2, err)
	}
	if r2.PatternID != "97d84811" || r2.EdgeID != "5aede62c" {
		t.Fatalf("parts: %+v", r2)
	}
	// Display form
	r3, err := LookupError("97d84811-5aede62c  (prometheus→postgres · no_stream_columns)")
	if err != nil || r3.Signature != "97d848115aede62c" {
		t.Fatalf("display form: %+v err=%v", r3, err)
	}
	if _, err := LookupError("not-a-sig"); err == nil {
		t.Fatal("expected invalid signature error")
	}
}

func TestSanitizeLogForPromptCapsAndEscapesFences(t *testing.T) {
	prev := env.Env
	t.Cleanup(func() { env.Env = prev })
	env.Env = &env.EnvFile{Connections: map[string]map[string]any{}}

	in := "before\n```\ninject\n```\nafter"
	out := sanitizeLogForPrompt(in, 0)
	if strings.Contains(out, "```") {
		t.Fatalf("fence not escaped: %q", out)
	}
	if !strings.Contains(out, "'''") {
		t.Fatalf("expected escaped fence: %q", out)
	}
	big := strings.Repeat("x", maxErrorTailBytes+1000)
	capped := sanitizeLogForPrompt(big, maxErrorTailBytes)
	if len(capped) > maxErrorTailBytes+len("[...truncated...]\n")+10 {
		t.Fatalf("cap too large: %d", len(capped))
	}
	if !strings.HasPrefix(capped, "[...truncated...]\n") {
		t.Fatalf("missing truncation marker: %q", capped[:40])
	}
}

func TestSanitizeLogForPromptScrubsSecrets(t *testing.T) {
	prev := env.Env
	t.Cleanup(func() { env.Env = prev })
	env.Env = &env.EnvFile{Connections: map[string]map[string]any{
		"MY_PG": {
			"type":     "postgres",
			"password": "super-secret-pass",
			"secrets": map[string]any{
				"api_key": "nested-api-key",
			},
		},
	}}
	out := sanitizeLogForPrompt("failed auth super-secret-pass and nested-api-key", 0)
	if strings.Contains(out, "super-secret-pass") {
		t.Fatalf("password leaked: %q", out)
	}
	if strings.Contains(out, "nested-api-key") {
		t.Fatalf("nested secret leaked: %q", out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("expected redaction marker: %q", out)
	}
}

func TestSensitivityClassify(t *testing.T) {
	cases := []struct {
		path string
		want Sensitivity
	}{
		{"/home/u/.sling/env.yaml", SensitivitySecret},
		{"/home/u/.claude.json", SensitivitySecret},
		{"/tmp/settings.json.backup", SensitivitySecret},
		{"/home/u/.sling/assist/errors/exec_x/meta.json", SensitivityPublic},
		{"/home/u/.sling/assist/errors/exec_x/error.txt", SensitivityInternal},
		{"/home/u/.agents/skills/sling/SKILL.md", SensitivityPublic},
		{"/unknown/random/file.txt", SensitivityInternal}, // default
	}
	for _, tc := range cases {
		if got := ClassifyPath(tc.path); got != tc.want {
			t.Errorf("ClassifyPath(%q)=%s want %s", tc.path, got, tc.want)
		}
	}
}

func TestSensitivityManifestNonEmpty(t *testing.T) {
	m := SensitivityManifest()
	if len(m) < 5 {
		t.Fatalf("manifest too small: %d", len(m))
	}
	ids := map[string]bool{}
	for _, c := range m {
		if c.ID == "" || c.Glob == "" || c.Reason == "" {
			t.Fatalf("incomplete entry: %+v", c)
		}
		if ids[c.ID] {
			t.Fatalf("duplicate id %s", c.ID)
		}
		ids[c.ID] = true
	}
}

func TestLegacyExecDirStillReadable(t *testing.T) {
	withTempHomeDir(t)
	id := "legacy_exec1"
	dir := filepath.Join(ErrorsDir(), id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := `{"exec_id":"` + id + `","exit_code":1,"object":"r.yaml"}`
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := findLocalExecDir(id); got != dir {
		t.Fatalf("findLocalExecDir = %q want %q", got, dir)
	}
	execs, err := ListLocalExecs()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range execs {
		if e.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatal("ListLocalExecs missed legacy exec dir")
	}
}

func TestReservedErrorDirNamesNotListed(t *testing.T) {
	withTempHomeDir(t)
	_ = ExecutionsDir()
	if err := os.MkdirAll(filepath.Join(ErrorsDir(), "signatures"), 0o755); err != nil {
		t.Fatal(err)
	}
	execs, err := ListLocalExecs()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range execs {
		if e.ID == "executions" || e.ID == "signatures" {
			t.Fatalf("reserved name listed as exec: %q", e.ID)
		}
	}
}

func TestLookupLocalExecPrefixAndAmbiguity(t *testing.T) {
	withTempHomeDir(t)
	for _, id := range []string{"abc111", "abc222", "zzz999"} {
		dir := filepath.Join(ErrorsDir(), id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		meta := `{"exec_id":"` + id + `","exit_code":1,"object":"r.yaml"}`
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(meta), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if le, ok := LookupLocalExec("zzz999"); !ok || le.ID != "zzz999" {
		t.Fatalf("full id: %+v ok=%v", le, ok)
	}
	if le, ok := LookupLocalExec("zzz"); !ok || le.ID != "zzz999" {
		t.Fatalf("unique prefix: %+v ok=%v", le, ok)
	}
	if _, ok := LookupLocalExec("abc"); ok {
		t.Fatal("ambiguous prefix must not resolve")
	}
	if _, err := ResolveLocalExec("abc"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("want ambiguous error, got %v", err)
	}
	if _, err := ResolveLocalExec("nope"); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("want unknown error, got %v", err)
	}
}

func TestWriteFailureSnapshotKeepsRunLog(t *testing.T) {
	withTempHomeDir(t)
	WriteFailureSnapshot(FailureSnapshot{
		ExecID: "exec_runlog",
		ErrMsg: "boom",
		RunLog: "DBG opened conn\nINF execution failed",
	})
	dir := filepath.Join(ExecutionsDir(), "exec_runlog")
	b, err := os.ReadFile(filepath.Join(dir, "stderr.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "DBG opened conn") {
		t.Fatalf("stderr.log missing run log: %q", string(b))
	}
	// error.txt keeps the error only, so runLogExcerpt sees them as distinct.
	e, _ := os.ReadFile(filepath.Join(dir, "error.txt"))
	if string(e) == string(b) {
		t.Fatal("stderr.log must not duplicate error.txt when a run log exists")
	}
	if got := runLogExcerpt(LocalExec{LogDir: dir}); !strings.Contains(got, "execution failed") {
		t.Fatalf("runLogExcerpt: %q", got)
	}
}

func TestRunLogExcerptEmptyWhenMirrored(t *testing.T) {
	withTempHomeDir(t)
	// No RunLog: stderr.log mirrors error.txt, so nothing extra reaches the prompt.
	WriteFailureSnapshot(FailureSnapshot{ExecID: "exec_mirror", ErrMsg: "boom"})
	dir := filepath.Join(ExecutionsDir(), "exec_mirror")
	if got := runLogExcerpt(LocalExec{LogDir: dir}); got != "" {
		t.Fatalf("want empty excerpt for mirrored log, got %q", got)
	}
}

func TestAutoTrimExecsKeepsNewest(t *testing.T) {
	withTempHomeDir(t)

	total := ExecsMaxEntries + 10
	base := time.Now().Add(-time.Duration(total) * time.Hour)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("exec_%03d", i)
		dir := filepath.Join(ExecutionsDir(), id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		// later index = newer
		when := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(dir, when, when); err != nil {
			t.Fatal(err)
		}
	}

	if err := AutoTrimExecs(); err != nil {
		t.Fatal(err)
	}

	ids, err := listLocalExecIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != ExecsMaxEntries {
		t.Fatalf("kept %d snapshots, want %d", len(ids), ExecsMaxEntries)
	}
	// the 10 oldest must be gone, the newest must stay
	if dir := findLocalExecDir("exec_000"); dir != "" {
		t.Fatalf("oldest snapshot survived: %s", dir)
	}
	if dir := findLocalExecDir(fmt.Sprintf("exec_%03d", total-1)); dir == "" {
		t.Fatal("newest snapshot was trimmed")
	}
}

func TestAutoTrimExecsNoopUnderCap(t *testing.T) {
	withTempHomeDir(t)

	for i := 0; i < 5; i++ {
		WriteFailureSnapshot(FailureSnapshot{
			ExecID: fmt.Sprintf("exec_keep%d", i),
			ErrMsg: "boom",
		})
	}
	if err := AutoTrimExecs(); err != nil {
		t.Fatal(err)
	}
	ids, err := listLocalExecIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("kept %d snapshots, want 5", len(ids))
	}
}

func TestAutoTrimExecsSkipsReservedDirs(t *testing.T) {
	withTempHomeDir(t)

	total := ExecsMaxEntries + 5
	base := time.Now().Add(-time.Duration(total) * time.Hour)
	for i := 0; i < total; i++ {
		// legacy layout: errors/<id>/
		dir := filepath.Join(ErrorsDir(), fmt.Sprintf("legacy_%03d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		when := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(dir, when, when); err != nil {
			t.Fatal(err)
		}
	}
	execsDir := ExecutionsDir() // reserved, must survive

	if err := AutoTrimExecs(); err != nil {
		t.Fatal(err)
	}

	if !g.PathExists(execsDir) {
		t.Fatal("executions dir was removed")
	}
	ids, err := listLocalExecIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != ExecsMaxEntries {
		t.Fatalf("kept %d snapshots, want %d", len(ids), ExecsMaxEntries)
	}
}

func TestWriteFailureSnapshotTrims(t *testing.T) {
	withTempHomeDir(t)

	base := time.Now().Add(-time.Duration(ExecsMaxEntries+1) * time.Hour)
	for i := 0; i < ExecsMaxEntries; i++ {
		dir := filepath.Join(ExecutionsDir(), fmt.Sprintf("exec_old%03d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		when := base.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(dir, when, when); err != nil {
			t.Fatal(err)
		}
	}

	WriteFailureSnapshot(FailureSnapshot{ExecID: "exec_newest", ErrMsg: "boom"})

	ids, err := listLocalExecIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != ExecsMaxEntries {
		t.Fatalf("kept %d snapshots, want %d", len(ids), ExecsMaxEntries)
	}
	if dir := findLocalExecDir("exec_newest"); dir == "" {
		t.Fatal("new snapshot was trimmed by its own write")
	}
	if dir := findLocalExecDir("exec_old000"); dir != "" {
		t.Fatalf("oldest snapshot survived: %s", dir)
	}
}
