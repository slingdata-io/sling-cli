package evals

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cast"
)

// KnownGraderNames is the set of grader kinds RunGraders accepts.
var KnownGraderNames = map[string]bool{
	"file_exists":           true,
	"file_absent":           true,
	"yaml_valid":            true,
	"sling":                 true,
	"expected":              true,
	"dry_run":               true,
	"transcript_contains":   true,
	"transcript_absent":     true,
	"transcript_absent_raw": true,
	"yq":                    true,
	"sql_equiv":             true,
	"skeleton":              true,
	"outcome":               true,
	"query":                 true,
	"rows_equal":            true,
	"dag":                   true,
	"tests_pass":            true,
	"api_spec":              true,
}

// KnownConnections is the eval suite connection set.
var KnownConnections = map[string]bool{
	"POSTGRES":    true,
	"MYSQL":       true,
	"CLICKHOUSE":  true,
	"DUCKDB":      true,
	"SQLITE":      true,
	"LOCAL":       true,
	"AWS_S3_TEST": true,
	"MOCK_API":    true,
}

var validTiers = map[string]bool{
	TierSmoke: true,
	TierCore:  true,
	TierDeep:  true,
}

// ValidateCase lints one loaded case. Returns error strings (empty = ok).
func ValidateCase(c Case, fixtures map[string]FixtureDef) []string {
	var errs []string
	folder := filepath.Base(c.CaseDir)
	if c.ID != folder {
		errs = append(errs, fmt.Sprintf("id %q does not match folder %q", c.ID, folder))
	}
	if c.Tier == "" {
		errs = append(errs, "missing tier")
	} else if !validTiers[c.Tier] {
		errs = append(errs, "unknown tier "+c.Tier)
	}
	if c.Gating == nil {
		errs = append(errs, "missing gating")
	}
	if c.Tier == TierSmoke && !contains(c.Arms, "noskills") {
		errs = append(errs, "tier smoke must list noskills in arms")
	}
	for _, f := range c.Fixtures {
		if fixtures != nil {
			if _, ok := fixtures[f]; !ok {
				errs = append(errs, "unknown fixture "+f)
			}
		}
	}
	for _, n := range c.Connections {
		if !KnownConnections[n] {
			errs = append(errs, "unknown connection "+n)
		}
	}
	if needsLiveConn(c) && len(c.Connections) == 0 {
		errs = append(errs, "connections required when a grader or fixture touches a live connection")
	}
	var hasOutcome bool
	checkSpec := func(spec GraderSpec) {
		kind := spec.kind()
		if !KnownGraderNames[kind] {
			errs = append(errs, "unknown grader "+kind)
		}
		if isOutcomeGrader(spec) {
			hasOutcome = true
		}
		errs = append(errs, missingGraderFiles(c, spec)...)
	}
	for _, spec := range c.Graders.Required {
		checkSpec(spec)
	}
	for _, spec := range c.Graders.Optional {
		checkSpec(spec)
	}
	if !c.IsNegative() && !hasOutcome && c.Task != "debug" {
		errs = append(errs, "non-negative case has no outcome grader")
	}
	if usesArtifactToken(c) && c.Artifact != "" {
		base := filepath.Base(c.Artifact)
		if !strings.Contains(c.Intention, base) && !strings.Contains(c.EditPath, base) {
			errs = append(errs, "intention must name artifact "+base)
		}
	}
	for _, spec := range append(append([]GraderSpec{}, c.Graders.Required...), c.Graders.Optional...) {
		if spec.kind() == "query" && strings.Contains(c.Task, "build") {
			m, _ := asStringMap(spec["query"])
			sql := strings.ToLower(cast.ToString(m["sql"]) + " " + cast.ToString(m["equals_query"]))
			if sqlMentionsUnqualifiedBuildTable(sql) {
				errs = append(errs, "build query grader must use a schema-qualified table")
			}
		}
	}
	if strings.Contains(c.Task, "build") && writesSharedConn(c) && len(c.ResetSchemas) == 0 {
		errs = append(errs, "build case on a shared connection must declare reset_schemas")
	}
	for _, sch := range c.ResetSchemas {
		if !validResetSchema.MatchString(sch) {
			errs = append(errs, "reset_schemas entry must be a simple identifier: "+sch)
		}
	}
	if exp := c.ExpectedPath(); exp == "" {
		// expected.yaml is optional for negative / file_absent cases
	}
	seedDir := filepath.Join(c.CaseDir, "seed")
	if st, err := os.Stat(seedDir); err == nil && st.IsDir() {
		// present is fine
	}
	mutDir := filepath.Join(c.CaseDir, "mutants")
	if ents, err := os.ReadDir(mutDir); err != nil || len(ents) == 0 {
		errs = append(errs, "missing mutants/")
	}
	refDir := filepath.Join(c.CaseDir, "reference")
	if ents, err := os.ReadDir(refDir); err == nil {
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".sql") {
				p := filepath.Join(refDir, e.Name())
				if _, err := os.Stat(p); err != nil {
					errs = append(errs, "missing "+p)
				}
			}
		}
	}
	return errs
}

func needsLiveConn(c Case) bool {
	if len(c.Fixtures) > 0 {
		return true
	}
	for _, spec := range append(append([]GraderSpec{}, c.Graders.Required...), c.Graders.Optional...) {
		switch spec.kind() {
		case "query", "rows_equal", "sql_equiv", "dry_run", "outcome", "tests_pass":
			return true
		case "sling":
			cmd := strings.ToLower(cast.ToString(spec["sling"]))
			if strings.Contains(cmd, "api_spec") {
				continue
			}
			if strings.Contains(cmd, " run") || strings.Contains(cmd, " test") {
				return true
			}
		}
	}
	return false
}

func isOutcomeGrader(spec GraderSpec) bool {
	switch spec.kind() {
	case "query", "rows_equal", "tests_pass", "outcome", "dry_run":
		return true
	case "sling":
		cmd := strings.ToLower(cast.ToString(spec["sling"]))
		return strings.Contains(cmd, " test") || strings.Contains(cmd, " run") || strings.Contains(cmd, " compile")
	case "api_spec":
		return true
	}
	return false
}

func missingGraderFiles(c Case, spec GraderSpec) []string {
	var errs []string
	checkRel := func(rel string) {
		if rel == "" || strings.Contains(rel, "{") {
			return
		}
		cands := []string{
			filepath.Join(c.CaseDir, rel),
			filepath.Join(c.CaseDir, "reference", rel),
		}
		for _, p := range cands {
			if _, err := os.Stat(p); err == nil {
				return
			}
		}
		if _, err := os.Stat(rel); err == nil {
			return
		}
		errs = append(errs, "missing referenced file "+rel)
	}
	if spec.kind() == "expected" {
		m, _ := asStringMap(spec["expected"])
		file := cast.ToString(m["file"])
		if file == "" {
			file = "expected.yaml"
		}
		if _, err := os.Stat(filepath.Join(c.CaseDir, file)); err != nil {
			if _, err2 := os.Stat(filepath.Join(c.CaseDir, "expected")); err2 != nil {
				errs = append(errs, "missing referenced file "+file)
			}
		}
	}
	if spec.kind() == "sql_equiv" {
		m, _ := asStringMap(spec["sql_equiv"])
		checkRel(cast.ToString(m["expected_sql"]))
	}
	if spec.kind() == "rows_equal" {
		m, _ := asStringMap(spec["rows_equal"])
		es := cast.ToString(m["expected_sql"])
		if es != "" && !looksLikeSQL(es) {
			checkRel(es)
		}
	}
	if spec.kind() == "query" {
		m, _ := asStringMap(spec["query"])
		eq := cast.ToString(m["equals_query"])
		if eq != "" && !looksLikeSQL(eq) {
			checkRel(eq)
		}
	}
	return errs
}

func usesArtifactToken(c Case) bool {
	check := func(specs []GraderSpec) bool {
		for _, spec := range specs {
			b, _ := yamlBytes(spec)
			if strings.Contains(string(b), "{artifact}") {
				return true
			}
		}
		return false
	}
	return check(c.Graders.Required) || check(c.Graders.Optional)
}

func yamlBytes(v any) ([]byte, error) {
	return []byte(fmt.Sprint(v)), nil
}

var (
	bareBuildTable   = regexp.MustCompile(`(?i)\bfrom\s+(stg_|fct_|dim_|int_|mart_)[a-z0-9_]*\b`)
	validResetSchema = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	sharedBuildConns = map[string]bool{"POSTGRES": true, "MYSQL": true, "CLICKHOUSE": true}
)

func writesSharedConn(c Case) bool {
	for _, n := range c.Connections {
		if sharedBuildConns[strings.ToUpper(n)] {
			return true
		}
	}
	return false
}

func sqlMentionsUnqualifiedBuildTable(sql string) bool {
	return bareBuildTable.MatchString(sql)
}

func looksLikeSQL(s string) bool {
	low := strings.ToLower(strings.TrimSpace(s))
	return strings.HasPrefix(low, "select") || strings.HasPrefix(low, "with") || strings.Contains(s, "\n")
}
