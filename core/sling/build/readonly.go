package build

import (
	"encoding/json"
	"strings"
	"unicode"

	"github.com/flarco/g"
	"github.com/slingdata-io/golyglot"
	"github.com/slingdata-io/sling-cli/core/dbio"
)

const readOnlyErrPrefix = "only read-only queries are allowed over MCP"

// ValidateReadOnlyQuery returns an error unless sql is a single read-only
// statement for the given database type. It parses with golyglot; if the
// parser is unavailable or cannot parse the dialect, a conservative
// keyword guard decides. Neither layer can catch side-effecting functions
// (select my_procedure()) — use a read-only database user for connections
// a project token can reach.
func ValidateReadOnlyQuery(sql string, dbType dbio.Type) error {
	result, err := golyglot.Parse(sql, mapDialect(dbType))
	if err != nil {
		return validateReadOnlyKeywords(sql)
	}

	var stmts []json.RawMessage
	if err := json.Unmarshal(result.AST, &stmts); err != nil {
		return validateReadOnlyKeywords(sql)
	}
	if len(stmts) != 1 {
		return readOnlyErrorf("%d statements", len(stmts))
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(stmts[0], &obj); err != nil {
		return readOnlyErrorf("unparseable statement")
	}
	if len(obj) != 1 {
		return readOnlyErrorf("unparseable statement")
	}

	var typeKey string
	for k := range obj {
		typeKey = k
	}
	if typeKey == "command" {
		if err := validateCommandReadOnly(sql); err != nil {
			return err
		}
	} else if !readOnlyASTKeys[typeKey] {
		return readOnlyErrorf("%s statement", typeKey)
	}

	var node any
	if err := json.Unmarshal(stmts[0], &node); err != nil {
		return readOnlyErrorf("unparseable statement")
	}
	if key := findMutatingASTKey(node); key != "" {
		return readOnlyErrorf("%s", key)
	}
	return nil
}

// WarmParser starts loading the golyglot dylib in the background so the
// first ValidateReadOnlyQuery does not pay the download/load cost.
// Init failure is non-fatal — the keyword fallback covers it.
func WarmParser() {
	go func() {
		_ = golyglot.Init()
	}()
}

func readOnlyErrorf(reason string, args ...any) error {
	if reason == "" {
		return g.Error(readOnlyErrPrefix)
	}
	if len(args) > 0 {
		reason = g.F(reason, args...)
	}
	return g.Error("%s (%s)", readOnlyErrPrefix, reason)
}

var readOnlyASTKeys = map[string]bool{
	"select":        true,
	"union":         true,
	"set_operation": true,
	"intersect":     true,
	"except":        true,
	"values":        true,
	"show":          true,
	"describe":      true,
	"explain":       true,
}

// validateCommandReadOnly allows SHOW/DESCRIBE/DESC/EXPLAIN when golyglot
// emits a generic command node. SET/USE/PRAGMA stay denied.
func validateCommandReadOnly(sql string) error {
	stripped := strings.TrimSpace(stripSQLCommentsAndLiterals(sql))
	for strings.HasSuffix(stripped, ";") {
		stripped = strings.TrimSpace(strings.TrimSuffix(stripped, ";"))
	}
	tokens := sqlWordTokens(stripped)
	if len(tokens) == 0 {
		return readOnlyErrorf("command statement")
	}
	if !readOnlyFirstKeywords[tokens[0]] {
		return readOnlyErrorf("%s statement", strings.ToLower(tokens[0]))
	}
	return nil
}

var mutatingASTKeys = map[string]bool{
	"insert":          true,
	"update":          true,
	"delete":          true,
	"merge":           true,
	"copy":            true,
	"into":            true,
	"truncate":        true,
	"create_table":    true,
	"create_view":     true,
	"create_index":    true,
	"create_schema":   true,
	"create_function": true,
	"drop_table":      true,
	"drop_view":       true,
	"drop_index":      true,
	"drop_schema":     true,
	"alter_table":     true,
}

func findMutatingASTKey(v any) string {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if mutatingASTKeys[k] && !isSkippedMutatingValue(val) {
				return k
			}
			if found := findMutatingASTKey(val); found != "" {
				return found
			}
		}
	case []any:
		for _, item := range x {
			if found := findMutatingASTKey(item); found != "" {
				return found
			}
		}
	}
	return ""
}

func isSkippedMutatingValue(v any) bool {
	if v == nil {
		return true
	}
	if b, ok := v.(bool); ok && !b {
		return true
	}
	if arr, ok := v.([]any); ok && len(arr) == 0 {
		return true
	}
	return false
}

// validateReadOnlyKeywords is a conservative keyword guard used when the
// parser is unavailable or cannot parse the SQL. It cannot catch
// side-effecting functions (SELECT my_procedure()) or dialect exotica —
// use a read-only database user for connections a project token can reach.
// SET/USE are denied: they are session-scoped but can change query
// semantics (schema switching).
func validateReadOnlyKeywords(sql string) error {
	stripped := stripSQLCommentsAndLiterals(sql)
	stripped = strings.TrimSpace(stripped)
	if stripped == "" {
		return g.Error(readOnlyErrPrefix)
	}

	for strings.HasSuffix(stripped, ";") {
		stripped = strings.TrimSpace(strings.TrimSuffix(stripped, ";"))
	}
	if strings.Contains(stripped, ";") {
		return g.Error(readOnlyErrPrefix)
	}

	tokens := sqlWordTokens(stripped)
	if len(tokens) == 0 {
		return g.Error(readOnlyErrPrefix)
	}
	if !readOnlyFirstKeywords[tokens[0]] {
		return g.Error(readOnlyErrPrefix)
	}
	for _, tok := range tokens {
		if _, denied := readOnlyDeniedKeywords[tok]; denied {
			return g.Error(readOnlyErrPrefix)
		}
	}
	return nil
}

var readOnlyFirstKeywords = map[string]bool{
	"SELECT":   true,
	"WITH":     true,
	"SHOW":     true,
	"DESCRIBE": true,
	"DESC":     true,
	"EXPLAIN":  true,
}

var readOnlyDeniedKeywords = map[string]struct{}{
	"INSERT":   {},
	"UPDATE":   {},
	"DELETE":   {},
	"MERGE":    {},
	"UPSERT":   {},
	"TRUNCATE": {},
	"DROP":     {},
	"ALTER":    {},
	"CREATE":   {},
	"REPLACE":  {},
	"GRANT":    {},
	"REVOKE":   {},
	"EXEC":     {},
	"EXECUTE":  {},
	"CALL":     {},
	"COPY":     {},
	"VACUUM":   {},
	"ATTACH":   {},
	"SET":      {},
	"USE":      {},
	"INTO":     {},
	"LOCK":     {},
}

func stripSQLCommentsAndLiterals(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	n := len(sql)
	for i < n {
		if i+1 < n && sql[i] == '-' && sql[i+1] == '-' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			continue
		}
		if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
			i += 2
			for i+1 < n && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				i = n
			}
			b.WriteByte(' ')
			continue
		}
		if sql[i] == '\'' {
			i = skipQuoted(sql, i, '\'')
			b.WriteByte(' ')
			continue
		}
		if sql[i] == '"' {
			i = skipQuoted(sql, i, '"')
			b.WriteByte(' ')
			continue
		}
		if sql[i] == '`' {
			i = skipQuoted(sql, i, '`')
			b.WriteByte(' ')
			continue
		}
		if sql[i] == '$' {
			if tag, ok := dollarTagAt(sql, i); ok {
				end := "$" + tag + "$"
				i += len(end)
				if j := strings.Index(sql[i:], end); j >= 0 {
					i += j + len(end)
				} else {
					i = n
				}
				b.WriteByte(' ')
				continue
			}
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

func skipQuoted(sql string, i int, quote byte) int {
	n := len(sql)
	i++ // opening quote
	for i < n {
		if sql[i] == quote {
			if i+1 < n && sql[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return n
}

func dollarTagAt(sql string, i int) (tag string, ok bool) {
	if i >= len(sql) || sql[i] != '$' {
		return "", false
	}
	j := i + 1
	if j < len(sql) && sql[j] == '$' {
		return "", true
	}
	start := j
	for j < len(sql) {
		r := rune(sql[j])
		if j == start {
			if !unicode.IsLetter(r) && r != '_' {
				return "", false
			}
		} else if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			break
		}
		j++
	}
	if j < len(sql) && sql[j] == '$' {
		return sql[start:j], true
	}
	return "", false
}

func sqlWordTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, strings.ToUpper(cur.String()))
			cur.Reset()
		}
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			cur.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return tokens
}
