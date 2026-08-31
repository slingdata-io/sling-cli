package build

import (
	"fmt"
	"strings"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// executeModelTests runs declarative frontmatter data tests against the
// materialized table. Each test compiles to a SELECT count(*) that must be 0.
func (e *Executor) executeModelTests(model *Model) error {
	if len(model.Config.Tests) == 0 {
		g.Debug("model '%s': no tests defined", model.Name)
		return nil
	}

	quoted, err := e.quoteFullTableName(model.FullTableName)
	if err != nil {
		return err
	}

	for i, raw := range model.Config.Tests {
		testSQL, label, err := compileDataTest(raw, quoted, e.DbConn.Quote)
		if err != nil {
			return g.Error(err, "model '%s' test %d", model.Name, i+1)
		}
		g.Debug("running test %q on %s: %s", label, model.Name, testSQL)

		data, err := e.DbConn.Query(testSQL)
		if err != nil {
			return g.Error(err, "model '%s' test %q failed to execute", model.Name, label)
		}
		if len(data.Rows) == 0 || len(data.Rows[0]) == 0 {
			continue
		}
		cnt := cast.ToInt64(data.Rows[0][0])
		if cnt > 0 {
			return g.Error("model '%s' test %q failed: %d violating row(s)", model.Name, label, cnt)
		}
	}
	return nil
}

// compileDataTest turns a frontmatter test entry into a failure-count SQL query.
// Supported forms (YAML):
//
//	- not_null: [id, customer_id]
//	- unique: [id]
//	- unique: id
//	- accepted_values: {column: status, values: [a, b]}
//	- expr: sum(amount) >= 0
//	- {not_null: id}   (single column string)
func compileDataTest(raw any, quotedTable string, quote func(string) string) (sql, label string, err error) {
	m, ok := raw.(map[string]any)
	if !ok {
		// YAML may produce map[any]any
		if m2, ok2 := raw.(map[any]any); ok2 {
			m = make(map[string]any, len(m2))
			for k, v := range m2 {
				m[cast.ToString(k)] = v
			}
		} else {
			return "", "", g.Error("test entry must be a mapping, got %T", raw)
		}
	}

	if v, ok := m["not_null"]; ok {
		cols := toStringList(v)
		if len(cols) == 0 {
			return "", "", g.Error("not_null requires one or more columns")
		}
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = quote(c) + " IS NULL"
		}
		label = "not_null(" + strings.Join(cols, ", ") + ")"
		sql = fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", quotedTable, strings.Join(parts, " OR "))
		return sql, label, nil
	}

	if v, ok := m["unique"]; ok {
		cols := toStringList(v)
		if len(cols) == 0 {
			return "", "", g.Error("unique requires one or more columns")
		}
		qcols := make([]string, len(cols))
		for i, c := range cols {
			qcols[i] = quote(c)
		}
		colList := strings.Join(qcols, ", ")
		label = "unique(" + strings.Join(cols, ", ") + ")"
		sql = fmt.Sprintf(
			"SELECT count(*) FROM (SELECT %s FROM %s GROUP BY %s HAVING count(*) > 1) _sling_uniq",
			colList, quotedTable, colList,
		)
		return sql, label, nil
	}

	if v, ok := m["accepted_values"]; ok {
		av, ok := v.(map[string]any)
		if !ok {
			if m2, ok2 := v.(map[any]any); ok2 {
				av = make(map[string]any, len(m2))
				for k, val := range m2 {
					av[cast.ToString(k)] = val
				}
			} else {
				return "", "", g.Error("accepted_values must be a mapping with column and values")
			}
		}
		col := cast.ToString(av["column"])
		vals := toStringList(av["values"])
		if col == "" || len(vals) == 0 {
			return "", "", g.Error("accepted_values requires column and values")
		}
		quotedVals := make([]string, len(vals))
		for i, val := range vals {
			quotedVals[i] = "'" + strings.ReplaceAll(val, "'", "''") + "'"
		}
		label = "accepted_values(" + col + ")"
		sql = fmt.Sprintf(
			"SELECT count(*) FROM %s WHERE %s IS NOT NULL AND %s NOT IN (%s)",
			quotedTable, quote(col), quote(col), strings.Join(quotedVals, ", "),
		)
		return sql, label, nil
	}

	if v, ok := m["expr"]; ok {
		expr := strings.TrimSpace(cast.ToString(v))
		if expr == "" {
			return "", "", g.Error("expr test requires a non-empty expression")
		}
		label = "expr(" + expr + ")"
		// Fail count = 1 when expression is false (or null)
		sql = fmt.Sprintf(
			"SELECT CASE WHEN (%s) THEN 0 ELSE 1 END FROM %s",
			expr, quotedTable,
		)
		return sql, label, nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return "", "", g.Error("unknown test type in %v; expected not_null, unique, accepted_values, or expr", keys)
}

// toStringList coerces a string or list of strings.
func toStringList(v any) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			out = append(out, cast.ToString(item))
		}
		return out
	default:
		s := cast.ToString(v)
		if s == "" {
			return nil
		}
		return []string{s}
	}
}
