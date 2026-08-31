package validate

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cast"
)

// GetOutput renders validate results as a string. --quiet is silent. JSON is the
// default when stdout is not a TTY.
func GetOutput(results []FileResult, opts Options) (string, error) {
	if opts.Quiet {
		return "", nil
	}
	if opts.NDJSON {
		return ndjsonOutput(results)
	}
	if opts.JSON {
		return jsonOutput(results)
	}
	if opts.Detailed {
		return detailedOutput(results), nil
	}
	if !isStdoutTTY() {
		return jsonOutput(results)
	}
	return tableOutput(results), nil
}

func isStdoutTTY() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}

// resultRow builds the JSON object for one file result. The JSON output keys
// rows by path, so it omits the inner "path" field; NDJSON includes it.
func resultRow(r FileResult, includePath bool) map[string]any {
	row := map[string]any{
		"kind":     r.Kind,
		"ok":       r.OK,
		"compiled": r.Compiled,
	}
	if includePath {
		row["path"] = r.Path
	}
	if r.Parsed != nil {
		row["parsed"] = r.Parsed
	}
	if r.Error != "" {
		row["error"] = r.Error
	}
	if len(r.Warnings) > 0 {
		row["warnings"] = r.Warnings
	}
	return row
}

func jsonOutput(results []FileResult) (string, error) {
	out := map[string]any{}
	for _, r := range results {
		out[r.Path] = resultRow(r, false)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b) + "\n", nil
}

func ndjsonOutput(results []FileResult) (string, error) {
	sb := strings.Builder{}
	enc := json.NewEncoder(&sb)
	for _, r := range results {
		if err := enc.Encode(resultRow(r, true)); err != nil {
			return "", err
		}
	}
	return sb.String(), nil
}

func tableOutput(results []FileResult) string {
	header := []string{"path", "kind", "ok"}
	rows := make([][]any, 0, len(results))
	var errs []string
	for _, r := range results {
		rows = append(rows, []any{r.Path, string(r.Kind), r.OK})
		if !r.OK {
			msg := r.Error
			if msg == "" {
				msg = "validation failed"
			}
			errs = append(errs, fmt.Sprintf("%s: %s", r.Path, msg))
		}
	}
	out := g.PrettyTable(header, rows)
	if w := warningLines(results); w != "" {
		out += w
	}
	if len(errs) > 0 {
		out += fmt.Sprintf("\nerrors:\n  %s\n", strings.Join(errs, "\n  "))
	}
	return out
}

// warningLines renders advisory findings under the file rows. Warnings
// never change the exit code.
func warningLines(results []FileResult) string {
	sb := strings.Builder{}
	for _, r := range results {
		for _, w := range r.Warnings {
			fmt.Fprintf(&sb, "  warning: %s: %s\n", r.Path, w)
		}
	}
	if sb.Len() == 0 {
		return ""
	}
	return "\n" + sb.String()
}

// detailedOutput renders one table per kind, each with fields useful
// for that kind. Files that failed to parse get their own error table.
func detailedOutput(results []FileResult) string {
	sections := []struct {
		title string
		body  string
	}{
		{"Replications", replicationSection(results)},
		{"Pipelines", pipelineSection(results)},
		{"API Specs", apiSpecSection(results)},
		{"Connections", connectionSection(results)},
		{"Build Projects", buildSection(results)},
		{"Projects", projectSection(results)},
		{"Other Files", otherSection(results)},
		{"Errors", errorSection(results)},
	}

	sb := strings.Builder{}
	for _, s := range sections {
		if s.body == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(g.Colorize(g.ColorCyan, "# "+s.title) + "\n")
		sb.WriteString(s.body)
	}
	if sb.Len() == 0 {
		return "no files parsed\n"
	}
	if w := warningLines(results); w != "" {
		sb.WriteString(w)
	}
	return sb.String()
}

// projectSection lists the jobs each manifest declares.
func projectSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindProject) {
		m := asMapOrEmpty(r.Parsed)
		jobs, ok := asMap(m["jobs"])
		if !ok || len(jobs) == 0 {
			rows = append(rows, []any{r.Path, cast.ToString(m["name"]), "-", "-", "-"})
			continue
		}
		for _, key := range sortedKeys(jobs) {
			jm := asMapOrEmpty(jobs[key])
			rows = append(rows, []any{
				r.Path,
				cast.ToString(m["name"]),
				key,
				firstNonEmpty(cast.ToString(jm["file"]), "-"),
				firstNonEmpty(strings.Join(toStringSlice(jm["schedules"]), ", "), "-"),
			})
		}
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "project", "job", "file", "schedules"}, rows)
}

func kindResults(results []FileResult, kind Kind) []FileResult {
	out := []FileResult{}
	for _, r := range results {
		if r.OK && r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

func replicationSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindReplication) {
		m := asMapOrEmpty(r.Parsed)
		source := cast.ToString(m["source"])
		target := cast.ToString(m["target"])
		streams, ok := asMap(m["streams"])
		if !ok || len(streams) == 0 {
			rows = append(rows, []any{r.Path, source, target, "-", "-", "-"})
			continue
		}
		defs := asMapOrEmpty(m["defaults"])
		for _, name := range sortedKeys(streams) {
			sm, _ := asMap(streams[name])
			rows = append(rows, []any{
				r.Path,
				source,
				target,
				name,
				firstNonEmpty(cast.ToString(sm["mode"]), defaultsMode(defs), "-"),
				boolLabel(sm["disabled"]),
			})
		}
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "source", "target", "stream", "mode", "disabled"}, rows)
}

func pipelineSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindPipeline) {
		m := asMapOrEmpty(r.Parsed)
		for _, step := range asSlice(m["steps"]) {
			sm, ok := asMap(step)
			if !ok {
				continue
			}
			rows = append(rows, []any{
				r.Path,
				stepType(sm),
				firstNonEmpty(cast.ToString(sm["id"]), "-"),
			})
		}
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "type", "id"}, rows)
}

// stepType resolves the step type, including the shorthand form
// where the type is the key (e.g. `- replication: path`).
func stepType(sm map[string]any) string {
	if typ := strings.TrimSpace(cast.ToString(sm["type"])); typ != "" {
		return typ
	}
	for _, k := range sortedKeys(sm) {
		if knownStepTypes[k] {
			return k
		}
	}
	return "-"
}

func apiSpecSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindAPISpec) {
		m := asMapOrEmpty(r.Parsed)
		eps, ok := asMap(m["endpoints"])
		if !ok || len(eps) == 0 {
			rows = append(rows, []any{r.Path, "-", "-", "-", "-"})
			continue
		}
		for _, name := range sortedKeys(eps) {
			em, _ := asMap(eps[name])
			req, _ := asMap(em["request"])
			rows = append(rows, []any{
				r.Path,
				name,
				firstNonEmpty(cast.ToString(req["method"]), "GET"),
				cast.ToString(req["url"]),
				boolLabel(em["disabled"]),
			})
		}
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "endpoint", "method", "url", "disabled"}, rows)
}

func connectionSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindEnv) {
		m := asMapOrEmpty(r.Parsed)
		conns, ok := asMap(m["connections"])
		if !ok {
			continue
		}
		for _, name := range sortedKeys(conns) {
			cm, _ := asMap(conns[name])
			rows = append(rows, []any{
				r.Path,
				name,
				firstNonEmpty(cast.ToString(cm["type"]), "-"),
				firstNonEmpty(
					cast.ToString(cm["host"]),
					cast.ToString(cm["bucket"]),
					cast.ToString(cm["account"]),
					cast.ToString(cm["url"]),
					"-",
				),
				firstNonEmpty(cast.ToString(cm["database"]), "-"),
				firstNonEmpty(cast.ToString(cm["schema"]), "-"),
			})
		}
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "connection", "type", "host", "database", "schema"}, rows)
}

func buildSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range kindResults(results, KindBuild) {
		m := asMapOrEmpty(r.Parsed)
		vars, _ := asMap(m["vars"])
		rows = append(rows, []any{
			r.Path,
			firstNonEmpty(cast.ToString(m["target"]), "-"),
			firstNonEmpty(cast.ToString(m["mode"]), defaultsMode(m["defaults"]), "-"),
			countOrDash(m["models"]),
			countOrDash(m["seeds"]),
			len(vars),
		})
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "target", "mode", "models", "seeds", "vars"}, rows)
}

func otherSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range results {
		if !r.OK {
			continue
		}
		switch r.Kind {
		case KindReplication, KindPipeline, KindEnv, KindAPISpec, KindBuild, KindProject:
			continue
		}
		m := asMapOrEmpty(r.Parsed)
		rows = append(rows, []any{r.Path, string(r.Kind), len(m)})
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "kind", "keys"}, rows)
}

func errorSection(results []FileResult) string {
	rows := [][]any{}
	for _, r := range results {
		if r.OK {
			continue
		}
		msg := r.Error
		if msg == "" {
			msg = "parse failed"
		}
		rows = append(rows, []any{r.Path, string(r.Kind), msg})
	}
	if len(rows) == 0 {
		return ""
	}
	return g.PrettyTable([]string{"path", "kind", "error"}, rows)
}

func defaultsMode(v any) string {
	m, ok := asMap(v)
	if !ok {
		return ""
	}
	// compiled defaults use the Go field name
	return firstNonEmpty(cast.ToString(m["mode"]), cast.ToString(m["Mode"]))
}

func countOrDash(v any) any {
	if s := asSlice(v); s != nil {
		return len(s)
	}
	if m, ok := asMap(v); ok {
		return len(m)
	}
	return "-"
}

func boolLabel(v any) string {
	if cast.ToBool(v) {
		return "yes"
	}
	return "-"
}
