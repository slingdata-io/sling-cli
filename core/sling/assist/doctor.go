package assist

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
)

// CellState is one cell in the doctor matrix.
type CellState int

const (
	CellOK    CellState = iota // ✓
	CellFail                   // ✗
	CellNA                     // ⊘ unsupported
	CellEmpty                  // — not applicable
)

// CheckResult is a typed status from Client.CheckSkills / CheckMCP.
type CheckResult struct {
	State CellState `json:"state"`
	Skill string    `json:"skill,omitempty"` // set for per-skill checks
	Note  string    `json:"note,omitempty"`  // short detail, no glyph
}

// Render returns a CLI display line (e.g. "✓ claude: sling").
func (r CheckResult) Render(clientName string) string {
	label := clientName
	if r.Skill != "" {
		if r.Note != "" {
			return fmt.Sprintf("%s %s: %s — %s", r.State.Glyph(), label, r.Skill, r.Note)
		}
		return fmt.Sprintf("%s %s: %s", r.State.Glyph(), label, r.Skill)
	}
	if r.Note != "" {
		return fmt.Sprintf("%s %s: %s", r.State.Glyph(), label, r.Note)
	}
	return fmt.Sprintf("%s %s", r.State.Glyph(), label)
}

// Glyph returns the terminal marker for a cell state.
func (s CellState) Glyph() string {
	switch s {
	case CellOK:
		return "✓"
	case CellFail:
		return "✗"
	case CellNA:
		return "⊘"
	case CellEmpty:
		return "—"
	default:
		return "?"
	}
}

// String is the JSON/API token for a cell state.
func (s CellState) String() string {
	switch s {
	case CellOK:
		return "ok"
	case CellFail:
		return "fail"
	case CellNA:
		return "na"
	case CellEmpty:
		return "empty"
	default:
		return "unknown"
	}
}

// MarshalJSON encodes CellState as a stable string.
func (s CellState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func checkOK(note string) CheckResult {
	return CheckResult{State: CellOK, Note: note}
}

func checkFail(note string) CheckResult {
	return CheckResult{State: CellFail, Note: note}
}

func checkNA(note string) CheckResult {
	return CheckResult{State: CellNA, Note: note}
}

func checkEmpty(note string) CheckResult {
	return CheckResult{State: CellEmpty, Note: note}
}

func checkSkill(state CellState, skill, note string) CheckResult {
	return CheckResult{State: state, Skill: skill, Note: note}
}

// MatrixRow is one row in the agent × capability matrix.
type MatrixRow struct {
	Label string               `json:"label"`
	Cells map[string]CellState `json:"cells"` // client name → state
	Notes map[string]string    `json:"notes,omitempty"`
}

// DoctorMatrix is the cross-check table for detected clients.
type DoctorMatrix struct {
	Clients []string    `json:"clients"`
	Rows    []MatrixRow `json:"rows"`
}

// DoctorFinding is one structured global check (profile, skill, env, version).
type DoctorFinding struct {
	ID      string `json:"id"`
	OK      bool   `json:"ok"`
	Summary string `json:"summary"`
	Detail  string `json:"detail,omitempty"`
	Hint    string `json:"hint,omitempty"`
}

// DoctorReport is the `sling assist setup --doctor` result.
type DoctorReport struct {
	OK           bool            `json:"ok"`
	SlingVersion string          `json:"sling_version"`
	Findings     []DoctorFinding `json:"findings"`
	Matrix       *DoctorMatrix   `json:"matrix,omitempty"`
	Lines        []string        `json:"-"` // CLI glyph prose
}

// AddFinding records a structured finding and a CLI display line.
func (r *DoctorReport) AddFinding(f DoctorFinding) {
	r.Findings = append(r.Findings, f)
	glyph := "✓"
	if !f.OK {
		glyph = "✗"
		r.OK = false
	}
	line := fmt.Sprintf("%s %s", glyph, f.Summary)
	if f.Hint != "" {
		line += "  → " + f.Hint
	}
	r.Lines = append(r.Lines, line)
}

// Add maps glyph prose into a finding.
func (r *DoctorReport) Add(pass bool, line string) {
	sum := strings.TrimSpace(line)
	for _, pfx := range []string{"✓ ", "✗ ", "⊘ ", "— "} {
		sum = strings.TrimPrefix(sum, pfx)
	}
	hint := ""
	if i := strings.Index(sum, "  → "); i >= 0 {
		hint = strings.TrimSpace(sum[i+4:])
		sum = strings.TrimSpace(sum[:i])
	}
	id := "misc"
	if parts := strings.SplitN(sum, ":", 2); len(parts) > 0 {
		id = strings.TrimSpace(strings.ReplaceAll(parts[0], " ", "_"))
	}
	r.AddFinding(DoctorFinding{ID: id, OK: pass, Summary: sum, Hint: hint})
}

// ToJSON returns a pretty-printed doctor.json payload.
func (r *DoctorReport) ToJSON() ([]byte, error) {
	if r == nil {
		return []byte("{}"), nil
	}
	return json.MarshalIndent(r, "", "  ")
}

// DoctorOptions configures Doctor. Zero value uses ScopeUser.
type DoctorOptions struct {
	Scope Scope // must match install scope under test
}

// Doctor probes the install end-to-end. opts[0].Scope defaults to ScopeUser.
func Doctor(ctx context.Context, opts ...DoctorOptions) (*DoctorReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope := ScopeUser
	if len(opts) > 0 {
		scope = opts[0].Scope
	}

	r := &DoctorReport{OK: true, SlingVersion: core.Version}

	prof, exists, err := LoadProfile()
	switch {
	case err != nil:
		r.AddFinding(DoctorFinding{
			ID: "profile", OK: false,
			Summary: fmt.Sprintf("profile: %v", err),
			Hint:    "run: sling assist setup",
		})
	case !exists:
		r.AddFinding(DoctorFinding{
			ID: "profile", OK: false,
			Summary: "profile: env.SLING_ASSIST missing in env.yaml",
			Hint:    "run: sling assist setup",
		})
	default:
		r.AddFinding(DoctorFinding{
			ID: "profile", OK: true,
			Summary: fmt.Sprintf("profile: agent=%s, hint_in_errors=%v", prof.Agent, prof.HintInErrors),
		})
	}

	if err := ctx.Err(); err != nil {
		return r, err
	}

	skillNames := listSkillNames()
	for _, name := range skillNames {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		canonical := canonicalSkillPath(name)
		id := "skills." + name
		if !g.PathExists(canonical) {
			r.AddFinding(DoctorFinding{
				ID: id, OK: false,
				Summary: fmt.Sprintf("skills: %s missing in ~/.agents/skills/", name),
				Hint:    "run: sling assist setup",
			})
			continue
		}
		ok, detail, merr := skillMatchesEmbedded(name)
		if merr != nil {
			r.AddFinding(DoctorFinding{
				ID: id, OK: false,
				Summary: fmt.Sprintf("skills: %s could not be compared", name),
				Detail:  merr.Error(),
			})
			continue
		}
		if !ok {
			r.AddFinding(DoctorFinding{
				ID: id, OK: false,
				Summary: fmt.Sprintf("skills: %s drifted from embedded", name),
				Hint:    "run: sling assist setup",
				Detail:  detail,
			})
			continue
		}
		r.AddFinding(DoctorFinding{
			ID: id, OK: true,
			Summary: fmt.Sprintf("skills: %s matches embedded", name),
		})
	}

	if !g.PathExists(envFilePath()) {
		r.AddFinding(DoctorFinding{
			ID: "env", OK: false,
			Summary: fmt.Sprintf("env: %s missing", envFilePath()),
		})
	} else {
		r.AddFinding(DoctorFinding{
			ID: "env", OK: true,
			Summary: fmt.Sprintf("env: %s present", envFilePath()),
		})
	}

	stamp, _ := os.ReadFile(VersionFilePath())
	stampVer := strings.TrimSpace(string(stamp))
	switch {
	case stampVer == "":
		r.AddFinding(DoctorFinding{
			ID: "version", OK: false,
			Summary: "version: ~/.sling/assist/version not stamped",
			Hint:    "run: sling assist setup",
		})
	case stampVer == versionUninstalled:
		r.AddFinding(DoctorFinding{
			ID: "version", OK: false,
			Summary: "version: assist uninstalled",
			Hint:    "run: sling assist setup",
		})
	case stampVer != core.Version:
		r.AddFinding(DoctorFinding{
			ID: "version", OK: false,
			Summary: fmt.Sprintf("version: stamped %q but binary is %q (auto-refresh did not run)", stampVer, core.Version),
		})
	default:
		r.AddFinding(DoctorFinding{
			ID: "version", OK: true,
			Summary: fmt.Sprintf("version: %s", core.Version),
		})
	}

	if err := ctx.Err(); err != nil {
		return r, err
	}

	r.addZenFinding()

	detected := DetectedClients()
	if len(detected) == 0 {
		r.AddFinding(DoctorFinding{
			ID: "clients", OK: false,
			Summary: "clients: no CLI agent on $PATH",
			Hint:    "install claude, codex, gemini, cursor, opencode, pi, or grok — or run sling assist setup to install OpenCode",
		})
		return r, nil
	}
	r.Matrix = buildMatrix(ctx, detected, skillNames, scope)
	for _, row := range r.Matrix.Rows {
		for _, cl := range r.Matrix.Clients {
			if row.Cells[cl] == CellFail {
				r.OK = false
			}
		}
	}
	return r, nil
}

func buildMatrix(ctx context.Context, detected []Client, skillNames []string, scope Scope) *DoctorMatrix {
	m := &DoctorMatrix{}
	for _, c := range detected {
		m.Clients = append(m.Clients, c.Name())
	}

	authRow := MatrixRow{Label: "auth", Cells: map[string]CellState{}, Notes: map[string]string{}}
	for _, c := range detected {
		if c.Kind() != KindCLIAgent {
			authRow.Cells[c.Name()] = CellEmpty
			continue
		}
		authRow.Cells[c.Name()] = c.AuthState().cell()
	}
	m.Rows = append(m.Rows, authRow)

	mcpRow := MatrixRow{Label: "MCP", Cells: map[string]CellState{}, Notes: map[string]string{}}
	for _, c := range detected {
		res := c.CheckMCP(ctx, scope)
		mcpRow.Cells[c.Name()] = res.State
		if res.State == CellFail && res.Note != "" {
			mcpRow.Notes[c.Name()] = res.Note
		}
	}
	m.Rows = append(m.Rows, mcpRow)

	for _, skill := range skillNames {
		row := MatrixRow{Label: skill, Cells: map[string]CellState{}, Notes: map[string]string{}}
		for _, c := range detected {
			results := c.CheckSkills(ctx, []string{skill}, scope)
			if len(results) == 0 {
				row.Cells[c.Name()] = CellNA
				continue
			}
			res := results[0]
			row.Cells[c.Name()] = res.State
			if res.State == CellFail && res.Note != "" {
				row.Notes[c.Name()] = res.Note
			}
		}
		m.Rows = append(m.Rows, row)
	}
	return m
}

// Render is the CLI doctor report (glyph lines + matrix).
func (r *DoctorReport) Render() string {
	if r == nil {
		return ""
	}
	var b strings.Builder
	for _, line := range r.Lines {
		b.WriteString(colorizeDoctorLine(line))
		b.WriteByte('\n')
	}
	if r.Matrix != nil {
		b.WriteByte('\n')
		b.WriteString(env.BlueString("Agent × Capability:"))
		b.WriteByte('\n')
		b.WriteString(r.Matrix.render())
	}
	return b.String()
}

// MissingComponents lists install pieces that look incomplete on disk.
func (r *DoctorReport) MissingComponents() []string {
	if r == nil || r.OK {
		return nil
	}
	skillsBad, mcpBad := false, false
	for _, f := range r.Findings {
		if !f.OK && strings.HasPrefix(f.ID, "skills.") {
			skillsBad = true
		}
	}
	for _, line := range r.Lines {
		if strings.HasPrefix(strings.TrimSpace(line), "✗ skills") {
			skillsBad = true
		}
	}
	if r.Matrix != nil {
		for _, row := range r.Matrix.Rows {
			for _, c := range r.Matrix.Clients {
				if row.Cells[c] == CellFail {
					switch row.Label {
					case "MCP":
						mcpBad = true
					case "auth":
					default:
						skillsBad = true
					}
				}
			}
		}
	}
	var out []string
	if skillsBad {
		out = append(out, "skills")
	}
	if mcpBad {
		out = append(out, "mcp")
	}
	return out
}

func (m *DoctorMatrix) render() string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)

	header := table.Row{""}
	for _, c := range m.Clients {
		header = append(header, c)
	}
	t.AppendHeader(header)

	var notes []string
	for _, row := range m.Rows {
		r := table.Row{row.Label}
		for _, c := range m.Clients {
			r = append(r, renderCell(row.Cells[c]))
		}
		t.AppendRow(r)
		for _, c := range m.Clients {
			if note, ok := row.Notes[c]; ok && note != "" {
				notes = append(notes, fmt.Sprintf("    %s/%s: %s",
					env.YellowString(c), env.YellowString(row.Label), note))
			}
		}
	}

	var colCfgs []table.ColumnConfig
	for i := range m.Clients {
		colCfgs = append(colCfgs, table.ColumnConfig{
			Number:      i + 2,
			Align:       text.AlignCenter,
			AlignHeader: text.AlignCenter,
		})
	}
	t.SetColumnConfigs(colCfgs)

	out := t.Render() + "\n"
	if len(notes) > 0 {
		out += "\n  " + env.YellowString("Notes:") + "\n"
		for _, n := range notes {
			out += n + "\n"
		}
	}
	return out
}

func renderCell(s CellState) string {
	switch s {
	case CellOK:
		return env.GreenString("✓")
	case CellFail:
		return env.RedString("✗")
	case CellNA:
		return env.YellowString("⊘")
	default:
		return env.DarkGrayString("—")
	}
}

func colorizeDoctorLine(line string) string {
	trimmed := strings.TrimLeft(line, " ")
	indent := line[:len(line)-len(trimmed)]
	var prefix, rest string
	switch {
	case strings.HasPrefix(trimmed, "✓"):
		prefix = env.GreenString("✓")
		rest = strings.TrimPrefix(trimmed, "✓")
	case strings.HasPrefix(trimmed, "✗"):
		prefix = env.RedString("✗")
		rest = strings.TrimPrefix(trimmed, "✗")
	case strings.HasPrefix(trimmed, "⊘"):
		prefix = env.YellowString("⊘")
		rest = strings.TrimPrefix(trimmed, "⊘")
	case strings.HasPrefix(trimmed, "—"):
		prefix = env.DarkGrayString("—")
		rest = strings.TrimPrefix(trimmed, "—")
	default:
		return line
	}
	if idx := strings.Index(rest, "→ run:"); idx >= 0 {
		rest = rest[:idx] + env.CyanString(rest[idx:])
	}
	return indent + prefix + rest
}
