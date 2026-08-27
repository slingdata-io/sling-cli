package assist

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/flarco/g"
	"gopkg.in/yaml.v3"
)

// AnswersFile is the persisted state of an assist run — saved to
// <history>/<id>/answers.yaml.
type AnswersFile struct {
	SchemaVersion   int            `yaml:"schema_version" json:"schema_version"`
	Name            string         `yaml:"name" json:"name"`
	Task            string         `yaml:"task" json:"task"`
	TemplateVersion string         `yaml:"template_version,omitempty" json:"template_version"`
	SlingVersion    string         `yaml:"sling_version" json:"sling_version"`
	Created         time.Time      `yaml:"created" json:"created"`
	Agent           string         `yaml:"agent" json:"agent"`
	Parent          string         `yaml:"parent,omitempty" json:"parent"`
	Cwd             string         `yaml:"cwd" json:"cwd"`
	Answers         map[string]any `yaml:"answers" json:"answers"`
}

// Meta is the runtime side-record of an entry — saved to <history>/<id>/meta.json.
type Meta struct {
	ID               string         `json:"id"`
	Task             string         `json:"task"`
	Agent            string         `json:"agent"`
	Model            string         `json:"model,omitempty"`
	HarnessSessionID string         `json:"harness_session_id,omitempty"`
	LaunchedAt       *time.Time     `json:"launched_at"`
	Doctor           map[string]any `json:"doctor,omitempty"`
	Parent           string         `json:"parent,omitempty"`
}

// Entry is a loaded view of one history dir, used by the listing/picker.
type Entry struct {
	ID      string
	Path    string
	Answers AnswersFile
	Meta    Meta
}

// SaveEntry writes <history>/<id>/{answers.yaml, prompt.md, meta.json}.
// id is generated from Created + slug if empty.
func SaveEntry(a AnswersFile, prompt string, m Meta) (string, error) {
	if a.SchemaVersion == 0 {
		a.SchemaVersion = SchemaVersion
	}
	if a.Created.IsZero() {
		a.Created = time.Now().UTC()
	}
	id := m.ID
	if id == "" {
		slug := slugify(a.Name)
		if slug == "" {
			slug = slugify(a.Task)
		}
		id = a.Created.UTC().Format("2006-01-02_15-04-05") + "_" + slug
		m.ID = id
	}
	dir := filepath.Join(HistoryDir(), id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", g.Error(err, "mkdir %s", dir)
	}

	ay, err := yaml.Marshal(a)
	if err != nil {
		return "", g.Error(err, "marshal answers")
	}
	if err := os.WriteFile(filepath.Join(dir, "answers.yaml"), ay, 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "prompt.md"), []byte(prompt), 0o644); err != nil {
		return "", err
	}

	e := Entry{ID: id, Path: dir, Answers: a, Meta: m}
	if err := e.saveMeta(); err != nil {
		return "", err
	}
	return id, nil
}

func (e Entry) saveMeta() error {
	mj, err := json.MarshalIndent(e.Meta, "", "  ")
	if err != nil {
		return err
	}
	mj = append(mj, '\n')
	return os.WriteFile(filepath.Join(e.Path, "meta.json"), mj, 0o644)
}

// LoadEntry reads <history>/<id>/.
func LoadEntry(id string) (Entry, error) {
	dir := filepath.Join(HistoryDir(), id)
	e := Entry{ID: id, Path: dir}
	ay, err := os.ReadFile(filepath.Join(dir, "answers.yaml"))
	if err != nil {
		return e, g.Error(err, "read answers")
	}
	if err := yaml.Unmarshal(ay, &e.Answers); err != nil {
		return e, g.Error(err, "parse answers")
	}
	if mj, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		_ = json.Unmarshal(mj, &e.Meta)
	}
	return e, nil
}

// ListEntries returns all entries in ~/.sling/assist/history/, most-recent first.
func ListEntries() ([]Entry, error) {
	root := HistoryDir()
	dirs, err := os.ReadDir(root)
	if err != nil {
		return nil, g.Error(err, "read %s", root)
	}
	out := []Entry{}
	for _, d := range dirs {
		if !d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			continue
		}
		e, err := LoadEntry(d.Name())
		if err != nil {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Answers.Created.After(out[j].Answers.Created)
	})
	return out, nil
}

// AutoTrim deletes the oldest entries until at most HistoryMaxEntries remain.
func AutoTrim() error {
	entries, err := ListEntries()
	if err != nil {
		return err
	}
	if len(entries) <= HistoryMaxEntries {
		return nil
	}
	var first error
	for _, e := range entries[HistoryMaxEntries:] {
		if rmErr := os.RemoveAll(e.Path); rmErr != nil && first == nil {
			first = g.Error(rmErr, "remove history %s", e.Path)
		}
	}
	return first
}

// FormatRelative returns a short human relative time like "3h ago", "yesterday",
// "2026-05-01" (for entries older than a week).
func FormatRelative(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 48*time.Hour:
		return "yesterday"
	case d < 7*24*time.Hour:
		return fmt.Sprintf("%d days ago", int(d.Hours()/24))
	default:
		return t.Format("2006-01-02")
	}
}

// maxSlugLen caps the slug so <timestamp>_<slug> stays a short directory name.
const maxSlugLen = 40

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	out := strings.Builder{}
	last := rune(0)
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			last = r
		case r == ' ' || r == '_' || r == '-':
			if last == '-' {
				continue // collapse runs of separators
			}
			out.WriteRune('-')
			last = '-'
		}
	}
	res := strings.Trim(out.String(), "-")
	if res == "" {
		return "entry"
	}
	return truncateSlug(res, maxSlugLen)
}

// truncateSlug cuts at the last word boundary within n, so the slug stays readable.
func truncateSlug(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	if i := strings.LastIndexByte(cut, '-'); i > 0 {
		cut = cut[:i]
	}
	return strings.Trim(cut, "-")
}

func collapseHome(p string) string {
	home := userHome()
	if home == "" {
		return p
	}
	if strings.HasPrefix(p, home) {
		return "~" + strings.TrimPrefix(p, home)
	}
	return p
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		g.Debug("assist: getwd failed: %s", err.Error())
		return ""
	}
	return wd
}

// PickHistoryEntry opens a searchable table of recent sessions.
// Returns ErrUserAborted when the user cancels.
func PickHistoryEntry() (Entry, error) {
	entries, err := ListEntries()
	if err != nil {
		return Entry{}, err
	}
	if len(entries) == 0 {
		return Entry{}, g.Error("no sessions yet — run `sling assist` first")
	}
	if !isTTY(os.Stdin) || !isTTY(os.Stdout) {
		return Entry{}, g.Error("pass a session id (`sling assist --resume <id>`) when not on a TTY")
	}

	m := newPickerModel(entries)
	p := tea.NewProgram(m, tea.WithAltScreen())
	final, err := p.Run()
	if err != nil {
		return Entry{}, g.Error(err, "session picker")
	}
	got, ok := final.(pickerModel)
	if !ok || got.chosen == nil {
		return Entry{}, ErrUserAborted
	}
	return *got.chosen, nil
}

func filterEntries(entries []Entry, query string) []Entry {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return entries
	}
	out := []Entry{}
	for _, e := range entries {
		if entryMatches(e, q) {
			out = append(out, e)
		}
	}
	return out
}

func entryMatches(e Entry, q string) bool {
	ask := ""
	if e.Answers.Answers != nil {
		if v, ok := e.Answers.Answers["ask"].(string); ok {
			ask = v
		}
		if v, ok := e.Answers.Answers["intention"].(string); ok && ask == "" {
			ask = v
		}
	}
	hay := strings.ToLower(strings.Join([]string{
		e.ID, e.Answers.Name, e.Answers.Task, e.Answers.Agent, e.Answers.Cwd, ask, e.Meta.Agent,
	}, " "))
	return strings.Contains(hay, q)
}

type pickerModel struct {
	all      []Entry
	filtered []Entry
	query    string
	cursor   int
	chosen   *Entry
	width    int
	height   int
	quit     bool
}

func newPickerModel(entries []Entry) pickerModel {
	return pickerModel{
		all:      entries,
		filtered: entries,
		width:    80,
		height:   24,
	}
}

func (m pickerModel) Init() tea.Cmd { return nil }

func (m pickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			m.quit = true
			return m, tea.Quit
		case "enter":
			if m.cursor >= 0 && m.cursor < len(m.filtered) {
				e := m.filtered[m.cursor]
				m.chosen = &e
			}
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.filtered)-1 {
				m.cursor++
			}
		case "backspace":
			if m.query != "" {
				r := []rune(m.query)
				m.query = string(r[:len(r)-1])
				m.filtered = filterEntries(m.all, m.query)
				if m.cursor >= len(m.filtered) {
					m.cursor = max(0, len(m.filtered)-1)
				}
			}
		default:
			if msg.Type == tea.KeyRunes {
				m.query += string(msg.Runes)
				m.filtered = filterEntries(m.all, m.query)
				m.cursor = 0
			}
		}
	}
	return m, nil
}

func (m pickerModel) View() string {
	var b strings.Builder
	title := lipgloss.NewStyle().Bold(true).Render("Resume session")
	fmt.Fprintf(&b, "%s\n", title)
	fmt.Fprintf(&b, "  search: %s█\n\n", m.query)

	header := fmt.Sprintf("  %-28s  %-16s  %-10s  %-12s  %s", "ID", "TASK", "AGENT", "CREATED", "NAME")
	fmt.Fprintln(&b, lipgloss.NewStyle().Faint(true).Render(header))

	if len(m.filtered) == 0 {
		fmt.Fprintln(&b, "  (no matches)")
		return b.String()
	}

	rows := m.height - 8
	if rows < 3 {
		rows = 3
	}
	start := 0
	if m.cursor >= rows {
		start = m.cursor - rows + 1
	}
	end := start + rows
	if end > len(m.filtered) {
		end = len(m.filtered)
	}

	sel := lipgloss.NewStyle().Reverse(true)
	for i := start; i < end; i++ {
		e := m.filtered[i]
		agent := e.Answers.Agent
		if agent == "" {
			agent = e.Meta.Agent
		}
		if agent == "" {
			agent = "—"
		}
		line := fmt.Sprintf("  %-28s  %-16s  %-10s  %-12s  %s",
			clipRunes(e.ID, 28),
			clipRunes(e.Answers.Task, 16),
			clipRunes(agent, 10),
			FormatRelative(e.Answers.Created),
			clipRunes(e.Answers.Name, 24),
		)
		if i == m.cursor {
			line = sel.Render(line)
		}
		fmt.Fprintln(&b, line)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, lipgloss.NewStyle().Faint(true).Render("  ↑/↓ move  enter resume  esc abort"))
	return b.String()
}

func clipRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	return string(r[:n-1]) + "…"
}
