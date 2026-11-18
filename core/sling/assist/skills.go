package assist

import (
	_ "embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/flarco/g"
	"gopkg.in/yaml.v3"
)

// SkillsCLIMap is the "MCP action → CLI command" table for agents with no MCP server.
//
//go:embed skills_cli.md
var SkillsCLIMap string

const skillMainFile = "SKILL.md"

// SkillCatalog prints the embedded skills for `sling skills`.
type SkillCatalog struct {
	fsys    fs.FS
	exclude []string
}

type SkillEntry struct {
	Name        string
	Description string
	Bytes       int
	MentionsMCP bool
	Files       []SkillFile // sub-files, sorted, without SKILL.md
}

type SkillFile struct {
	Name        string
	Title       string
	Bytes       int
	MentionsMCP bool
}

// NewSkillCatalog returns a catalog of SkillsFS without agent-browser.
func NewSkillCatalog() *SkillCatalog {
	sub, err := fs.Sub(SkillsFS, "skills")
	if err != nil {
		panic(err) // the embed always has the skills folder
	}
	return NewSkillCatalogFS(sub, []string{"agent-browser"})
}

func NewSkillCatalogFS(fsys fs.FS, exclude []string) *SkillCatalog {
	return &SkillCatalog{fsys: fsys, exclude: exclude}
}

func (c *SkillCatalog) Skills() ([]SkillEntry, error) {
	dirs, err := fs.ReadDir(c.fsys, ".")
	if err != nil {
		return nil, g.Error(err, "could not read the skills")
	}
	out := []SkillEntry{}
	for _, d := range dirs {
		if !d.IsDir() || g.In(d.Name(), c.exclude...) {
			continue
		}
		entry, err := c.readSkill(d.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, nil
}

func (c *SkillCatalog) readSkill(name string) (entry SkillEntry, err error) {
	main, err := fs.ReadFile(c.fsys, path.Join(name, skillMainFile))
	if err != nil {
		return entry, g.Error("skill %q has no %s", name, skillMainFile)
	}

	front := struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	}{}
	if block, ok := frontmatter(string(main)); ok {
		if err = yaml.Unmarshal([]byte(block), &front); err != nil {
			return entry, g.Error(err, "could not parse the frontmatter of skill %q", name)
		}
	}

	entry = SkillEntry{
		Name:        name,
		Description: strings.Join(strings.Fields(front.Description), " "),
		Bytes:       len(main),
		MentionsMCP: mentionsMCP(string(main)),
	}

	files, err := fs.ReadDir(c.fsys, name)
	if err != nil {
		return entry, g.Error(err, "could not read skill %q", name)
	}
	for _, f := range files {
		if f.IsDir() || f.Name() == skillMainFile || !strings.HasSuffix(f.Name(), ".md") {
			continue
		}
		data, err := fs.ReadFile(c.fsys, path.Join(name, f.Name()))
		if err != nil {
			return entry, g.Error(err, "could not read %s/%s", name, f.Name())
		}
		entry.Files = append(entry.Files, SkillFile{
			Name:        f.Name(),
			Title:       firstTitle(string(data)),
			Bytes:       len(data),
			MentionsMCP: mentionsMCP(string(data)),
		})
	}
	sort.Slice(entry.Files, func(i, j int) bool { return entry.Files[i].Name < entry.Files[j].Name })
	return entry, nil
}

func (c *SkillCatalog) Index(version string) (string, error) {
	skills, err := c.Skills()
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("# Sling skills\n\n")
	fmt.Fprintf(&b, "Sling %s has %d skills. Print one with `sling skills <skill>`. Print a sub-file with `sling skills <skill>/<FILE>.md`.\n", version, len(skills))
	b.WriteString("To install the skills for later sessions, run `sling assist setup`.\n\n")
	for _, s := range skills {
		fmt.Fprintf(&b, "## %s (%s, %s)\n", s.Name, skillMainFile, kb(s.Bytes))
		b.WriteString(s.Description + "\n")
		if len(s.Files) > 0 {
			b.WriteString("Sub-files:\n")
			for _, f := range s.Files {
				fmt.Fprintf(&b, "- %s (%s): %s\n", f.Name, kb(f.Bytes), f.Title)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString(SkillsCLIMap)
	return b.String(), nil
}

// Render returns one skill file with a header. ref is <skill> or <skill>/<FILE>[.md], any case.
func (c *SkillCatalog) Render(ref string) (string, error) {
	skillRef, fileRef, err := parseSkillRef(ref)
	if err != nil {
		return "", err
	}

	skills, err := c.Skills()
	if err != nil {
		return "", err
	}
	var skill *SkillEntry
	names := make([]string, len(skills))
	for i := range skills {
		names[i] = skills[i].Name
		if strings.EqualFold(skills[i].Name, skillRef) {
			skill = &skills[i]
		}
	}
	if skill == nil {
		return "", g.Error("unknown skill %q. Run \"sling skills\" to list the skills: %s", skillRef, strings.Join(names, ", "))
	}

	fileName, mentions := skillMainFile, skill.MentionsMCP
	if !strings.EqualFold(fileRef, skillMainFile) {
		var file *SkillFile
		for i := range skill.Files {
			if strings.EqualFold(skill.Files[i].Name, fileRef) {
				file = &skill.Files[i]
			}
		}
		if file == nil {
			fileNames := []string{}
			for _, f := range skill.Files {
				fileNames = append(fileNames, f.Name)
			}
			if len(fileNames) == 0 {
				fileNames = []string{skillMainFile}
			}
			return "", g.Error("skill %q has no file %q. Files: %s", skill.Name, fileRef, strings.Join(fileNames, ", "))
		}
		fileName, mentions = file.Name, file.MentionsMCP
	}

	raw, err := fs.ReadFile(c.fsys, path.Join(skill.Name, fileName))
	if err != nil {
		return "", g.Error(err, "could not read %s/%s", skill.Name, fileName)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# Skill: %s (%s)\n", skill.Name, fileName)
	if fileName == skillMainFile && len(skill.Files) > 0 {
		cmds := make([]string, len(skill.Files))
		for i, f := range skill.Files {
			cmds[i] = fmt.Sprintf("`sling skills %s/%s`", skill.Name, f.Name)
		}
		first := skill.Files[0].Name
		fmt.Fprintf(&b, "Sub-files: %s\n", strings.Join(cmds, ", "))
		fmt.Fprintf(&b, "Links such as [%s](%s) in this text mean: run `sling skills %s/%s`.\n", first, first, skill.Name, first)
	}
	b.WriteString("\n")
	if mentions {
		b.WriteString(SkillsCLIMap)
	}
	b.WriteString("---\n")
	b.Write(raw)
	return b.String(), nil
}

// parseSkillRef splits a reference into the skill and the file name (with .md).
func parseSkillRef(ref string) (skill, file string, err error) {
	ref = strings.TrimSpace(ref)
	badErr := g.Error("bad skill reference %q. Use <skill> or <skill>/<FILE>.md", ref)
	if ref == "" || strings.Contains(ref, "..") || strings.Contains(ref, `\`) || strings.Count(ref, "/") > 1 {
		return "", "", badErr
	}
	skill, file, _ = strings.Cut(ref, "/")
	if skill == "" {
		return "", "", badErr
	}
	if file == "" {
		return skill, skillMainFile, nil
	}
	if !strings.HasSuffix(strings.ToLower(file), ".md") {
		file += ".md"
	}
	return skill, file, nil
}

// frontmatter returns the YAML block between the first two "---" lines.
func frontmatter(text string) (string, bool) {
	if !strings.HasPrefix(text, "---\n") {
		return "", false
	}
	block, _, ok := strings.Cut(text[4:], "\n---")
	return block, ok
}

func firstTitle(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "# ") {
			return strings.TrimSpace(line[2:])
		}
	}
	return ""
}

func mentionsMCP(text string) bool {
	return strings.Contains(strings.ToLower(text), "mcp")
}

func kb(bytes int) string {
	return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
}
