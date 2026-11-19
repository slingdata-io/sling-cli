package assist

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
)

var embeddedSkillNames = "sling, sling-api-specs, sling-build, sling-pipelines, sling-platform, sling-python, sling-replications"

func TestSkillCatalogExcludesAgentBrowser(t *testing.T) {
	skills, err := NewSkillCatalog().Skills()
	if err != nil {
		t.Fatal(err)
	}
	if len(skills) != 7 {
		t.Fatalf("got %d skills, want 7", len(skills))
	}
	if skills[0].Name != "sling" {
		t.Fatalf("first skill is %q, want sling", skills[0].Name)
	}
	for _, s := range skills {
		if s.Name == "agent-browser" {
			t.Fatal("agent-browser is in the catalog")
		}
	}
}

func TestSkillCatalogEmbeddedValid(t *testing.T) {
	skills, err := NewSkillCatalog().Skills()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range skills {
		main, err := fs.ReadFile(SkillsFS, "skills/"+s.Name+"/SKILL.md")
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		block, ok := frontmatter(string(main))
		if !ok || !strings.Contains(block, "name: "+s.Name+"\n") {
			t.Errorf("%s: frontmatter name is not the folder name", s.Name)
		}
		if s.Description == "" {
			t.Errorf("%s: empty description", s.Name)
		}
		for _, f := range s.Files {
			if f.Title == "" {
				t.Errorf("%s/%s: no title", s.Name, f.Name)
			}
		}
	}
}

// padTo makes text exactly n bytes long.
func padTo(text string, n int) string {
	return text + strings.Repeat("x", n-len(text)-1) + "\n"
}

func testCatalog() *SkillCatalog {
	return NewSkillCatalogFS(fstest.MapFS{
		"alpha/SKILL.md":     {Data: []byte(padTo("---\nname: alpha\ndescription: >\n  First line\n  second line.\n---\n# Alpha\nUse the MCP tools.\n", 1536))},
		"alpha/GUIDE.md":     {Data: []byte(padTo("# Alpha Guide\nText.\n", 512))},
		"alpha/BASICS.md":    {Data: []byte(padTo("Intro.\n# Basics\n", 2048))},
		"alpha/notes.txt":    {Data: []byte("not a skill file")},
		"beta/SKILL.md":      {Data: []byte(padTo("---\nname: beta\ndescription: Beta skill.\n---\n# Beta\n", 1024))},
		"hidden/SKILL.md":    {Data: []byte("---\nname: hidden\ndescription: x\n---\n")},
		"hidden/EXTRA.md":    {Data: []byte("# Extra\n")},
		"stray-file-at-root": {Data: []byte("x")},
	}, []string{"hidden"})
}

func TestSkillIndexFormat(t *testing.T) {
	got, err := testCatalog().Index("1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	want := "# Sling skills\n\n" +
		"Sling 1.2.3 has 2 skills. Print one with `sling skills <skill>`. Print a sub-file with `sling skills <skill>/<FILE>.md`.\n" +
		"To install the skills for later sessions, run `sling assist setup`.\n\n" +
		"## alpha (SKILL.md, 1.5 KB)\n" +
		"First line second line.\n" +
		"Sub-files:\n" +
		"- BASICS.md (2.0 KB): Basics\n" +
		"- GUIDE.md (0.5 KB): Alpha Guide\n\n" +
		"## beta (SKILL.md, 1.0 KB)\n" +
		"Beta skill.\n\n" +
		SkillsCLIMap
	if got != want {
		t.Fatalf("index mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	if strings.Count(got, "## Without the Sling MCP server") != 1 {
		t.Fatal("the CLI table heading must show one time")
	}
}

func TestSkillRenderSubFile(t *testing.T) {
	c := NewSkillCatalog()
	for _, ref := range []string{"sling/connections", "SLING/CONNECTIONS.md"} {
		got, err := c.Render(ref)
		if err != nil {
			t.Fatalf("%s: %v", ref, err)
		}
		if !strings.HasPrefix(got, "# Skill: sling (CONNECTIONS.md)\n") {
			t.Errorf("%s: wrong header: %q", ref, strings.SplitN(got, "\n", 2)[0])
		}
		if !strings.Contains(got, "# Connection Management") {
			t.Errorf("%s: no file text", ref)
		}
		if strings.Contains(got, "Sub-files:") {
			t.Errorf("%s: sub-file output must not list sub-files", ref)
		}
	}

	// sub-files link to their siblings, so their header explains the links too
	got, err := testCatalog().Render("alpha/guide")
	if err != nil {
		t.Fatal(err)
	}
	wantHead := "# Skill: alpha (GUIDE.md)\n" +
		"Links such as [BASICS.md](BASICS.md) in this text mean: run `sling skills alpha/BASICS.md`.\n\n"
	if !strings.HasPrefix(got, wantHead) {
		t.Fatalf("render mismatch\n--- got ---\n%s", got[:min(len(got), 300)])
	}
	got, err = testCatalog().Render("alpha/basics")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Links such as [GUIDE.md](GUIDE.md) in this text mean: run `sling skills alpha/GUIDE.md`.\n") {
		t.Fatalf("example must name another file\n--- got ---\n%s", got[:min(len(got), 300)])
	}
}

func TestSkillRenderMain(t *testing.T) {
	got, err := testCatalog().Render("alpha")
	if err != nil {
		t.Fatal(err)
	}
	wantHead := "# Skill: alpha (SKILL.md)\n" +
		"Sub-files: `sling skills alpha/BASICS.md`, `sling skills alpha/GUIDE.md`\n" +
		"Links such as [BASICS.md](BASICS.md) in this text mean: run `sling skills alpha/BASICS.md`.\n\n" +
		SkillsCLIMap + "---\n---\nname: alpha\n"
	if !strings.HasPrefix(got, wantHead) {
		t.Fatalf("render mismatch\n--- got ---\n%s", got[:min(len(got), 1500)])
	}

	got, err = testCatalog().Render("beta")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "# Skill: beta (SKILL.md)\n\n---\n---\nname: beta\n") {
		t.Fatalf("render mismatch: %q", got[:80])
	}
}

func TestSkillRenderCLIMapOnlyWhenMCP(t *testing.T) {
	c := NewSkillCatalog()
	py, err := c.Render("sling-python")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(py, "## Without the Sling MCP server") {
		t.Error("sling-python must not have the CLI table")
	}
	sl, err := c.Render("sling")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sl, "## Without the Sling MCP server") {
		t.Error("sling must have the CLI table")
	}
}

func TestSkillRenderUnknown(t *testing.T) {
	c := NewSkillCatalog()
	for _, ref := range []string{"foo", "agent-browser"} {
		_, err := c.Render(ref)
		if err == nil || !strings.Contains(err.Error(), "unknown skill") || !strings.Contains(err.Error(), embeddedSkillNames) {
			t.Errorf("%s: got %v", ref, err)
		}
	}
	_, err := c.Render("sling/FOO.md")
	if err == nil || !strings.Contains(err.Error(), "has no file") || !strings.Contains(err.Error(), "CONNECTIONS.md, PROJECT.md, TROUBLESHOOTING.md") {
		t.Errorf("sling/FOO.md: got %v", err)
	}
}

func TestSkillRefTraversal(t *testing.T) {
	c := NewSkillCatalog()
	for _, ref := range []string{"../assist.go", "sling/../sling-build", "a/b/c", "", `sling\SKILL.md`, "/SKILL.md"} {
		_, err := c.Render(ref)
		if err == nil || !strings.Contains(err.Error(), "bad skill reference") {
			t.Errorf("%q: got %v", ref, err)
		}
	}
}

func TestSkillCLIMapCoversMCPTools(t *testing.T) {
	actionRe := regexp.MustCompile(`"action"\s*:\s*"(\w+)"`)
	toolRe := regexp.MustCompile("`(connection|database|file_system|api_spec|replication|pipeline)[ .](\\w+)`")

	// first cell of each table row, as words
	rows := [][]string{}
	for _, line := range strings.Split(SkillsCLIMap, "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 3 {
			continue
		}
		rows = append(rows, strings.FieldsFunc(cells[1], func(r rune) bool { return r == ' ' || r == '/' }))
	}
	covered := func(words ...string) bool {
		for _, row := range rows {
			n := 0
			for _, w := range words {
				for _, cell := range row {
					if cell == w {
						n++
						break
					}
				}
			}
			if n == len(words) {
				return true
			}
		}
		return false
	}

	skills, err := NewSkillCatalog().Skills()
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, s := range skills {
		names := []string{"SKILL.md"}
		for _, f := range s.Files {
			names = append(names, f.Name)
		}
		for _, name := range names {
			data, err := fs.ReadFile(SkillsFS, "skills/"+s.Name+"/"+name)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range actionRe.FindAllStringSubmatch(string(data), -1) {
				found++
				if !covered(m[1]) {
					t.Errorf("%s/%s: MCP action %q has no row in skills_cli.md", s.Name, name, m[1])
				}
			}
			for _, m := range toolRe.FindAllStringSubmatch(string(data), -1) {
				found++
				if !covered(m[1], m[2]) {
					t.Errorf("%s/%s: MCP action %q has no row in skills_cli.md", s.Name, name, m[1]+" "+m[2])
				}
			}
		}
	}
	if found == 0 {
		t.Fatal("found no MCP actions in the skills")
	}
}
