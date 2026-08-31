package project

import (
	"bufio"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/flarco/g"
)

//go:embed all:scaffold
var scaffoldFS embed.FS

const defaultEntity = "example"

// Options controls project scaffolding.
type Options struct {
	Dir    string
	Name   string
	Source string
	Target string
	Entity string
	Yes    bool
	Force  bool
}

// Result lists files written by Init.
type Result struct {
	Dir   string
	Name  string
	Files []string
}

// Init writes a canonical Sling project into dir.
func Init(opts Options) (*Result, error) {
	dir := opts.Dir
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, g.Error(err, "could not get working directory")
		}
		dir = wd
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, g.Error(err, "could not resolve project directory")
	}

	if err := os.MkdirAll(abs, 0755); err != nil {
		return nil, g.Error(err, "could not create project directory")
	}

	if !opts.Force {
		if HasManifest(abs) {
			return nil, g.Error("project already exists in %s; re-run with --force to overwrite", abs)
		}
		if root, findErr := FindRoot(abs); findErr == nil && root != "" && root != abs {
			return nil, g.Error("already inside a project at %s; re-run with --force", root)
		}
	}

	source := strings.TrimSpace(opts.Source)
	target := strings.TrimSpace(opts.Target)
	if source == "" || target == "" {
		return nil, g.Error("source and target are required")
	}

	name := strings.TrimSpace(opts.Name)
	if name == "" {
		name = filepath.Base(abs)
	}
	entity := strings.TrimSpace(opts.Entity)
	if entity == "" {
		entity = defaultEntity
	}
	sourceSlug := slug(source)

	replacer := strings.NewReplacer(
		"{{SOURCE_SLUG}}", sourceSlug,
		"{{SOURCE}}", source,
		"{{TARGET}}", target,
		"{{NAME}}", name,
		"{{ENTITY}}", entity,
	)

	planned, err := plannedFiles(abs, sourceSlug, entity, replacer)
	if err != nil {
		return nil, err
	}

	if err := confirmOverwrite(planned, opts.Yes); err != nil {
		return nil, err
	}

	written := make([]string, 0, len(planned))
	for _, f := range planned {
		if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
			return nil, g.Error(err, "could not create directory %s", filepath.Dir(f.path))
		}
		if err := os.WriteFile(f.path, f.body, 0644); err != nil {
			return nil, g.Error(err, "could not write %s", f.path)
		}
		rel, _ := filepath.Rel(abs, f.path)
		written = append(written, filepath.ToSlash(rel))
	}

	return &Result{Dir: abs, Name: name, Files: written}, nil
}

type plannedFile struct {
	path string
	body []byte
}

func plannedFiles(abs, sourceSlug, entity string, replacer *strings.Replacer) ([]plannedFile, error) {
	var out []plannedFile
	err := fs.WalkDir(scaffoldFS, "scaffold", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		body, err := fs.ReadFile(scaffoldFS, path)
		if err != nil {
			return g.Error(err, "could not read scaffold file %s", path)
		}
		rel, err := filepath.Rel("scaffold", path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		rel = strings.TrimSuffix(rel, ".tmpl")
		rel = strings.ReplaceAll(rel, "SOURCE_SLUG", sourceSlug)
		rel = strings.ReplaceAll(rel, "ENTITY", entity)
		if rel == "gitignore" {
			rel = ".gitignore"
		}
		out = append(out, plannedFile{
			path: filepath.Join(abs, filepath.FromSlash(rel)),
			body: []byte(replacer.Replace(string(body))),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func confirmOverwrite(files []plannedFile, yes bool) error {
	existing := []string{}
	for _, f := range files {
		if g.PathExists(f.path) {
			existing = append(existing, f.path)
		}
	}
	if len(existing) == 0 {
		return nil
	}
	if yes {
		return nil
	}
	if !isInteractive() {
		return g.Error("project files already exist; re-run with --yes to overwrite in non-interactive mode")
	}

	fmt.Print("Project files already exist. Overwrite? [y/N]: ")
	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		return g.Error("aborted")
	}
	return nil
}

func isInteractive() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func slug(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	prevUnderscore := false
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore && b.Len() > 0 {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
