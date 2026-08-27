package project_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/sling/build"
	"github.com/slingdata-io/sling-cli/core/sling/validate"
	"github.com/slingdata-io/sling-cli/core/sling/project"
)

func TestScaffoldParseAndLoad(t *testing.T) {
	dir := t.TempDir()
	res, err := project.Init(project.Options{
		Dir:    dir,
		Name:   "wave45",
		Source: "SQLITE",
		Target: "POSTGRES",
		Yes:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || len(res.Files) == 0 {
		t.Fatal("expected scaffolded files")
	}

	required := []string{
		"sling_project.yml",
		"models/sling_build.yml",
		"pipelines/daily.yaml",
		"replications/sqlite.yaml",
		"models/staging/stg_sqlite__example.sql",
		"models/marts/fct_example.sql",
		".gitignore",
	}
	for _, rel := range required {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing %s: %v", rel, err)
		}
	}

	m, err := project.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "wave45" {
		t.Fatalf("manifest name = %q", m.Name)
	}

	yamlRoots := []string{
		filepath.Join(dir, "replications"),
		filepath.Join(dir, "pipelines"),
		filepath.Join(dir, "models"),
	}
	results := validate.ParsePaths(yamlRoots, validate.Options{})
	if len(results) == 0 {
		t.Fatal("parse returned no yaml files")
	}
	var kinds []validate.Kind
	for _, r := range results {
		if !r.OK {
			t.Errorf("parse failed for %s (%s): %s", r.Path, r.Kind, r.Error)
			continue
		}
		kinds = append(kinds, r.Kind)
	}
	if !hasKind(kinds, validate.KindReplication) {
		t.Error("expected a replication yaml")
	}
	if !hasKind(kinds, validate.KindPipeline) {
		t.Error("expected a pipeline yaml")
	}
	if !hasKind(kinds, validate.KindBuild) {
		t.Error("expected a build yaml")
	}

	proj, err := build.LoadProject(filepath.Join(dir, "models"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := proj.Models["stg_sqlite__example"]; !ok {
		t.Fatalf("missing staging model, have %v", modelNames(proj))
	}
	if _, ok := proj.Models["fct_example"]; !ok {
		t.Fatalf("missing mart model, have %v", modelNames(proj))
	}

	_, err = project.Init(project.Options{
		Dir:    dir,
		Source: "SQLITE",
		Target: "POSTGRES",
		Yes:    true,
	})
	if err == nil {
		t.Fatal("expected refuse inside existing project without --force")
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Fatalf("unexpected error: %s", err)
	}
}

func hasKind(kinds []validate.Kind, want validate.Kind) bool {
	for _, k := range kinds {
		if k == want {
			return true
		}
	}
	return false
}

func modelNames(proj *build.BuildProject) []string {
	names := make([]string, 0, len(proj.Models))
	for name := range proj.Models {
		names = append(names, name)
	}
	return names
}
