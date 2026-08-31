package project_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/sling/project"
)

func TestParseJobsUnknownFields(t *testing.T) {
	body := []byte(`
name: demo
jobs:
  orders_hourly:
    file: replications/postgres_orders.yaml
    schedules: ["0 * * * *"]
    streams: [public.orders]
    mode: incremental
    retries: 2
    timezone: UTC
    extra_ignored: true
`)
	m, err := project.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	spec, ok := m.Jobs["orders_hourly"]
	if !ok {
		t.Fatal("missing orders_hourly")
	}
	if spec.File != "replications/postgres_orders.yaml" {
		t.Fatalf("file = %q", spec.File)
	}
	if len(spec.Schedules) != 1 || spec.Schedules[0] != "0 * * * *" {
		t.Fatalf("schedules = %#v", spec.Schedules)
	}
	if spec.Mode != "incremental" {
		t.Fatalf("mode = %q", spec.Mode)
	}
	if spec.Retries == nil || *spec.Retries != 2 {
		t.Fatalf("retries = %#v", spec.Retries)
	}
}

func TestLoadJobsFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, project.ManifestFileName)
	body := []byte("name: fromfile\njobs:\n  daily:\n    file: pipelines/daily.yaml\n    schedules: [\"0 6 * * *\"]\n")
	if err := os.WriteFile(path, body, 0644); err != nil {
		t.Fatal(err)
	}
	m, err := project.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Jobs["daily"].File != "pipelines/daily.yaml" {
		t.Fatalf("jobs = %#v", m.Jobs)
	}
}

func TestPlanJobsCreateUpdateOrphanRename(t *testing.T) {
	m := project.Manifest{
		Jobs: map[string]project.JobSpec{
			"orders_hourly": {
				File:      "replications/postgres_orders.yaml",
				Schedules: []string{"0 * * * *"},
				Streams:   []string{"public.orders"},
			},
			"daily": {
				File:      "pipelines/daily.yaml",
				Schedules: []string{"0 6 * * *", "0 18 * * *"},
			},
		},
	}

	empty := project.PlanJobs(m, nil, project.PlanOptions{})
	if got := actions(empty); strings.Join(got, ",") != "create,create" {
		t.Fatalf("empty stamped actions = %v plan=\n%s", got, project.FormatPlan(empty))
	}
	out := project.FormatPlan(empty)
	if !strings.Contains(out, "orders_hourly") || !strings.Contains(out, "create") {
		t.Fatalf("plan output missing create:\n%s", out)
	}
	if !strings.Contains(out, "only the first cron fires") {
		t.Fatalf("expected multi-schedule warning:\n%s", out)
	}

	stamped := []project.StampedJob{
		{SourceKey: "orders_hourly", File: "replications/old.yaml", Schedules: []string{"0 * * * *"}},
		{SourceKey: "legacy", File: "replications/legacy.yaml"},
		{SourceKey: "unrelated", File: "pipelines/old_daily.yaml", Schedules: []string{"0 6 * * *"}},
	}
	updated := project.PlanJobs(m, stamped, project.PlanOptions{})
	got := actions(updated)
	if !contains(got, "update") || !contains(got, "create") || !contains(got, "orphan") {
		t.Fatalf("actions = %v\n%s", got, project.FormatPlan(updated))
	}
	if contains(got, "delete") {
		t.Fatalf("unpruned plan must not label orphans as delete: %v", got)
	}
	if !strings.Contains(project.FormatPlan(updated), "orphan managed job") {
		t.Fatal("expected orphan warning without prune")
	}

	pruned := project.PlanJobs(m, stamped, project.PlanOptions{Prune: true})
	if !contains(actions(pruned), "delete") {
		t.Fatalf("prune plan missing delete: %v", actions(pruned))
	}

	renamed := project.PlanJobs(
		project.Manifest{Jobs: map[string]project.JobSpec{
			"orders_v2": {File: "replications/postgres_orders.yaml", Schedules: []string{"0 * * * *"}},
		}},
		[]project.StampedJob{{SourceKey: "orders_hourly", File: "replications/postgres_orders.yaml", Schedules: []string{"0 * * * *"}}},
		project.PlanOptions{Renames: map[string]string{"orders_hourly": "orders_v2"}},
	)
	if !contains(actions(renamed), "rename") {
		t.Fatalf("rename plan = %v\n%s", actions(renamed), project.FormatPlan(renamed))
	}
	if contains(actions(renamed), "create") || contains(actions(renamed), "delete") {
		t.Fatalf("rename should not create/delete: %v", actions(renamed))
	}
}

func TestPlanJobsUnmanagedEmptySourceKeyIgnored(t *testing.T) {
	m := project.Manifest{Jobs: map[string]project.JobSpec{
		"a": {File: "replications/a.yaml"},
	}}
	plan := project.PlanJobs(m, []project.StampedJob{
		{SourceKey: "", Name: "Default Job (a.yaml)", File: "replications/a.yaml"},
	}, project.PlanOptions{})
	if contains(actions(plan), "delete") {
		t.Fatalf("unmanaged empty source_key treated as stamped: %v", actions(plan))
	}
}

func actions(p project.JobPlan) []string {
	out := make([]string, 0, len(p.Items))
	for _, it := range p.Items {
		if it.Action == project.PlanKeep {
			continue
		}
		out = append(out, string(it.Action))
	}
	return out
}

func contains(items []string, want string) bool {
	for _, s := range items {
		if s == want {
			return true
		}
	}
	return false
}

func TestSetProjectIDKeepsJobsComment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, project.ManifestFileName)
	body := "name: demo\n# project_id: set when linked\n\n# jobs:\n#   daily:\n#     file: pipelines/daily.yaml\n#     schedules: [\"0 6 * * *\"]\n"
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if err := project.SetProjectID(dir, "proj_123"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(got)
	if !strings.Contains(s, "proj_123") {
		t.Fatalf("missing project_id:\n%s", s)
	}
	if !strings.Contains(s, "# jobs:") {
		t.Fatalf("lost # jobs: comment:\n%s", s)
	}
}

func TestResolveLinkProjectID(t *testing.T) {
	id, err := project.ResolveLinkProjectID("from-token", nil, nil)
	if err != nil || id != "from-token" {
		t.Fatalf("token id: %q %v", id, err)
	}

	one := []project.LinkProject{{ID: "only", Name: "Only"}}
	id, err = project.ResolveLinkProjectID("", one, nil)
	if err != nil || id != "only" {
		t.Fatalf("single listed: %q %v", id, err)
	}

	many := []project.LinkProject{{ID: "a"}, {ID: "b"}}
	_, err = project.ResolveLinkProjectID("", many, nil)
	if err == nil {
		t.Fatal("expected error for multiple without pick")
	}
	id, err = project.ResolveLinkProjectID("", many, func(listed []project.LinkProject) (string, error) {
		return listed[1].ID, nil
	})
	if err != nil || id != "b" {
		t.Fatalf("pick: %q %v", id, err)
	}
}

func writeManifest(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, project.ManifestFileName), []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestResolveJobHit(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, `
name: demo
jobs:
  daily:
    file: replications/r.yaml
    mode: truncate
    streams: [public.orders]
`)
	root, spec, err := project.ResolveJob(dir, "daily")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(root, filepath.Base(dir)) {
		t.Fatalf("unexpected root %s", root)
	}
	if spec.File != "replications/r.yaml" || spec.Mode != "truncate" {
		t.Fatalf("unexpected spec %+v", spec)
	}
	if len(spec.Streams) != 1 || spec.Streams[0] != "public.orders" {
		t.Fatalf("unexpected streams %v", spec.Streams)
	}
}

func TestResolveJobFromSubfolder(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "jobs:\n  daily:\n    file: r.yaml\n")
	sub := filepath.Join(dir, "replications", "nested")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := project.ResolveJob(sub, "daily"); err != nil {
		t.Fatal(err)
	}
}

func TestResolveJobMissListsKeys(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "jobs:\n  daily:\n    file: a.yaml\n  hourly:\n    file: b.yaml\n")
	_, _, err := project.ResolveJob(dir, "nope")
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "daily") || !strings.Contains(msg, "hourly") {
		t.Fatalf("error does not list the keys: %s", msg)
	}
}

func TestResolveJobNoJobs(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "name: demo\n")
	_, _, err := project.ResolveJob(dir, "daily")
	if err == nil || !strings.Contains(err.Error(), "no jobs") {
		t.Fatalf("got %v", err)
	}
}

func TestResolveJobNoProject(t *testing.T) {
	dir := t.TempDir()
	_, _, err := project.ResolveJob(dir, "daily")
	if err == nil || !strings.Contains(err.Error(), "no sling project found") {
		t.Fatalf("got %v", err)
	}
}

func TestResolveJobEmptyKey(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "jobs:\n  daily:\n    file: a.yaml\n")
	if _, _, err := project.ResolveJob(dir, "  "); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}
