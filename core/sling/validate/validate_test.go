package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/sling/build"
)

func TestParseEnvNoInterpolate(t *testing.T) {
	os.Setenv("MY_VALIDATE_SECRET", "SuperSecretValueXYZ")
	t.Cleanup(func() { os.Unsetenv("MY_VALIDATE_SECRET") })

	body := []byte(`
connections:
  MY_PG:
    type: postgres
    password: LiteralPass123ABC
    secret_access_key: ${MY_VALIDATE_SECRET}
`)
	parsed, err := parseDTO(KindEnv, body)
	if err != nil {
		t.Fatal(err)
	}
	s := mustJSON(t, parsed)
	if strings.Contains(s, "SuperSecretValueXYZ") {
		t.Fatal("interpolated env secret into parse output")
	}
	if strings.Contains(s, "LiteralPass123ABC") {
		t.Fatal("literal password leaked")
	}
	if !strings.Contains(s, "${MY_VALIDATE_SECRET}") {
		t.Fatal("env ref was not passed through")
	}
}

func TestParseUnknownExplicit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "foo.yaml")
	if err := os.WriteFile(path, []byte("foo: bar\n"), 0644); err != nil {
		t.Fatal(err)
	}
	res := ParseFile(path, Options{})
	if res.OK || res.Kind != KindUnknown {
		t.Fatalf("got ok=%v kind=%q", res.OK, res.Kind)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDetectFileKindOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		path string
		want Kind
	}{
		{
			name: "steps beats connections",
			body: "steps:\n  - type: log\n    message: hi\nconnections:\n  PG:\n    type: postgres\n",
			path: "env.yaml",
			want: KindPipeline,
		},
		{
			name: "replication needs source target streams",
			body: "source: PG\ntarget: SF\nstreams:\n  public.t:\n    object: t\n",
			path: "foo.yaml",
			want: KindReplication,
		},
		{
			name: "api spec name plus endpoints",
			body: "name: demo\nendpoints:\n  ping:\n    request:\n      url: https://example.com\n",
			path: "spec.yaml",
			want: KindAPISpec,
		},
		{
			name: "api spec name plus dynamic_endpoints",
			body: "name: demo\ndynamic_endpoints:\n  - iterate: []\n",
			path: "spec.yaml",
			want: KindAPISpec,
		},
		{
			name: "monitor connection plus objects",
			body: "connection: PG\nobjects:\n  - public.t\n",
			path: "mon.yaml",
			want: KindMonitor,
		},
		{
			name: "routine",
			body: "routines:\n  nightly:\n    steps: []\n",
			path: "r.yaml",
			want: KindRoutine,
		},
		{
			name: "env connections",
			body: "connections:\n  PG:\n    type: postgres\n",
			path: "other.yaml",
			want: KindEnv,
		},
		{
			name: "content wins over env.yaml name",
			body: "steps:\n  - type: log\n    message: hi\n",
			path: "env.yaml",
			want: KindPipeline,
		},
		{
			name: "env.yaml fallback when content unknown",
			body: "foo: bar\n",
			path: "env.yaml",
			want: KindEnv,
		},
		{
			name: "sling_build.yml path rule",
			body: "target: POSTGRES\ndefaults:\n  mode: full-refresh\n",
			path: "sling_build.yml",
			want: KindBuild,
		},
		{
			name: "sling_build.yaml path rule",
			body: "target: POSTGRES\ndefaults:\n  mode: full-refresh\n",
			path: "sling_build.yaml",
			want: KindBuild,
		},
		{
			name: "unknown yaml stays unknown",
			body: "services:\n  db:\n    image: postgres\n",
			path: "docker-compose.yml",
			want: KindUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := DetectFileKind([]byte(tt.body), tt.path)
			if got != tt.want {
				t.Fatalf("DetectFileKind() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDetectFileKindBuildDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, build.ConfigFileName)
	if err := os.WriteFile(path, []byte("target: POSTGRES\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got := DetectFileKind(nil, dir)
	if got != KindBuild {
		t.Fatalf("directory with sling_build.yml: got %q want %q", got, KindBuild)
	}
}

func TestDetectFileKindBuildDirYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sling_build.yaml")
	if err := os.WriteFile(path, []byte("target: POSTGRES\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got := DetectFileKind(nil, dir)
	if got != KindBuild {
		t.Fatalf("directory with sling_build.yaml: got %q want %q", got, KindBuild)
	}
}

func TestRedactEnvRefsPassThrough(t *testing.T) {
	t.Parallel()
	in := map[string]any{
		"connections": map[string]any{
			"PG": map[string]any{
				"password":          "LiteralPass123ABC",
				"secret_access_key": "${MY_VALIDATE_SECRET}",
			},
		},
	}
	out, _ := Redact(in).(map[string]any)
	conns := out["connections"].(map[string]any)
	pg := conns["PG"].(map[string]any)
	if pg["password"] != "***" {
		t.Fatalf("password = %#v, want ***", pg["password"])
	}
	if pg["secret_access_key"] != "${MY_VALIDATE_SECRET}" {
		t.Fatalf("ref = %#v, want ${MY_VALIDATE_SECRET}", pg["secret_access_key"])
	}
}

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

const testReplication = `
source: '{source}'
target: LOCAL
streams:
  main.example:
    object: raw.example
`

func TestProjectManifestExplicit(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "replications/r.yaml", testReplication)
	path := writeFile(t, dir, "sling_project.yml", `
name: demo
jobs:
  daily:
    file: replications/r.yaml
    schedules: ["0 6 * * *"]
`)
	res := ParseFile(path, Options{})
	if !res.OK || res.Kind != KindProject {
		t.Fatalf("got ok=%v kind=%q err=%s", res.OK, res.Kind, res.Error)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", res.Warnings)
	}
}

func TestProjectManifestInWalk(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "replications/r.yaml", testReplication)
	writeFile(t, dir, "sling_project.yml", "name: demo\n")

	var found bool
	for _, r := range ParsePaths([]string{dir}, Options{}) {
		if r.Kind == KindProject {
			found = true
			if !r.OK {
				t.Fatalf("manifest failed: %s", r.Error)
			}
		}
	}
	if !found {
		t.Fatal("walk did not include the manifest")
	}
}

func TestProjectBadCron(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "replications/r.yaml", testReplication)
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    file: replications/r.yaml
    schedules: ["not a cron"]
`)
	res := ParseFile(path, Options{})
	if res.OK {
		t.Fatal("bad cron should fail")
	}
	if !strings.Contains(res.Error, "invalid schedule") {
		t.Fatalf("unclear error: %s", res.Error)
	}
}

func TestProjectCronDescriptors(t *testing.T) {
	for _, spec := range []string{"@daily", "@hourly", "@every 1h", "@every 2d", "0 6 * * *", "*/5 * * * *"} {
		if err := validateCron(spec); err != nil {
			t.Fatalf("%q should be valid: %s", spec, err)
		}
	}
	for _, spec := range []string{"", "not a cron", "0 6 * *", "@nope"} {
		if err := validateCron(spec); err == nil {
			t.Fatalf("%q should be invalid", spec)
		}
	}
}

func TestProjectMultiScheduleWarns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "replications/r.yaml", testReplication)
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    file: replications/r.yaml
    schedules: ["0 6 * * *", "0 7 * * *"]
`)
	res := ParseFile(path, Options{})
	if !res.OK {
		t.Fatalf("multi schedule should warn, not fail: %s", res.Error)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "only the first cron fires") {
		t.Fatalf("got warnings %v", res.Warnings)
	}
	if AnyFailed([]FileResult{res}) {
		t.Fatal("warnings must not fail the run")
	}
}

func TestProjectEmptyJobFile(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    schedules: ["0 6 * * *"]
`)
	res := ParseFile(path, Options{})
	if res.OK {
		t.Fatal("job without file should fail")
	}
	if !strings.Contains(res.Error, "'file'") {
		t.Fatalf("unclear error: %s", res.Error)
	}
}

func TestProjectJobMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    file: replications/nope.yaml
`)
	res := ParseFile(path, Options{})
	if res.OK {
		t.Fatal("missing job file should fail")
	}
	if !strings.Contains(res.Error, "does not exist") {
		t.Fatalf("unclear error: %s", res.Error)
	}
}

func TestProjectJobWrongKind(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "env.yaml", "connections:\n  MY_PG:\n    type: postgres\n")
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    file: env.yaml
`)
	res := ParseFile(path, Options{})
	if res.OK {
		t.Fatal("env-kind job file should fail")
	}
	if !strings.Contains(res.Error, "expected a replication or pipeline") {
		t.Fatalf("unclear error: %s", res.Error)
	}
}

func TestProjectStreamOverrideMissWarns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "replications/r.yaml", testReplication)
	path := writeFile(t, dir, "sling_project.yml", `
jobs:
  daily:
    file: replications/r.yaml
    streams: [main.example, not_there]
`)
	res := ParseFile(path, Options{})
	if !res.OK {
		t.Fatalf("stream miss should warn, not fail: %s", res.Error)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "not_there") {
		t.Fatalf("got warnings %v", res.Warnings)
	}
}

func TestPipelineStepBadPathWarns(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "pipelines/p.yaml", `
steps:
  - type: replication
    path: replications/nope.yaml
`)
	res := ParseFile(path, Options{})
	if !res.OK {
		t.Fatalf("bad step path should warn, not fail: %s", res.Error)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "does not exist") {
		t.Fatalf("got warnings %v", res.Warnings)
	}
}

func TestPipelineStepGoodPathNoWarn(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "pipelines/replications/r.yaml", testReplication)
	path := writeFile(t, dir, "pipelines/p.yaml", `
steps:
  - type: replication
    path: replications/r.yaml
`)
	res := ParseFile(path, Options{})
	if !res.OK {
		t.Fatalf("failed: %s", res.Error)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", res.Warnings)
	}
}

func TestParsePipelineNestedUnknownStepType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		compile bool
		wantErr string
	}{
		{
			name: "top-level unknown type",
			body: `
steps:
  - type: run
    source: LOCAL
`,
			wantErr: `pipeline step 0 has unknown type "run"`,
		},
		{
			name: "nested group unknown type",
			body: `
steps:
  - type: group
    loop: [1]
    steps:
      - type: set
        key: table_name
        value: t
      - type: run
        source: LOCAL
        target: DUCKDB
      - type: log
        message: done
`,
			wantErr: `pipeline step 0.steps.1 has unknown type "run"`,
		},
		{
			name: "nested group unknown type compile",
			body: `
steps:
  - type: group
    loop: [1]
    steps:
      - type: log
        message: hi
      - type: run
        source: LOCAL
`,
			compile: true,
			wantErr: `pipeline step 0.steps.1 has unknown type "run"`,
		},
		{
			name: "doubly nested group unknown type",
			body: `
steps:
  - type: group
    steps:
      - type: group
        steps:
          - type: run
            source: LOCAL
`,
			wantErr: `pipeline step 0.steps.0.steps.0 has unknown type "run"`,
		},
		{
			name: "shorthand group unknown nested type",
			body: `
steps:
  - group:
      loop: [1]
      steps:
        - type: run
          source: LOCAL
`,
			wantErr: `pipeline step 0.steps.0 has unknown type "run"`,
		},
		{
			name: "nested non-mapping step",
			body: `
steps:
  - type: group
    steps:
      - not-a-mapping
`,
			wantErr: `pipeline step 0.steps.0 must be a mapping`,
		},
		{
			name: "valid nested group",
			body: `
steps:
  - type: group
    loop: [1]
    steps:
      - type: log
        message: hi
      - type: query
        connection: DUCKDB
        query: select 1
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := writeFile(t, dir, "loop.yaml", tt.body)
			res := ParseFile(path, Options{Compile: tt.compile})
			if tt.wantErr == "" {
				if !res.OK {
					t.Fatalf("valid nested group failed: %s", res.Error)
				}
				return
			}
			if res.OK {
				t.Fatal("expected unknown nested step type to fail")
			}
			if !strings.Contains(res.Error, tt.wantErr) {
				t.Fatalf("error %q does not contain %q", res.Error, tt.wantErr)
			}
		})
	}
}

func TestLintReplicationUnknownConnWarns(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "r.yaml", `
source: WAREHOUSE_PROD_EVAL_XYZ
target: LOCAL
defaults:
  object: main.{stream_table}
streams:
  public.orders:
    mode: full-refresh
`)
	res := ParseFile(path, Options{})
	if !res.OK {
		t.Fatalf("unknown conn should warn, not fail: %s", res.Error)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "WAREHOUSE_PROD_EVAL_XYZ") {
		t.Fatalf("got warnings %v", res.Warnings)
	}
}

func TestLintAPISpecMissingURL(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "spec.yaml", `
name: missing-url
endpoints:
  ping:
    request:
      method: GET
`)
	res := ParseFile(path, Options{})
	if res.OK {
		t.Fatal("missing request url should fail")
	}
	if !strings.Contains(strings.ToLower(res.Error), "url") {
		t.Fatalf("unclear error: %s", res.Error)
	}
}

func TestResultRowCompiled(t *testing.T) {
	row := resultRow(FileResult{Kind: KindReplication, OK: true, Compiled: false}, false)
	if row["compiled"] != false {
		t.Fatalf("compiled = %#v, want false", row["compiled"])
	}
	row = resultRow(FileResult{Kind: KindReplication, OK: true, Compiled: true}, false)
	if row["compiled"] != true {
		t.Fatalf("compiled = %#v, want true", row["compiled"])
	}
}
