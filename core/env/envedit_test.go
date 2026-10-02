package env

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnvFileEditorHandWritten runs edits on files written by hand: blank
// lines, aligned comments, quotes, 4-space indentation, anchors, flow maps,
// CRLF and no final newline. Each result is compared byte for byte with
// testdata/<case>.out.yaml (WRITE_GOLDEN=1 writes them).
func TestEnvFileEditorHandWritten(t *testing.T) {
	cases := []struct {
		name, in string
		op       func(e *EnvFileEditor) error
	}{
		{"hand-add", "hand", func(e *EnvFileEditor) error {
			return e.Set("NEW_PG", g.M("type", "postgres", "host", "h2", "user", "u", "password", "${NEW_PG_PASSWORD}"), EditOptions{})
		}},
		{"hand-update-quoted", "hand", func(e *EnvFileEditor) error {
			return e.Set("PG_PROD", g.M("host", "db2.example.com", "user", "admin2"), EditOptions{AllowOverwrite: true})
		}},
		{"hand-update-port", "hand", func(e *EnvFileEditor) error {
			// the CLI passes strings: a plain number stays a plain number
			return e.Set("PG_PROD", g.M("port", "5433"), EditOptions{AllowOverwrite: true})
		}},
		{"hand-add-key", "hand", func(e *EnvFileEditor) error {
			return e.Set("PG_PROD", g.M("schema", "public", "password", "${PG_PASS}"), EditOptions{AllowOverwrite: true})
		}},
		{"hand-update-flow", "hand", func(e *EnvFileEditor) error {
			return e.Set("DUCK", g.M("options", g.M("threads", 8)), EditOptions{AllowOverwrite: true})
		}},
		{"hand-anchor-merge", "hand", func(e *EnvFileEditor) error {
			return e.Set("S3_COPY", g.M("region", "eu-west-1", "bucket", "other-bucket"), EditOptions{AllowOverwrite: true})
		}},
		{"hand-nested", "hand", func(e *EnvFileEditor) error {
			return e.Set("MY_API", g.M("secrets", g.M("account", "acct_1"), "inputs", g.M("start", "2025-01-01")), EditOptions{AllowOverwrite: true})
		}},
		{"hand-url", "hand", func(e *EnvFileEditor) error {
			return e.Set("PG_URL", g.M("url", "postgres://u:p@h2:5432/db"), EditOptions{AllowOverwrite: true})
		}},
		{"hand-replace", "hand", func(e *EnvFileEditor) error {
			return e.Set("PG_PROD", g.M("type", "postgres", "host", "db.example.com", "port", 5432, "database", "app"),
				EditOptions{Replace: true, AllowOverwrite: true})
		}},
		{"hand-delete-middle", "hand", func(e *EnvFileEditor) error { return e.Delete("DUCK") }},
		{"hand-delete-first", "hand", func(e *EnvFileEditor) error { return e.Delete("PG_PROD") }},
		{"hand-delete-last", "hand", func(e *EnvFileEditor) error { return e.Delete("PG_URL") }},
		{"hand-rename", "hand", func(e *EnvFileEditor) error { return e.Rename("DUCK", "LOCAL_DUCK") }},
		{"hand-setenv", "hand", func(e *EnvFileEditor) error {
			return e.SetEnv(g.M("SLING_THREADS", 8, "NEW_VAR", "x"), true)
		}},
		{"hand-promote", "hand", func(e *EnvFileEditor) error {
			return e.Set("PG_NEW", g.M("type", "postgres", "host", "h", "password", "${PG_NEW_PASSWORD}"),
				EditOptions{EnvUpdates: g.M("PG_NEW_PASSWORD", "hunter3")})
		}},
		{"indent4-add", "indent4", func(e *EnvFileEditor) error {
			return e.Set("NEW", g.M("type", "sqlite", "instance", "a.db", "tags", []any{"z"}), EditOptions{})
		}},
		{"indent4-update", "indent4", func(e *EnvFileEditor) error {
			return e.Set("PG", g.M("host", "b", "tags", []any{"x", "y", "w"}), EditOptions{AllowOverwrite: true})
		}},
		{"indent4-setenv", "indent4", func(e *EnvFileEditor) error {
			return e.SetEnv(g.M("NEW_K", "w"), false)
		}},
		{"no-newline-add", "no-newline", func(e *EnvFileEditor) error {
			return e.Set("B", g.M("type", "duckdb", "instance", "b.db"), EditOptions{})
		}},
		{"crlf-add", "crlf-add", func(e *EnvFileEditor) error {
			return e.Set("B", g.M("type", "duckdb", "instance", "b.db"), EditOptions{EnvUpdates: g.M("NEW_K", "w")})
		}},
		{"empty-conns-add", "empty-conns", func(e *EnvFileEditor) error {
			return e.Set("FIRST", g.M("type", "duckdb", "instance", "a.db"), EditOptions{})
		}},
		{"null-conns-add", "null-conns", func(e *EnvFileEditor) error {
			return e.Set("FIRST", g.M("type", "duckdb", "instance", "a.db"), EditOptions{})
		}},
		{"no-conns-add", "no-conns", func(e *EnvFileEditor) error {
			return e.Set("FIRST", g.M("type", "duckdb", "instance", "a.db"), EditOptions{})
		}},
		{"empty-env-setenv", "empty-env", func(e *EnvFileEditor) error {
			return e.SetEnv(g.M("B_KEY", "2", "A_KEY", "1"), false)
		}},
		{"comments-only-add", "comments-only", func(e *EnvFileEditor) error {
			return e.Set("FIRST", g.M("type", "duckdb", "instance", "a.db"), EditOptions{})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join("testdata", tc.in+".in.yaml"))
			require.NoError(t, err)
			path := editTestPath(t, in)

			e, err := LoadEnvEditor(path)
			require.NoError(t, err)
			require.NoError(t, tc.op(e))
			require.NoError(t, e.Save(""))

			got, err := os.ReadFile(path)
			require.NoError(t, err)
			outPath := filepath.Join("testdata", tc.name+".out.yaml")
			if os.Getenv("WRITE_GOLDEN") != "" {
				require.NoError(t, os.WriteFile(outPath, got, 0o644))
				return
			}
			want, err := os.ReadFile(outPath)
			require.NoError(t, err, "missing golden")
			assert.Equal(t, string(want), string(got))

			// the result must load as an env file
			_, err = LoadEnvEditor(path)
			assert.NoError(t, err)
		})
	}
}

// TestEnvFileEditorChangesOnlyItsLines checks the exact lines an edit removes
// and adds. All other lines must stay, in order.
func TestEnvFileEditorChangesOnlyItsLines(t *testing.T) {
	in, err := os.ReadFile(filepath.Join("testdata", "hand.in.yaml"))
	require.NoError(t, err)

	cases := []struct {
		name           string
		op             func(e *EnvFileEditor) error
		removed, added []string
	}{
		{
			name: "add appends after the last entry",
			op:   func(e *EnvFileEditor) error { return e.Set("NEW", g.M("type", "postgres", "host", "h"), EditOptions{}) },
			// a blank line separates entries in this file, so one comes with it
			added:   []string{"  NEW:", "    type: postgres", "    host: h", ""},
			removed: nil,
		},
		{
			name: "update keeps quotes and comment spacing",
			op: func(e *EnvFileEditor) error {
				return e.Set("PG_PROD", g.M("host", "db2.example.com"), EditOptions{AllowOverwrite: true})
			},
			removed: []string{`    host: "db.example.com"      # primary`},
			added:   []string{`    host: "db2.example.com"      # primary`},
		},
		{
			name: "single quotes stay single quotes",
			op: func(e *EnvFileEditor) error {
				return e.Set("PG_PROD", g.M("user", "it's me"), EditOptions{AllowOverwrite: true})
			},
			removed: []string{`    user: 'admin'`},
			added:   []string{`    user: 'it''s me'`},
		},
		{
			name: "same values change nothing",
			op: func(e *EnvFileEditor) error {
				return e.Set("PG_PROD", g.M("type", "postgres", "port", "5432", "password", "${PG_PASS}"), EditOptions{AllowOverwrite: true})
			},
		},
		{
			name: "env update keeps the inline comment",
			op: func(e *EnvFileEditor) error {
				return e.SetEnv(g.M("PG_PASS", "secret456"), true)
			},
			removed: []string{"  PG_PASS: secret123     # inline"},
			added:   []string{"  PG_PASS: secret456     # inline"},
		},
		{
			name: "replace drops keys with their lines only",
			op: func(e *EnvFileEditor) error {
				return e.Set("DUCK", g.M("type", "duckdb", "instance", "/tmp/a.db"), EditOptions{Replace: true, AllowOverwrite: true})
			},
			removed: []string{"    options: {read_only: true, threads: 4}"},
		},
		{
			name: "a changed ref value gets no comment when one is there",
			op: func(e *EnvFileEditor) error {
				return e.Set("PG_PROD", g.M("password", "${PG_PROD_PASSWORD}"), EditOptions{AllowOverwrite: true})
			},
			removed: []string{"    password: ${PG_PASS}        # from vault"},
			added:   []string{"    password: ${PG_PROD_PASSWORD}        # from vault"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := editTestPath(t, in)
			e, err := LoadEnvEditor(path)
			require.NoError(t, err)
			require.NoError(t, tc.op(e))
			require.NoError(t, e.Save(""))
			got, _ := os.ReadFile(path)

			removed, added := lineDiff(string(in), string(got))
			assert.Equal(t, tc.removed, removed, "removed lines")
			assert.Equal(t, tc.added, added, "added lines")
		})
	}
}

// TestEnvFileEditorRoundTrip adds, renames and deletes entries, and checks
// that undoing each edit gives back the original bytes.
func TestEnvFileEditorRoundTrip(t *testing.T) {
	for _, fixture := range []string{"hand", "indent4", "crlf-add", "no-newline", "delete-middle.in", "update"} {
		t.Run(fixture, func(t *testing.T) {
			name := fixture
			if !strings.HasSuffix(name, ".in") {
				name += ".in"
			}
			in, err := os.ReadFile(filepath.Join("testdata", name+".yaml"))
			require.NoError(t, err)

			steps := []func(e *EnvFileEditor) error{
				func(e *EnvFileEditor) error {
					return e.Set("ROUND_TRIP", g.M("type", "postgres", "host", "h", "port", 5432), EditOptions{})
				},
				func(e *EnvFileEditor) error { return e.Delete("ROUND_TRIP") },
			}
			names := e2eNames(t, in)
			if len(names) > 0 {
				first := names[0]
				steps = append(steps,
					func(e *EnvFileEditor) error { return e.Rename(first, "RENAMED_ENTRY") },
					func(e *EnvFileEditor) error { return e.Rename("RENAMED_ENTRY", first) },
				)
			}

			path := editTestPath(t, in)
			for i, step := range steps {
				e, err := LoadEnvEditor(path)
				require.NoError(t, err)
				require.NoError(t, step(e), "step %d", i)
				require.NoError(t, e.Save(""))
			}
			got, _ := os.ReadFile(path)
			assert.Equal(t, string(in), string(got))
		})
	}
}

// TestEnvFileEditorRefusesUnsafeEdits checks that edits on files sling cannot
// splice fail and leave the file as it was.
func TestEnvFileEditorRefusesUnsafeEdits(t *testing.T) {
	cases := map[string]struct {
		body string
		op   func(e *EnvFileEditor) error
	}{
		"connections is a list": {
			body: "connections:\n  - PG1\nenv:\n  KEEP: me\n",
			op:   func(e *EnvFileEditor) error { return e.Set("PG2", g.M("type", "postgres"), EditOptions{}) },
		},
		"env is a list": {
			body: "connections: {}\nenv:\n  - KEEP\n",
			op:   func(e *EnvFileEditor) error { return e.SetEnv(g.M("NEW_K", "v"), true) },
		},
		"existing entry without overwrite": {
			body: "connections:\n  A:\n    type: postgres\n",
			op:   func(e *EnvFileEditor) error { return e.Set("a", g.M("type", "mysql"), EditOptions{}) },
		},
		"env overwrite without allow": {
			body: "env:\n  K: v\n",
			op:   func(e *EnvFileEditor) error { return e.SetEnv(g.M("K", "w"), false) },
		},
		"rename onto an existing name": {
			body: "connections:\n  A:\n    type: postgres\n  B:\n    type: mysql\n",
			op:   func(e *EnvFileEditor) error { return e.Rename("A", "b") },
		},
		"delete a missing entry": {
			body: "connections:\n  A:\n    type: postgres\n",
			op:   func(e *EnvFileEditor) error { return e.Delete("NOPE") },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := editTestPath(t, []byte(tc.body))
			e, err := LoadEnvEditor(path)
			require.NoError(t, err)
			assert.Error(t, tc.op(e))
			require.NoError(t, e.Save(""))
			after, _ := os.ReadFile(path)
			assert.Equal(t, tc.body, string(after))
		})
	}

}

// TestEnvFileEditorSequentialEdits applies several edits with one editor
// before one Save: each edit sees the lines of the edit before it.
func TestEnvFileEditorSequentialEdits(t *testing.T) {
	body := "# head\n\nconnections:\n  A:\n    type: postgres   # db\n    host: a\n\nenv:\n  K: v\n"
	path := editTestPath(t, []byte(body))
	e, err := LoadEnvEditor(path)
	require.NoError(t, err)
	require.NoError(t, e.Set("B", g.M("type", "duckdb", "instance", "b.db"), EditOptions{}))
	require.NoError(t, e.Set("A", g.M("host", "a2"), EditOptions{AllowOverwrite: true}))
	require.NoError(t, e.Rename("B", "C"))
	require.NoError(t, e.SetEnv(g.M("K2", "w"), false))
	require.NoError(t, e.Save(""))

	got, _ := os.ReadFile(path)
	assert.Equal(t, "# head\n\nconnections:\n  A:\n    type: postgres   # db\n    host: a2\n  C:\n    type: duckdb\n    instance: b.db\n\nenv:\n  K: v\n  K2: w\n", string(got))
}

// e2eNames returns the connection names of body, in file order.
func e2eNames(t *testing.T, body []byte) []string {
	t.Helper()
	e, err := LoadEnvEditorBytes("", body)
	require.NoError(t, err)
	conns := mappingChild(e.root, "connections")
	if conns == nil {
		return nil
	}
	var names []string
	for i := 0; i < len(conns.Content)-1; i += 2 {
		names = append(names, conns.Content[i].Value)
	}
	return names
}

// lineDiff returns the lines of a that are not in b and the lines of b that
// are not in a, from a longest-common-subsequence match.
func lineDiff(a, b string) (removed, added []string) {
	x := strings.Split(strings.TrimSuffix(a, "\n"), "\n")
	y := strings.Split(strings.TrimSuffix(b, "\n"), "\n")
	lcs := make([][]int, len(x)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(y)+1)
	}
	for i := len(x) - 1; i >= 0; i-- {
		for j := len(y) - 1; j >= 0; j-- {
			if x[i] == y[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	i, j := 0, 0
	for i < len(x) || j < len(y) {
		switch {
		case i < len(x) && j < len(y) && x[i] == y[j]:
			i++
			j++
		case j < len(y) && (i == len(x) || lcs[i][j+1] >= lcs[i+1][j]):
			added = append(added, y[j])
			j++
		default:
			removed = append(removed, x[i])
			i++
		}
	}
	return removed, added
}
