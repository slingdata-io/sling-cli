package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[mGKHJA-Z]`)

type testCase struct {
	ID      int               `yaml:"id"`    // unique in its own suite file
	After   []string          `yaml:"after"` // local IDs, or full IDs (<key>.<id>) of other suites
	Group   string            `yaml:"group"` // comma-separated; runs one at a time per group
	Name    string            `yaml:"name"`
	Run     string            `yaml:"run"`
	Env     map[string]string `yaml:"env"`
	Err     bool              `yaml:"err"`
	Rows    string            `yaml:"rows"`    // number of rows
	Bytes   string            `yaml:"bytes"`   // number of bytes
	Streams string            `yaml:"streams"` // number of streams
	Fails   string            `yaml:"fails"`   // number of fails

	Suite string `yaml:"suite"` // path to a nested CLI test suite yaml file to merge in
	Key   string `yaml:"key"`   // namespace of the nested suite, default from its file name

	OutputContains       []string `yaml:"output_contains"`
	OutputDoesNotContain []string `yaml:"output_does_not_contain"`

	suiteKey string // "" in the root suite, "<key>" or "<parent>.<key>" in a nested suite
	fullID   string // "<id>" in the root suite, "<suiteKey>.<id>" in a nested suite
}

var (
	cliSuiteKeyRegex  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	cliNumSpecRegex   = regexp.MustCompile(`^\d+(\+|-\d+)?$`)
	cliSelectorRegex  = regexp.MustCompile(`^[a-z0-9_][a-z0-9_.+,-]*$`)
	cliSuiteKeyPrefix = []string{"suite.cli.", "suite."}
)

// cliSuite holds the cases of a root CLI suite and of its nested suites.
// A nested suite has its own ID space, so its cases are "<key>.<id>".
type cliSuite struct {
	cases []testCase
	keys  map[string]bool
	ids   map[string]bool
}

func loadCLISuite(filePath string) (*cliSuite, error) {
	s := &cliSuite{keys: map[string]bool{"": true}, ids: map[string]bool{}}
	if err := s.load(filePath, ""); err != nil {
		return nil, err
	}

	for _, tc := range s.cases {
		for _, id := range tc.After {
			if !s.ids[id] {
				return nil, g.Error("case %s has `after: %s`, but no case has that ID", tc.fullID, id)
			}
		}
	}

	return s, nil
}

func (s *cliSuite) load(filePath, key string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return g.Error(err, "could not read test suite file: %s", filePath)
	}

	entries := []testCase{}
	if err = yaml.Unmarshal(content, &entries); err != nil {
		return g.Error(err, "could not unmarshal test suite file: %s", filePath)
	}

	qualify := func(id string) string {
		if key == "" {
			return id
		}
		return key + "." + id
	}

	for _, tc := range entries {
		if tc.Suite != "" {
			nestedPath := tc.Suite
			if !filepath.IsAbs(nestedPath) {
				nestedPath = filepath.Join(filepath.Dir(filePath), nestedPath)
			}

			childKey := tc.Key
			if childKey == "" {
				childKey = strings.TrimSuffix(filepath.Base(nestedPath), filepath.Ext(nestedPath))
				for _, prefix := range cliSuiteKeyPrefix {
					childKey = strings.TrimPrefix(childKey, prefix)
				}
				childKey = strings.ReplaceAll(childKey, ".", "_")
			}
			if !cliSuiteKeyRegex.MatchString(childKey) {
				return g.Error("invalid key %#v for nested suite %s in %s", childKey, tc.Suite, filePath)
			}

			childKey = qualify(childKey)
			if s.keys[childKey] {
				return g.Error("duplicate suite key %#v in %s", childKey, filePath)
			}
			s.keys[childKey] = true

			if err := s.load(nestedPath, childKey); err != nil {
				return g.Error(err, "could not load nested suite referenced in %s", filePath)
			}
			continue
		}

		if tc.ID <= 0 {
			return g.Error("case %#v in %s has no id", tc.Name, filePath)
		}

		tc.suiteKey = key
		tc.fullID = qualify(cast.ToString(tc.ID))
		if s.ids[tc.fullID] {
			return g.Error("duplicate id %d in %s", tc.ID, filePath)
		}
		s.ids[tc.fullID] = true

		for i, id := range tc.After {
			if _, err := strconv.Atoi(id); err == nil {
				tc.After[i] = qualify(id)
			}
		}

		s.cases = append(s.cases, tc)
	}

	return nil
}

// Select returns the cases that a selector such as "1-5,9+,build,arrow.701"
// matches, in file order. A bare number refers to the root suite. A key alone
// selects its suite and the suites nested in it. An empty selector selects all.
func (s *cliSuite) Select(selector string) ([]testCase, error) {
	if strings.TrimSpace(selector) == "" {
		return s.cases, nil
	}

	type matcher struct {
		key    string
		all    bool
		lo, hi int
	}

	matchers := []matcher{}
	for _, token := range strings.Split(selector, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		key, spec := "", token
		if !cliNumSpecRegex.MatchString(token) {
			key, spec = token, ""
			if i := strings.LastIndex(token, "."); i > 0 && cliNumSpecRegex.MatchString(token[i+1:]) {
				key, spec = token[:i], token[i+1:]
			}
			if !s.keys[key] {
				return nil, g.Error("unknown suite key %#v in test selection %#v", key, selector)
			}
		}

		m := matcher{key: key, all: spec == ""}
		switch {
		case m.all:
		case strings.HasSuffix(spec, "+"):
			m.lo, m.hi = cast.ToInt(strings.TrimSuffix(spec, "+")), int(^uint(0)>>1)
		case strings.Contains(spec, "-"):
			parts := strings.SplitN(spec, "-", 2)
			m.lo, m.hi = cast.ToInt(parts[0]), cast.ToInt(parts[1])
		default:
			m.lo = cast.ToInt(spec)
			m.hi = m.lo
		}
		matchers = append(matchers, m)
	}

	selected := []testCase{}
	for _, tc := range s.cases {
		for _, m := range matchers {
			if m.all && (tc.suiteKey == m.key || strings.HasPrefix(tc.suiteKey, m.key+".")) ||
				!m.all && tc.suiteKey == m.key && tc.ID >= m.lo && tc.ID <= m.hi {
				selected = append(selected, tc)
				break
			}
		}
	}

	return selected, nil
}

func TestCLI(t *testing.T) {
	args := os.Args
	var parallelMode bool

	for _, arg := range args {
		if arg == "-d" || arg == "--debug" {
			os.Setenv("DEBUG", "LOW")
			env.InitLogger()
		}
		if arg == "-t" || arg == "--trace" {
			os.Setenv("DEBUG", "TRACE")
			env.InitLogger()
		}
		if arg == "-p" || arg == "--parallel" {
			parallelMode = true
		}

		if arg == "-a" || arg == "--all" {
			os.Setenv("RUN_ALL", "true") // runs all test, don't fail earlys
		}
	}
	for _, arg := range args[1:] {
		if cliSelectorRegex.MatchString(arg) {
			allTestContext.Map.Set("TESTS", arg)
		}
	}

	bin := os.Getenv("SLING_BIN")
	if bin == "" {
		bin = "./sling"
		if !g.PathExists(bin) {
			t.Fatalf("SLING_BIN environment variable is not set")
			return
		}
	}

	bin = "cmd/sling/" + bin

	defaultEnv := g.KVArrToMap(os.Environ()...)

	// Resolve the freshly-built sling binary to an absolute path.
	absBin := bin
	if wd, err := os.Getwd(); err == nil {
		repoRoot := filepath.Clean(filepath.Join(wd, "..", ".."))
		absBin = filepath.Clean(filepath.Join(repoRoot, bin))
		defaultEnv["PATH"] = filepath.Dir(absBin) + string(os.PathListSeparator) + defaultEnv["PATH"]
	}

	suite, err := loadCLISuite("../../tests/suite.cli.yaml")
	if !g.AssertNoError(t, err) {
		return
	}

	tnsV, _ := allTestContext.Map.Get("TESTS")
	cases, err := suite.Select(cast.ToString(tnsV))
	if !g.AssertNoError(t, err) {
		return
	}

	tests := []testCase{}
	for _, tc := range cases {
		if len(tc.Env) == 0 {
			tc.Env = map[string]string{}
		}

		if tc.Rows != "" {
			tc.Env["SLING_ROW_CNT"] = tc.Rows
		}
		if tc.Bytes != "" {
			tc.Env["SLING_TOTAL_BYTES"] = tc.Bytes
		}
		if tc.Streams != "" {
			tc.Env["SLING_STREAM_CNT"] = tc.Streams
		}
		if tc.Fails != "" {
			tc.Env["SLING_CONSTRAINT_FAILS"] = tc.Fails
		}

		tests = append(tests, tc)
	}

	running := cmap.New[testCase]()

	// a case takes all of its groups at once, or waits
	var groupMu sync.Mutex
	groupRun := map[string]bool{}
	takeGroups := func(groups []string) bool {
		groupMu.Lock()
		defer groupMu.Unlock()
		for _, group := range groups {
			if groupRun[group] {
				return false
			}
		}
		for _, group := range groups {
			groupRun[group] = true
		}
		return true
	}
	releaseGroups := func(groups []string) {
		groupMu.Lock()
		defer groupMu.Unlock()
		for _, group := range groups {
			delete(groupRun, group)
		}
	}

	// a case waits until the selected cases of its `after` IDs finish
	var pendingMu sync.Mutex
	pending := map[string]int{}
	for _, tc := range tests {
		pending[tc.fullID]++
	}
	afterDone := func(ids []string) bool {
		pendingMu.Lock()
		defer pendingMu.Unlock()
		for _, id := range ids {
			if pending[id] > 0 {
				return false
			}
		}
		return true
	}
	finish := func(id string) {
		pendingMu.Lock()
		defer pendingMu.Unlock()
		pending[id]--
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// group-waiters don't appear in `running`, so an empty map
				// doesn't mean the run is over
				if ids := running.Keys(); len(ids) > 0 {
					now := time.Now().Format(time.DateTime)
					env.Println(env.YellowString(g.F("%s -- running => %s", now, g.Marshal(ids))))
				}
			}
		}
	}()

	allTestContext.SetConcurrencyLimit(1)
	if parallelMode {
		g.Warn("using parallel mode")
		allTestContext.SetConcurrencyLimit(8)
	}

	// allDone tracks completion of every test, including re-queued attempts
	allDone := sync.WaitGroup{}

	for _, tt := range tests {
		if !assert.NotEmpty(t, tt.Run, "Command is empty") {
			break
		}

		testID := g.F("%s/%s", tt.fullID, tt.Name)

		allDone.Add(1)

		// attempts re-queue themselves when their groups are taken, releasing
		// the concurrency slot while waiting, so other tests keep it busy
		var attempt func(tt testCase, testID string)
		attempt = func(tt testCase, testID string) {
			if allTestContext.Ctx.Err() != nil {
				allDone.Done() // canceled, this test will never run
				return
			}

			allTestContext.Wg.Read.Add()

			groups := lo.Compact(lo.Map(strings.Split(tt.Group, ","), func(group string, _ int) string {
				return strings.TrimSpace(group)
			}))

			if !afterDone(tt.After) || (len(groups) > 0 && !takeGroups(groups)) {
				// do not hold a concurrency slot (or the spawn loop) while
				// waiting on deps or groups: release the slot and retry async
				allTestContext.Wg.Read.Done()
				go func() {
					time.Sleep(time.Second)
					if allTestContext.Ctx.Err() != nil {
						allDone.Done() // canceled, this test will never run
						return
					}
					attempt(tt, testID)
				}()
				return
			}

			go func() {
				defer allDone.Done()
				defer finish(tt.fullID)
				defer allTestContext.Wg.Read.Done()
				if len(groups) > 0 {
					defer releaseGroups(groups)
				}

				t.Run(testID, func(t *testing.T) {
					running.Set(tt.fullID, tt)
					defer running.Remove(tt.fullID)

					now := time.Now().Format(time.DateTime)
					env.Println(env.GreenString(g.F("%s -- %s | ", now, tt.fullID) + tt.Run))

					p, err := process.NewProc("bash")
					if !g.AssertNoError(t, err) {
						return
					}
					p.Capture = true
					if os.Getenv("DEBUG") != "" {
						p.Print = true
					}
					p.WorkDir = "../.."

					// set new env
					p.Env = map[string]string{}
					for k, v := range defaultEnv {
						p.Env[k] = v
					}
					for k, v := range tt.Env {
						p.Env[k] = v
					}

					// create a tmp bash script with the command in tmp folder
					tmpDir := os.TempDir()
					tmpFile, err := os.CreateTemp(tmpDir, g.F("sling_cli_test.%s.*.sh", tt.fullID))
					if err != nil {
						t.Fatalf("Failed to create temp file: %v", err)
					}
					defer os.Remove(tmpFile.Name())

					// write the command to the tmp file
					lines := []string{
						"#!/bin/bash",
						"set -e",
						"shopt -s expand_aliases",
						g.F("alias sling=%s", absBin),
						tt.Run,
					}
					content := strings.Join(lines, "\n")
					_, err = tmpFile.WriteString(content)
					if err != nil {
						t.Fatalf("Failed to write command to temp file: %v", err)
					}
					tmpFile.Close()

					// run
					err = p.Run(tmpFile.Name())
					if tt.Err {
						assert.Error(t, err)
					} else {
						assert.NoError(t, err)
					}

					// check output
					stderr := ansiEscapeRegex.ReplaceAllString(p.Stderr.String(), "")
					stdout := ansiEscapeRegex.ReplaceAllString(p.Stdout.String(), "")
					for _, contains := range tt.OutputContains {
						if contains == "" {
							continue
						}

						found := false
						if strings.Contains(stderr, contains) {
							found = true
						}
						if strings.Contains(stdout, contains) {
							found = true
						}
						assert.True(t, found, "Output does not contain %#v", contains)
					}
					for _, notContain := range tt.OutputDoesNotContain {
						if notContain == "" {
							continue
						}

						found := false
						if strings.Contains(stderr, notContain) {
							found = true
						}
						if strings.Contains(stdout, notContain) {
							found = true
						}
						assert.False(t, found, "Output contains %#v", notContain)
					}

					// Track failure inside the subtest where t refers to the subtest
					if t.Failed() {
						testFailuresMux.Lock()
						testFailures = append(testFailures, testFailure{
							connType: "CLI",
							testID:   testID,
							otherIDs: lo.Filter(running.Keys(), func(k string, i int) bool {
								return k != tt.fullID
							}),
						})
						testFailuresMux.Unlock()
					}
				})
			}()
		}
		attempt(tt, testID)

		// cancel early if not specified (check parent test failure status)
		if t.Failed() && !cast.ToBool(os.Getenv("RUN_ALL")) {
			allTestContext.Cancel()
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	// wait for all tests to complete, re-queued attempts included
	allDone.Wait()
}

func TestBase64(t *testing.T) {
	payload := `{
		"name": "Sling",
		"outputfilename": "Sling",
		"author": {
			"name": "Fritz Larco",
			"email": "support@slingdata.io"
		}
	}`

	encoded := base64.URLEncoding.EncodeToString([]byte(payload))
	g.Info(encoded)
	decoded, err := base64.URLEncoding.DecodeString(encoded)
	g.AssertNoError(t, err)
	assert.Equal(t, payload, string(decoded))

	compressor := iop.NewCompressor(iop.GzipCompressorType)
	compressed, err := io.ReadAll(compressor.Compress(strings.NewReader(payload)))
	g.AssertNoError(t, err)
	encodedGzip := base64.URLEncoding.EncodeToString(compressed)
	g.Info(encodedGzip)

	decodedGzip, err := base64.URLEncoding.DecodeString(encodedGzip)
	g.AssertNoError(t, err)
	decompressedReader, err := compressor.Decompress(bytes.NewReader(decodedGzip))
	g.AssertNoError(t, err)

	decompressed, err := io.ReadAll(decompressedReader)
	g.AssertNoError(t, err)
	assert.Equal(t, payload, string(decompressed))

	g.Info("%d => %d", len(encoded), len(encodedGzip))
}

type fakeGracefulRun struct {
	stops    int
	statuses int
}

func (f *fakeGracefulRun) Stop() bool {
	f.stops++
	return f.stops == 1
}

func (f *fakeGracefulRun) PrintStatus() { f.statuses++ }

func TestSignals(t *testing.T) {
	s := newSignals()
	assert.False(t, s.stopRun(syscall.SIGINT)) // no run: the old signal path
	assert.Equal(t, 0, s.ExitCode(0))

	run := &fakeGracefulRun{}
	detach := s.Attach(run)
	assert.True(t, s.stopRun(syscall.SIGTERM))  // first signal: graceful stop
	assert.False(t, s.stopRun(syscall.SIGTERM)) // second signal: the old signal path
	assert.Equal(t, 111, s.ExitCode(0))
	detach()
	assert.False(t, s.stopRun(syscall.SIGINT))

	// watch: first SIGINT stops the run, second goes to hardStop with code 11
	s, run = newSignals(), &fakeGracefulRun{}
	defer s.Attach(run)()
	done, hard := make(chan struct{}), make(chan os.Signal, 1)
	go s.watch(done, func(sig os.Signal) { hard <- sig })
	s.status <- os.Interrupt
	s.stop <- syscall.SIGINT
	s.stop <- syscall.SIGINT
	assert.Equal(t, syscall.SIGINT, <-hard)
	assert.Equal(t, 2, run.stops)
	assert.Equal(t, 1, run.statuses)
	assert.Equal(t, 11, s.ExitCode(0))
}

func TestOneShotRun(t *testing.T) {
	sling.SetContinuousRunMode(true)
	defer sling.SetContinuousRunMode(false)

	inner := false
	run := oneShotRun(func(string, *sling.Config, ...string) error {
		inner = sling.IsContinuousRunMode()
		return nil
	})
	assert.NoError(t, run("repl.yaml", nil))
	assert.False(t, inner)                      // the nested run is one-shot
	assert.True(t, sling.IsContinuousRunMode()) // the outer run stays continuous

	sling.SetContinuousRunMode(false)
	assert.NoError(t, run("repl.yaml", nil))
	assert.False(t, sling.IsContinuousRunMode())
}

func TestLoadCLISuite(t *testing.T) {
	suite, err := loadCLISuite("../../tests/suite.cli.yaml")
	if !assert.NoError(t, err) {
		return
	}
	assert.Greater(t, len(suite.cases), 300)

	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		assert.NoError(t, os.WriteFile(path, []byte(content), 0644))
		return path
	}

	write("suite.cli.build.yaml", `
- {id: 1, run: b1}
- {id: 2, run: b2, after: [1]}
- {id: 3, run: b3, after: [val.1]}
- suite: suite.cli.deep.yaml
`)
	write("suite.cli.deep.yaml", `- {id: 1, run: d1}`)
	write("v.yaml", `- {id: 1, run: v1}`)
	root := write("root.yaml", `
- {id: 1, run: r1}
- {id: 2, run: r2, after: [1]}
- suite: suite.cli.build.yaml
- suite: v.yaml
  key: val
`)

	suite, err = loadCLISuite(root)
	if !assert.NoError(t, err) {
		return
	}

	ids := func(selector string) []string {
		cases, err := suite.Select(selector)
		assert.NoError(t, err, selector)
		return lo.Map(cases, func(tc testCase, _ int) string { return tc.fullID })
	}
	assert.Equal(t, []string{"1", "2", "build.1", "build.2", "build.3", "build.deep.1", "val.1"}, ids(""))
	assert.Equal(t, []string{"1"}, ids("1"))
	assert.Equal(t, []string{"2"}, ids("2+"))
	assert.Equal(t, []string{"1", "build.2", "build.3"}, ids("build.2-3,1"))
	assert.Equal(t, []string{"build.1", "build.2", "build.3", "build.deep.1"}, ids("build"))
	assert.Equal(t, []string{"build.deep.1", "val.1"}, ids("build.deep.1,val"))
	assert.Equal(t, []string{"build.1"}, suite.cases[3].After)
	assert.Equal(t, []string{"val.1"}, suite.cases[4].After)

	_, err = suite.Select("nope.1")
	assert.ErrorContains(t, err, `unknown suite key "nope"`)

	for content, msg := range map[string]string{
		"- {id: 1, run: a}\n- {id: 1, run: b}": "duplicate id 1",
		"- {id: 1, run: a, after: [7]}":        "no case has that ID",
		"- {run: a}":                           "has no id",
		"- suite: v.yaml\n- suite: v.yaml":     `duplicate suite key "v"`,
		"- suite: v.yaml\n  key: Bad":          `invalid key "Bad"`,
		"- {id: 1, run: a}\n- suite: v.yaml\n  key: x\n- {id: 2, run: b, after: [x.9]}": "no case has that ID",
	} {
		_, err := loadCLISuite(write("bad.yaml", content))
		assert.ErrorContains(t, err, msg, content)
	}
}
