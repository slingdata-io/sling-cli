package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type testCase struct {
	ID      int               `yaml:"id"`
	After   []int             `yaml:"after"`
	Group   string            `yaml:"group"`
	Name    string            `yaml:"name"`
	Run     string            `yaml:"run"`
	Env     map[string]string `yaml:"env"`
	Err     bool              `yaml:"err"`
	Rows    string            `yaml:"rows"`    // number of rows
	Bytes   string            `yaml:"bytes"`   // number of bytes
	Streams string            `yaml:"streams"` // number of streams
	Fails   string            `yaml:"fails"`   // number of fails

	OutputContains       []string `yaml:"output_contains"`
	OutputDoesNotContain []string `yaml:"output_does_not_contain"`
}

func TestCLI(t *testing.T) {
	args := os.Args
	var parallelMode bool

	for _, arg := range args {
		if arg == "-d" || arg == "--debug" {
			os.Setenv("DEBUG", "true")
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
		if arg != "" && unicode.IsDigit(rune(arg[0])) {
			os.Setenv("TESTS", arg)
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

	// Load tests from suite.cli.yaml
	filePath := "tests/suite.cli.yaml"
	content, err := os.ReadFile(filePath)
	if !g.AssertNoError(t, err) {
		return
	}

	cases := []testCase{}
	err = yaml.Unmarshal(content, &cases)
	if !g.AssertNoError(t, err) {
		return
	}

	// get test numbers from env
	testNumbers := []int{}
	tns := os.Getenv("TESTS")
	if tns != "" {
		for _, tn := range strings.Split(tns, ",") {
			if strings.HasSuffix(tn, "+") {
				start := cast.ToInt(strings.TrimSuffix(tn, "+"))

				for _, tc := range cases {
					if tc.ID < start {
						continue
					}
					testNumbers = append(testNumbers, tc.ID)
				}
			} else if parts := strings.Split(tn, "-"); len(parts) == 2 {
				start := cast.ToInt(parts[0])
				end := cast.ToInt(parts[1])
				for testNumber := start; testNumber <= end; testNumber++ {
					testNumbers = append(testNumbers, testNumber)
				}
			} else if testNumber := cast.ToInt(tn); testNumber > 0 {
				testNumbers = append(testNumbers, testNumber)
			}
		}
	}

	tests := []testCase{}
	for _, tc := range cases {
		if len(testNumbers) > 0 && !g.In(tc.ID, testNumbers...) {
			continue
		}

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
	groupRun := cmap.New[testCase]()

	go func() {
		ticker := time.NewTicker(20 * time.Second)
		for range ticker.C {
			ids := running.Keys()
			if len(ids) == 0 {
				ticker.Stop()
				return
			}
			now := time.Now().Format(time.DateTime)
			env.Println(env.YellowString(g.F("%s -- running => %s", now, g.Marshal(ids))))
		}
	}()

	testContext.SetConcurrencyLimit(1)
	if parallelMode {
		g.Warn("using parallel mode")
		testContext.SetConcurrencyLimit(8)
	}

	for _, tt := range tests {
		if !assert.NotEmpty(t, tt.Run, "Command is empty") {
			break
		}

		testID := g.F("%d/%s", tt.ID, tt.Name)

		testContext.Wg.Read.Add()

		go func(tt testCase) {
			defer testContext.Wg.Read.Done()

			if tt.Group != "" {
			retryGroup:
				if groupRun.Has(tt.Group) {
					time.Sleep(time.Second)
					goto retryGroup
				}
				groupRun.Set(tt.Group, tt)
				defer groupRun.Remove(tt.Group)
			}

			t.Run(testID, func(t *testing.T) {
				running.Set(g.CastToString(tt.ID), tt)
				defer running.Remove(g.CastToString(tt.ID))

			retry:
				for _, needID := range tt.After {
					if running.Has(g.CastToString(needID)) {
						time.Sleep(time.Second)
						goto retry
					}
				}

				now := time.Now().Format(time.DateTime)
				env.Println(env.GreenString(g.F("%s -- %02d | ", now, tt.ID) + tt.Run))

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
				tmpFile, err := os.CreateTemp(tmpDir, g.F("sling_cli_test.%02d.*.sh", tt.ID))
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				defer os.Remove(tmpFile.Name())

				// write the command to the tmp file
				lines := []string{
					"#!/bin/bash",
					"set -e",
					"shopt -s expand_aliases",
					g.F("alias sling=%s", bin),
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
				stderr := p.Stderr.String()
				stdout := p.Stdout.String()
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
							return k != testID
						}),
					})
					testFailuresMux.Unlock()
				}
			})
		}(tt)

		// cancel early if not specified (check parent test failure status)
		if t.Failed() && !cast.ToBool(os.Getenv("RUN_ALL")) {
			testContext.Cancel()
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	testContext.Wg.Read.Wait()
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
