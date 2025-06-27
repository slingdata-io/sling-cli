package main_test

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"strings"
	"testing"
	"unicode"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

type testCase struct {
	ID             int               `yaml:"id"`
	Needs          []int             `yaml:"needs"`
	Name           string            `yaml:"name"`
	Run            string            `yaml:"run"`
	Env            map[string]string `yaml:"env"`
	Err            bool              `yaml:"err"`
	Rows           string            `yaml:"rows"`    // number of rows
	Bytes          string            `yaml:"bytes"`   // number of bytes
	Streams        string            `yaml:"streams"` // number of streams
	Fails          string            `yaml:"fails"`   // number of fails
	OutputContains []string          `yaml:"output_contains"`
}

func TestCLI(t *testing.T) {
	args := os.Args
	for _, arg := range args {
		if arg == "-d" || arg == "--debug" {
			os.Setenv("DEBUG", "true")
			env.InitLogger()
		}
		if arg == "-t" || arg == "--trace" {
			os.Setenv("DEBUG", "TRACE")
			env.InitLogger()
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

		tc.Env = map[string]string{
			"DEBUG": os.Getenv("DEBUG"),
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

	for _, tt := range tests {
		if !assert.NotEmpty(t, tt.Run, "Command is empty") {
			break
		}
		t.Run(g.F("%d/%s", tt.ID, tt.Name), func(t *testing.T) {
			env.Println(env.GreenString(g.F("%02d | ", tt.ID) + tt.Run))

			p, err := process.NewProc("bash")
			if !g.AssertNoError(t, err) {
				return
			}
			p.Capture = true
			p.Print = true
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
		})
		if t.Failed() {
			break
		}
	}
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
