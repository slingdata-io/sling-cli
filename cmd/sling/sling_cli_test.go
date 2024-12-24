package main_test

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestCLI(t *testing.T) {
	bin := os.Getenv("SLING_BIN")
	if bin == "" {
		bin = "./sling"
		if !g.PathExists(bin) {
			t.Fatalf("SLING_BIN environment variable is not set")
			return
		}
	}

	p, err := process.NewProc("bash")
	if !g.AssertNoError(t, err) {
		return
	}
	p.Capture = true
	p.WorkDir = "../.."
	bin = "cmd/sling/" + bin

	defaultEnv := g.KVArrToMap(os.Environ()...)

	// Load tests from suite.cli.tsv
	filePath := "tests/suite.cli.tsv"
	dataT, err := iop.ReadCsv(filePath)
	if !g.AssertNoError(t, err) {
		return
	}

	// rewrite correctly for displaying in Github
	c := iop.CSV{Path: filePath, Delimiter: '\t'}
	c.WriteStream(dataT.Stream())

	type testCase struct {
		Number         int
		Name           string
		Command        string
		Env            map[string]string
		Err            bool
		Rows           string // number of rows
		Bytes          string // number of bytes
		Streams        string // number of streams
		Fails          string // number of fails
		OutputContains []string
	}

	// get test numbers from env
	testNumbers := []int{}
	tns := os.Getenv("TESTS")
	if tns != "" {
		for _, tn := range strings.Split(tns, ",") {
			if strings.HasSuffix(tn, "+") {
				start := cast.ToInt(strings.TrimSuffix(tn, "+"))

				for _, rec := range dataT.RecordsString() {
					n := cast.ToInt(rec["n"])
					if n < start {
						continue
					}
					testNumbers = append(testNumbers, n)
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
	for _, record := range dataT.RecordsString() {
		tc := testCase{
			Number:         cast.ToInt(record["n"]),
			Name:           record["test_name"],
			Command:        record["command"],
			Env:            map[string]string{},
			Err:            false,
			Rows:           record["rows"],
			Bytes:          record["bytes"],
			Streams:        record["streams"],
			Fails:          record["fails"],
			OutputContains: strings.Split(record["output_contains"], "|"),
		}
		if len(testNumbers) > 0 && !g.In(tc.Number, testNumbers...) {
			continue
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
		if !assert.NotEmpty(t, tt.Command, "Command is empty") {
			break
		}
		t.Run(g.F("%d/%s", tt.Number, tt.Name), func(t *testing.T) {
			g.Info(env.GreenString(tt.Command))

			// set env
			p.Env = map[string]string{}
			for k, v := range defaultEnv {
				p.Env[k] = v
			}
			for k, v := range tt.Env {
				p.Env[k] = v
			}

			// set print
			p.Print = true

			// create a tmp bash script with the command in tmp folder
			tmpDir := os.TempDir()
			tmpFile, err := os.CreateTemp(tmpDir, "sling_cli_test_*.sh")
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
				tt.Command,
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
			found := false
			stderr := p.Stderr.String()
			stdout := p.Stdout.String()
			for _, contains := range tt.OutputContains {
				if strings.Contains(stderr, contains) {
					found = true
					break
				}
				if strings.Contains(stdout, contains) {
					found = true
					break
				}
			}
			assert.True(t, found, "Output does not contain %v", tt.OutputContains)
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
