package main

import (
	"fmt"
	"github.com/flarco/gutil/net"
	core2 "github.com/slingdata/sling/core"
	"github.com/slingdata/sling/core/elt"
	"github.com/slingdata/sling/core/env"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	h "github.com/flarco/gutil"
	"github.com/jedib0t/go-pretty/table"
	"github.com/kardianos/osext"
	"github.com/slingdata/sling/core/iop"
	"github.com/spf13/cast"
)

func listLocalConns() {
	rows := [][]interface{}{}
	for key, val := range h.KVArrToMap(os.Environ()...) {
		if !strings.Contains(val, ":/") {
			continue
		}
		conn := iop.DataConn{ID: key, URL: val}
		if conn.GetType() == iop.ConnTypeNone || conn.GetType() == iop.ConnTypeFileHTTP {
			continue
		}
		connType := ""
		switch {
		case conn.IsDbType():
			connType = "database"
		case conn.IsFileType():
			connType = "file-system"
		}
		connType = conn.GetTypeNameLong()
		rows = append(rows, []interface{}{conn.ID, connType})
	}

	sort.Slice(rows, func(i, j int) bool {
		return cast.ToString(rows[i][0]) < cast.ToString(rows[j][0])
	})

	T := table.NewWriter()
	T.AppendHeader(table.Row{"Conn Name", "Conn Type"})
	for _, row := range rows {
		T.AppendRow(row)
	}

	println(T.Render())
	os.Exit(0)
}


func processELT(c *h.CliSC) (err error) {

	doLog := func() {
		logUsage(h.M(
			"cmd", "elt",
			"error", h.ErrorText(err),
		))
	}
	defer doLog()


	local, ok := c.Vals["local-conns"]
	if ok && cast.ToBool(local) {
		listLocalConns()
	}

	cfg := elt.Config{}
	cfgStr := ""
	showExamples := false
	// saveAsJob := false

	// determine if stdin data is piped
	// https://stackoverflow.com/a/26567513
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cfg.StdIn = true
	}

	for k, v := range c.Vals {
		switch k {
		case "remote":
			cfg.Remote = cast.ToBool(v)
		case "config":
			cfgStr = cast.ToString(v)
		case "src-file":
			cfg.SrcFileObj = cast.ToString(v)
		case "src-table":
			cfg.SrcTable = cast.ToString(v)
		case "src-sql", "query":
			cfg.SrcSQL = cast.ToString(v)
		case "tgt-file":
			cfg.TgtFileObj = cast.ToString(v)
		case "src-conn", "conn":
			cfg.SrcConnObj = cast.ToString(v)
		case "tgt-conn":
			cfg.TgtConnObj = cast.ToString(v)
		case "tgt-table":
			cfg.TgtTable = cast.ToString(v)
		case "pre-sql":
			cfg.TgtPreSQL = cast.ToString(v)
		case "post-sql":
			cfg.TgtPostSQL = cast.ToString(v)
		case "stdout":
			cfg.StdOut = cast.ToBool(v)
		case "options":
			cfg.Options = cast.ToString(v)
		case "mode":
			cfg.Mode = cast.ToString(v)
		case "examples":
			showExamples = cast.ToBool(v)
		case "email":
			cfg.Email = cast.ToString(v)
			// case "save":
			// 	saveAsJob = cast.ToBool(v)
		}
	}

	if showExamples {
		println(examples)
		return nil
	}


	if cfgStr != "" {
		err = cfg.Unmarshal(cfgStr)
	} else {
		err = cfg.Prepare()
	}
	if err != nil {
		return h.Error(err, "Unable to parse config string")
	}

	task := elt.NewTask(0, cfg)
	if task.Err != nil {
		return h.Error(task.Err)
	}

	doLog = func() {
		srcType := task.Cfg.SrcConn.GetTypeNameLong()
		if srcType == "" {
			srcType = task.Cfg.SrcFile.GetTypeNameLong()
		}
		tgtType := task.Cfg.TgtConn.GetTypeNameLong()
		if tgtType == "" {
			tgtType = task.Cfg.TgtFile.GetTypeNameLong()
		}
		logUsage(h.M(
			"cmd", "elt",
			"error", h.ErrorText(task.Err),
			"job_type", task.Type,
			"job_mode", task.Cfg.Mode,
			"job_remote", task.Cfg.Remote,
			"job_status", task.Status,
			"job_src_type", srcType,
			"job_tgt_type", tgtType,
			"job_start_time", task.StartTime,
			"job_end_time", task.EndTime,
			"job_rows_count", task.GetCount(),
		))
	}

	// set context
	task.Ctx = ctx

	// run task
	err = task.Execute()
	if err != nil {
		return h.Error(err)
	}

	return nil
}

func updateCLI(c *h.CliSC) (err error) {
	// Print Progress: https://gist.github.com/albulescu/e61979cc852e4ee8f49c
	go logUsage(h.M(
		"cmd", "update",
	))

	url := ""
	if runtime.GOOS == "linux" {
		url = "https://f.slingdata.io/linux/sling"
	} else if runtime.GOOS == "darwin" {
		url = "https://f.slingdata.io/mac/sling"
	} else if runtime.GOOS == "windows" {
		url = "https://f.slingdata.io/windows/sling.exe"
	} else {
		return fmt.Errorf("OS Unsupported: %s", runtime.GOOS)
	}

	execFileName, err := osext.Executable()
	if err != nil {
		return h.Error(err, "Unable to determine executable path")
	}

	fileStat, _ := os.Stat(execFileName)
	fileMode := fileStat.Mode()

	filePath := execFileName + ".new"

	h.Info("Checking update...")
	err = net.DownloadFile(url, filePath)
	if err != nil {
		println("Unable to download update!")
		return fmt.Errorf(strings.ReplaceAll(err.Error(), url, ""))
	}
	err = os.Chmod(filePath, fileMode)
	if err != nil {
		println("Unable to make new binary executable.")
		return err
	}

	versionOut, err := exec.Command(filePath, "--version").Output()
	if err != nil {
		println("Unable to get version of current binary executable.")
		return err
	}

	if strings.HasSuffix(strings.TrimSpace(string(versionOut)), core2.Version) {
		os.Remove(filePath)
		h.Info("Already using latest version: " + core2.Version)
		return nil
	}

	os.Rename(execFileName, execFileName+".old")
	os.Rename(filePath, execFileName)

	os.Remove(execFileName + ".old")

	h.Info("Updated to " + strings.TrimSpace(string(versionOut)))

	return nil
}


func logUsage(m map[string]interface{}) {
	m["application_name"] = "sling"
	m["version"] = core2.Version
	m["os"] = runtime.GOOS
	env.LogEvent(m)
}
