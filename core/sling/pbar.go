package sling

import (
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	pb "gopkg.in/cheggaaa/pb.v2"
)

var ShowProgress = env.IsInteractiveTerminal()

type ProgressBar struct {
	bar      *pb.ProgressBar
	started  bool
	finished bool
}

// NewPBar creates a new progress bar
func NewPBar(d time.Duration) *ProgressBar {
	pbar := new(pb.ProgressBar)

	pb.RegisterElement("status", elementStatus, true)
	pb.RegisterElement("counters", elementCounters, true)
	pb.RegisterElement("bytes", elementBytes, true)
	pb.RegisterElement("rowRate", elementRowRate, true)
	pb.RegisterElement("byteRate", elementByteRate, true)
	tmpl := `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{ bytes . | blue }} {{ status . }}`
	if g.IsDebugLow() {
		pb.RegisterElement("mem", elementMem, true)
		pb.RegisterElement("cpu", elementCPU, true)
		// tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{ bytes . | blue }} {{ byteRate . }} {{ mem . }} {{ cpu . }} {{ status . }}`
		tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{ bytes . | blue }} {{ mem . }} {{ cpu . }} {{ status . }}`
	}
	barTmpl := pb.ProgressBarTemplate(tmpl)
	pbar = barTmpl.New(0)
	pbar.SetRefreshRate(d)
	pbar.SetWidth(40)
	return &ProgressBar{
		bar: pbar,
	}
}

// SetStatus sets the progress bar status
func (pb *ProgressBar) SetStatus(status string) {
	if !pb.finished {
		pb.bar.Set("status", status)
		pb.bar.Write()
	}
}

func (pb *ProgressBar) Start() {
	pb.started = true
	pb.bar.Start()
}

func (pb *ProgressBar) Finish() {
	if !pb.finished {
		pb.bar.Finish()
		pb.finished = true
	}
}

// https://github.com/cheggaaa/pb/blob/master/v3/element.go
// calculates the RAM percent
var elementMem pb.ElementFunc = func(state *pb.State, args ...string) string {
	memRAM, err := mem.VirtualMemory()
	if err != nil {
		return ""
	}
	return g.F("| %d%% MEM", cast.ToInt(memRAM.UsedPercent))
}

// calculates the CPU percent
var elementCPU pb.ElementFunc = func(state *pb.State, args ...string) string {
	cpuPct, err := cpu.Percent(0, false)
	if err != nil || len(cpuPct) == 0 {
		return ""
	}
	return g.F("| %d%% CPU", cast.ToInt(cpuPct[0]))
}

var elementStatus pb.ElementFunc = func(state *pb.State, args ...string) string {
	status := cast.ToString(state.Get("status"))
	if status == "" {
		return ""
	}
	return g.F("| %s", status)
}

type argsHelper []string

func (args argsHelper) getOr(n int, value string) string {
	if len(args) > n {
		return args[n]
	}
	return value
}

func (args argsHelper) getNotEmptyOr(n int, value string) (v string) {
	if v = args.getOr(n, value); v == "" {
		return value
	}
	return
}

var elementCounters pb.ElementFunc = func(state *pb.State, args ...string) string {
	var f string
	if state.Total() > 0 {
		f = argsHelper(args).getNotEmptyOr(0, "%s / %s")
	} else {
		f = argsHelper(args).getNotEmptyOr(1, "%[1]s")
	}
	return fmt.Sprintf(
		f, humanize.Commaf(cast.ToFloat64(state.Value())),
		humanize.Commaf(cast.ToFloat64(state.Total())),
	)
}

var elementBytes pb.ElementFunc = func(state *pb.State, args ...string) string {
	bytes := cast.ToString(state.Get("bytes"))
	if bytes == "0 B" {
		return ""
	}
	return g.F("%s", bytes)
}

var elementByteRate pb.ElementFunc = func(state *pb.State, args ...string) string {
	bytes := cast.ToString(state.Get("byteRate"))
	if bytes == "0 B" {
		return ""
	}
	return g.F("| %s", bytes)
}

var elementRowRate pb.ElementFunc = func(state *pb.State, args ...string) string {
	bytes := cast.ToString(state.Get("rowRate"))
	return g.F("| %s", bytes)
}
