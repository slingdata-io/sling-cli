package project

import (
	"fmt"
	"sort"
	"strings"

	"github.com/flarco/g"
)

const multiScheduleWarn = "only the first cron fires"

// JobSpec is one entry under manifest jobs:. Extra YAML keys are ignored.
type JobSpec struct {
	File      string            `yaml:"file,omitempty" json:"file,omitempty"`
	Schedules []string          `yaml:"schedules,omitempty" json:"schedules,omitempty"`
	Streams   []string          `yaml:"streams,omitempty" json:"streams,omitempty"`
	Mode      string            `yaml:"mode,omitempty" json:"mode,omitempty"`
	Variables map[string]string `yaml:"variables,omitempty" json:"variables,omitempty"`
	Retries   *int              `yaml:"retries,omitempty" json:"retries,omitempty"`
	Timezone  string            `yaml:"timezone,omitempty" json:"timezone,omitempty"`
}

// StampedJob is a platform job keyed by source_key.
type StampedJob struct {
	SourceKey string
	Name      string
	File      string
	Schedules []string
	Streams   []string
	Mode      string
	Variables map[string]string
	Retries   *int
	Timezone  string
}

// PlanAction is one row in a job deploy plan.
type PlanAction string

const (
	PlanCreate PlanAction = "create"
	PlanUpdate PlanAction = "update"
	PlanDelete PlanAction = "delete"
	PlanOrphan PlanAction = "orphan"
	PlanRename PlanAction = "rename"
	PlanKeep   PlanAction = "keep"
)

// PlanItem is one planned change.
type PlanItem struct {
	Action    PlanAction
	SourceKey string
	OldKey    string
	Spec      JobSpec
	Warning   string
}

// PlanOptions controls rename mapping and prune.
type PlanOptions struct {
	Renames map[string]string
	Prune   bool
}

// JobPlan is the diff of manifest jobs vs stamped jobs.
type JobPlan struct {
	Items    []PlanItem
	Warnings []string
}

// PlanJobs diffs manifest jobs against the stamped (source_key) set.
func PlanJobs(m Manifest, stamped []StampedJob, opts PlanOptions) JobPlan {
	plan := JobPlan{}
	jobs := m.Jobs
	if jobs == nil {
		jobs = map[string]JobSpec{}
	}

	stampedByKey := map[string]StampedJob{}
	for _, s := range stamped {
		if strings.TrimSpace(s.SourceKey) == "" {
			continue
		}
		stampedByKey[s.SourceKey] = s
	}

	renames := opts.Renames
	if renames == nil {
		renames = map[string]string{}
	}
	oldOfNew := map[string]string{}
	for oldKey, newKey := range renames {
		if oldKey == "" || newKey == "" || oldKey == newKey {
			continue
		}
		sj, ok := stampedByKey[oldKey]
		if !ok {
			plan.Warnings = append(plan.Warnings, g.F("rename source %s is not a managed job", oldKey))
			continue
		}
		if _, exists := jobs[newKey]; !exists {
			plan.Warnings = append(plan.Warnings, g.F("rename target %s is not in the manifest", newKey))
			continue
		}
		delete(stampedByKey, oldKey)
		sj.SourceKey = newKey
		stampedByKey[newKey] = sj
		oldOfNew[newKey] = oldKey
	}

	keys := make([]string, 0, len(jobs))
	for k := range jobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	used := map[string]bool{}
	for _, key := range keys {
		spec := jobs[key]
		if w := spec.scheduleWarning(key); w != "" {
			plan.Warnings = append(plan.Warnings, w)
		}
		if strings.TrimSpace(spec.File) == "" {
			plan.Warnings = append(plan.Warnings, g.F("job %s has no file", key))
		}
		sj, ok := stampedByKey[key]
		if !ok {
			plan.Items = append(plan.Items, PlanItem{Action: PlanCreate, SourceKey: key, Spec: spec})
			continue
		}
		used[key] = true
		item := PlanItem{SourceKey: key, Spec: spec}
		if oldKey, renamed := oldOfNew[key]; renamed {
			item.Action = PlanRename
			item.OldKey = oldKey
			plan.Items = append(plan.Items, item)
			continue
		}
		if jobSpecEqual(spec, sj) {
			item.Action = PlanKeep
		} else {
			item.Action = PlanUpdate
		}
		plan.Items = append(plan.Items, item)
	}

	orphanKeys := make([]string, 0, len(stampedByKey))
	for key := range stampedByKey {
		if used[key] {
			continue
		}
		if _, inManifest := jobs[key]; inManifest {
			continue
		}
		orphanKeys = append(orphanKeys, key)
	}
	sort.Strings(orphanKeys)
	for _, key := range orphanKeys {
		sj := stampedByKey[key]
		action := PlanDelete
		if !opts.Prune {
			action = PlanOrphan
			plan.Warnings = append(plan.Warnings, g.F("orphan managed job %s (use --prune to delete)", key))
		}
		plan.Items = append(plan.Items, PlanItem{
			Action:    action,
			SourceKey: key,
			Spec: JobSpec{
				File:      sj.File,
				Schedules: sj.Schedules,
				Streams:   sj.Streams,
				Mode:      sj.Mode,
				Variables: sj.Variables,
				Retries:   sj.Retries,
				Timezone:  sj.Timezone,
			},
		})
	}

	return plan
}

func (s JobSpec) scheduleWarning(key string) string {
	if len(s.Schedules) > 1 {
		return g.F("job %s has %d schedules; %s", key, len(s.Schedules), multiScheduleWarn)
	}
	return ""
}

func jobSpecEqual(spec JobSpec, sj StampedJob) bool {
	if spec.File != sj.File {
		return false
	}
	if spec.Mode != sj.Mode || spec.Timezone != sj.Timezone {
		return false
	}
	if !strSliceEqual(spec.Schedules, sj.Schedules) {
		return false
	}
	if !strSliceEqual(spec.Streams, sj.Streams) {
		return false
	}
	if !strMapEqual(spec.Variables, sj.Variables) {
		return false
	}
	return intPtrEqual(spec.Retries, sj.Retries)
}

func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func strMapEqual(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func intPtrEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// FormatPlan prints a job plan as a table plus warnings.
func FormatPlan(p JobPlan) string {
	rows := [][]any{}
	for _, it := range p.Items {
		if it.Action == PlanKeep {
			continue
		}
		key := it.SourceKey
		if it.Action == PlanRename && it.OldKey != "" {
			key = it.OldKey + " -> " + it.SourceKey
		}
		rows = append(rows, []any{string(it.Action), key, it.Spec.File, strings.Join(it.Spec.Schedules, ", ")})
	}

	var b strings.Builder
	if len(rows) == 0 {
		b.WriteString("No job changes.")
	} else {
		b.WriteString(g.PrettyTable([]string{"Action", "Job", "File", "Schedules"}, rows))
	}
	for _, w := range p.Warnings {
		fmt.Fprintf(&b, "\nwarning: %s", w)
	}
	return b.String()
}

// HasPushActions reports whether the plan creates, updates, renames, or (when prune) deletes.
func (p JobPlan) HasPushActions() bool {
	for _, it := range p.Items {
		switch it.Action {
		case PlanCreate, PlanUpdate, PlanRename, PlanDelete:
			return true
		}
	}
	return false
}

// ResolveJob finds the project root from startDir and returns the job spec
// for key. The error lists the available keys when the key misses.
func ResolveJob(startDir, key string) (root string, spec JobSpec, err error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", spec, g.Error("job key is empty")
	}

	root, err = FindRoot(startDir)
	if err != nil {
		return "", spec, g.Error("no sling project found; run `sling init` or pass a file path")
	}

	m, err := Load(root)
	if err != nil {
		return root, spec, g.Error(err, "could not load the project manifest")
	}

	spec, ok := m.Jobs[key]
	if !ok {
		return root, spec, g.Error("job %s is not in the manifest%s", key, availableKeysSuffix(m.Jobs))
	}
	return root, spec, nil
}

func availableKeysSuffix(jobs map[string]JobSpec) string {
	if len(jobs) == 0 {
		return ". The manifest has no jobs"
	}
	keys := make([]string, 0, len(jobs))
	for k := range jobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return ". Available jobs: " + strings.Join(keys, ", ")
}
