package sling

import "github.com/flarco/g"

type HookType string
type HookKind string
type OnFailType string

const (
	HookKindHook HookKind = "hook"
	HookKindStep HookKind = "step"
)

var HookRunReplication func(string, *Config, ...string) error

type Hook interface {
	Type() HookType
	ID() string
	Data() map[string]any
	SetExtra(map[string]any)
	Stage() HookStage
	Status() ExecStatus
	Execute() error
	PayloadMap() map[string]any
	ExecuteOnDone(error) (OnFailType, error)
}

type Hooks []Hook

type HookMap struct {
	Start []any `json:"start,omitempty" yaml:"start,omitempty"`
	End   []any `json:"end,omitempty" yaml:"end,omitempty"`
	Pre   []any `json:"pre,omitempty" yaml:"pre,omitempty"`
	Post  []any `json:"post,omitempty" yaml:"post,omitempty"`
}

type ParseOptions struct {
	stage HookStage
	kind  HookKind
	index int
	state RuntimeState
	md5   string
}

type HookStage string

const (
	HookStagePre   HookStage = "pre"
	HookStagePost  HookStage = "post"
	HookStageStart HookStage = "start"
	HookStageEnd   HookStage = "end"
)

var ParseHook = func(any, ParseOptions) (Hook, error) {
	return nil, g.Error("please use the official sling-cli release for using hooks and pipelines")
}

func (hs Hooks) Execute() (err error) {
	for _, hook := range hs {
		if !g.In(hook.Type(), "log") {
			g.Debug(`executing hook "%s" (type: %s)`, hook.ID(), hook.Type())
		}

		hookErr := hook.Execute()
		_, err = hook.ExecuteOnDone(hookErr)

		if err != nil {
			return g.Error(err, "error executing hook")
		}
	}
	return nil
}
