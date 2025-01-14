package sling

import "github.com/flarco/g"

type HookType string

type Hook interface {
	Type() HookType
	ID() string
	Stage() HookStage
	Execute() error
	ExecuteOnDone(error) error
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
	index int
	state *RuntimeState
}

type HookStage string

const (
	HookStagePre   HookStage = "pre"
	HookStagePost  HookStage = "post"
	HookStageStart HookStage = "start"
	HookStageEnd   HookStage = "end"
)

var ParseHook = func(any, ParseOptions) (Hook, error) { return nil, nil }

func (hs Hooks) Execute() (err error) {
	for _, hook := range hs {
		if !g.In(hook.Type(), "log") {
			g.Debug(`executing hook "%s" (type: %s)`, hook.ID(), hook.Type())
		}

		hookErr := hook.Execute()
		err = hook.ExecuteOnDone(hookErr)

		if err != nil {
			return g.Error(err, "error executing hook")
		}
	}
	return nil
}
