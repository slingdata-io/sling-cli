package sling

import "github.com/flarco/g"

type HookType string

type Hook interface {
	Type() HookType
	ID() string
	Execute(*TaskExecution) error
	ExecuteOnDone(error, *TaskExecution) error
}

type Hooks struct {
	Pre  []any `json:"pre" yaml:"pre"`
	Post []any `json:"post" yaml:"post"`
}

func (h *Hooks) PreHooks(te *TaskExecution) (hooks []Hook, err error) {
	for i, hook := range h.Pre {
		preHook, err := ParseHook(hook, te, g.F("pre-%02d", i+1))
		if err != nil {
			return nil, g.Error(err, "error making pre-hook")
		} else if preHook != nil {
			hooks = append(hooks, preHook)
		}
	}
	return hooks, nil
}

func (h *Hooks) PostHooks(te *TaskExecution) (hooks []Hook, err error) {
	for i, hook := range h.Post {
		postHook, err := ParseHook(hook, te, g.F("post-%02d", i+1))
		if err != nil {
			return nil, g.Error(err, "error making pre-hook")
		} else if postHook != nil {
			hooks = append(hooks, postHook)
		}
	}
	return hooks, nil
}

var ParseHook = func(any, *TaskExecution, string) (Hook, error) { return nil, nil }
