package sling

import "github.com/flarco/g"

type HookType string

type Hook interface {
	Type() HookType
	ID() string
	Execute(*TaskExecution) error
	ExecuteOnDone(error, *TaskExecution) error
}

type Hooks []any

func (hks Hooks) Parse(stage string, te *TaskExecution) (hooks []Hook, err error) {
	for i, hook := range hks {
		parsedHook, err := ParseHook(hook, te, g.F("%s-%02d", stage, i+1))
		if err != nil {
			return nil, g.Error(err, "error making %s-hook", stage)
		} else if parsedHook != nil {
			hooks = append(hooks, parsedHook)
		}
	}
	return hooks, nil
}

var ParseHook = func(any, *TaskExecution, string) (Hook, error) { return nil, nil }
