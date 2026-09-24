package env

import (
	"os"
	"sync"
)

// liveChildProcs holds the running child processes. Some of them (e.g. the
// duckdb sidecar) are not in sling's process group, so the CLI must kill them
// itself on exit.
var liveChildProcs = &childProcs{procs: map[*os.Process]struct{}{}}

type childProcs struct {
	mu    sync.Mutex
	procs map[*os.Process]struct{}
}

func (cp *childProcs) add(p *os.Process) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.procs[p] = struct{}{}
}

func (cp *childProcs) remove(p *os.Process) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	delete(cp.procs, p)
}

func (cp *childProcs) killAll() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for p := range cp.procs {
		p.Kill() // an exited process returns an error, which is fine
		delete(cp.procs, p)
	}
}

// AddChildProc registers a child process, so KillChildProcs can stop it.
// Call RemoveChildProc when the process ends.
func AddChildProc(p *os.Process) {
	if p != nil {
		liveChildProcs.add(p)
	}
}

// RemoveChildProc unregisters a child process.
func RemoveChildProc(p *os.Process) {
	if p != nil {
		liveChildProcs.remove(p)
	}
}

// KillChildProcs kills all registered child processes. Call it before the process exits.
func KillChildProcs() {
	liveChildProcs.killAll()
}
