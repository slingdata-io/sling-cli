# Assist eval suite

Cases live in `cases/e.*/case.yaml`. The harness is `go test ./tests/evals`.

## Case hygiene

Every new case must:

1. Use the **canonical schema**: required `tier` (`smoke` | `core` | `deep`) and `gating`, explicit `connections` when a grader or fixture touches a live connection, `tags` only for `--tags` selection, `fixtures` as registry names.
2. State the user ask as a ticket (intention text).
3. Ship at least one **outcome grader** (`query`, `rows_equal`, `tests_pass`, `outcome`, or a live `sling … test/run`) unless tagged `negative`.
4. Ship at least one **mutant** the required graders reject.

Unknown YAML keys fail load. `TestEvalCasesValid` lints the tree.

Do not put `smoke` or `flaky` in `tags`. Use `tier` and `gating`.

Every `tier: smoke` case must list `noskills` in `arms`.

## Tiers

| Tier | Default use |
|---|---|
| smoke | PR: mock all + claude on smoke |
| core | Nightly with smoke |
| deep | Weekly, including Lane B real APIs |

`--tier smoke` selects only smoke. Thresholds apply to **gating** cases only. A baseline case id missing from the current run is **removed**, not failed.

## Fixtures

Named datasets in `fixtures/registry.yaml`. `--reset-fixtures` drops `eval_*` schemas and the local TPC-H DuckDB file.

`e.48` (CDC outcome) is **not shipped**. It needs a `wal_level=logical` Postgres fixture.

## Arms

| Arm | Binary | Auth | Notes |
|---|---|---|---|
| mock | (none) | — | Offline invariants (passable + mutants). Always available. |
| claude | `claude` | `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN` / `~/.claude` | Host HOME kept for keychain. |
| grok | `grok` | `XAI_API_KEY` / `~/.grok/auth.json` | Sandbox HOME. |
| opencode2 | `opencode2` | provider API key (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `XAI_API_KEY`) or `~/.local/share/opencode/auth.json` | OpenCode v2. Sandbox HOME + XDG pins + `--standalone`. Default model `opencode/x-preview-f-free` (seeded `opencode.json`). **No auto-download** — install the beta yourself (`npm i -g opencode2` or the documented installer). Override the binary with `OPENCODE2_PATH`. Override the seeded model with `EVAL_OPENCODE2_MODEL`. |
| codex | `codex` | `OPENAI_API_KEY` / `CODEX_API_KEY` / `~/.codex/auth.json` | Codex CLI (`codex exec`). Sandbox `CODEX_HOME`. Uses the host default model from `~/.codex/config.toml` (whitelist-copied `model` / `model_provider` / `model_providers`). MCP: `[mcp_servers.sling] default_tools_approval_mode = "approve"` so tools run under `approval_policy = "never"`. Eval instructions are prepended to the exec prompt (`$CODEX_HOME/AGENTS.md` is not a reliable channel for `codex exec`). **No auto-download** — install Codex yourself. Override the binary with `CODEX_PATH`. Override the seeded model with `EVAL_CODEX_MODEL`. |
| noskills | `claude` | same as claude | Skills-delta control. Smoke cases must list it. |

Missing binary or login skips that arm; the suite stays green.

## Pro token

`api_spec test` is a Pro feature. Set `SLING_CLI_TOKEN` in the host env
before a live run. The runner passes it to each arm's MCP server: codex via
`[mcp_servers.sling.env]` in the seeded `config.toml`, claude via `env` in
`.mcp.json`, grok via `env` in `.grok/config.toml`, opencode2 via
`environment` in `.opencode/opencode.json`. Without the token, the suite
still runs (one warning at codex arm setup), but agent-side live api tests
fail at the license gate and the trial is marked `InfraError`. Token-bearing
config files (`.mcp.json`) are never persisted into `results/`.

## Run

```bash
cd cmd/sling && go build .
go test ./tests/evals -count=1 -timeout 30m
go test ./tests/evals -count=1 -timeout 45m -run TestEvalAssist -- --arms mock
go test ./tests/evals -count=1 -timeout 45m -run TestEvalAssist -- --arms opencode2 --tier smoke --trials 1
go test ./tests/evals -count=1 -timeout 45m -run TestEvalAssist -- --arms codex --tier smoke --trials 1
```

Parallel runs lose trials to scheduler contention. After a parallel run
(`--parallel > 1`), the suite re-runs each failed live trial once,
sequentially, and the retry row replaces the parallel one (`retried: true`
in the JSONL). Disable with `--retry-failed=false`.
