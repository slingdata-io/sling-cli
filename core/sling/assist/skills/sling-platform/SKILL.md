---
name: sling-platform
description: >
  Sling Platform CLI — `sling project jobs|execs|files|connections`, creating and scheduling jobs, activating/deactivating, checking run history, investigating failed executions, and pushing file fixes back to the platform. Also indexes docs for monitors (freshness, schema drift, anomaly detection), agents, and self-hosting. Use when the user is managing or debugging anything hosted on Sling Platform.
---

# Sling Platform — Working with Hosted Projects

The Sling Platform stores replications, pipelines, jobs, and connections on a server. The CLI authenticates via `SLING_PROJECT_TOKEN` and exposes the platform through `sling project ...` subcommands. Full reference: [https://docs.slingdata.io/llms.txt](https://docs.slingdata.io/llms.txt).

## Authentication

Every platform command requires `SLING_PROJECT_TOKEN`. Project tokens are created in the platform UI (Settings → Tokens). Sling never reads the token from an AI agent's environment — only its own.

```bash
export SLING_PROJECT_TOKEN=...   # required
sling project status             # smoke test: project info + counts
```

## Subcommand map

| Subcommand                        | Purpose                                            |
|-----------------------------------|----------------------------------------------------|
| `sling init`                      | Scaffold a local project folder (no token needed)  |
| `sling project status`            | Platform project overview (needs a token)          |
| `sling project sync [--force]`    | Two-way file sync (local dir ↔ platform, by mtime) |
| `sling project jobs list`         | List jobs; `--type`, `--name`, `--file-name` filters |
| `sling project jobs status`       | Job states + last run; `--id`/`--name` substring match |
| `sling project jobs get <id>`     | Full job JSON (use as template for edits)          |
| `sling project jobs save`         | Create/update a job; `-f -` (stdin) or `-p '<json>'` |
| `sling project jobs trigger <id>` | Run now; `--wait`, `--streams s1,s2`, `--full-refresh` |
| `sling project jobs activate <id>` | Enable a job's schedules                          |
| `sling project jobs deactivate <id>` | Disable a job's schedules                       |
| `sling project jobs delete <id>`  | Remove a job (`--force` skips confirm)             |
| `sling project execs list`        | Run history; `--job-id`, `--job-name`, `--status`, `--since`, `--until`, `--limit` |
| `sling project execs status <id>` | Per-task / per-step state of one execution         |
| `sling project execs log <id>`    | Full log; `--task <stream/step>`, `--status error` to filter |
| `sling project execs cancel <id>` | Cancel a running exec                              |
| `sling project files list`        | List YAML files (replications, pipelines, specs)   |
| `sling project files get <name>`  | Print a file                                       |
| `sling project files save <name>` | Write a file: `-f <path>`, `-f -` (stdin), or `-b '<body>'` |
| `sling project files delete <name>` | Delete a file (`--force` skips confirm)          |
| `sling project files rename`      | Rename a file                                      |
| `sling project connections list`  | Project-scoped connections                         |
| `sling project connections test <name>` | Test a project connection                    |

Most list/get commands accept `-o json` — prefer it when parsing output.

## Job payload shape

`jobs save` sends the JSON straight to the platform's Job model. Key fields:

```json
{
  "name": "Users daily",
  "type": "replication",
  "file_name": "replications/users.yaml",
  "active": true,
  "schedules": ["0 4 * * *"],
  "timezone": "America/New_York",
  "streams": [],
  "config": {"mode": "incremental", "threads": 2, "retries": 1}
}
```

- **Omit `id` to create** — the server assigns a `job_...` ID. Include the existing `id` to update.
- `type`: `replication` | `pipeline` | `query` | `monitor`.
- `schedules` is an **array of cron expressions** (multiple allowed), paired with `timezone`. `active` controls whether they fire.
- `streams: []` means all streams; list names to restrict.
- `config` (optional): `mode`, `threads`, `retries`, `timeout`, `range`, `variables` (list of `{NAME: value}` maps — set `SLING_AGENT_ID` to pin an agent).
- Free-plan projects cannot save `active: true`, `threads > 1`, or retries — the server rejects with an upgrade message; monitors need the Advanced plan.

## Common workflows

### Create + schedule a job

```bash
cat job.json | sling project jobs save -f -    # no "id" → creates
sling project jobs get <returned_id>
sling project jobs trigger <returned_id> --wait   # verify it runs
```

### Activate / deactivate a job

```bash
sling project jobs deactivate <id>   # pause: schedules stop firing
sling project jobs activate <id>     # resume
```

For any other field change, note **`jobs save` is a full replace, not a patch** — sending only `{"id": ..., "name": "new"}` would wipe the job's schedules, streams, and config (the CLI warns and lists the fields that would be cleared). Always round-trip the full object:

```bash
sling project jobs get <id> > job.json
# edit job.json
cat job.json | sling project jobs save -f -
```

### Check history / last run

```bash
sling project jobs status --name users        # per-job: status, last executed, next scheduled
sling project execs list --job-name users --limit 1     # most recent run (name must match one job)
sling project execs list --job-id <id> --since 7d       # last week's history
sling project execs list --status error --since 24h     # all recent failures
```

### Investigate a failed exec

`sling assist --id <exec_id>` starts an assist session with this workflow pre-loaded when the exec exists on the platform.

1. `sling project execs list --status error --limit 5` — find the failure.
2. `sling project execs status <exec_id>` — which task/stream/step failed.
3. `sling project execs log <exec_id> --status error` — logs for failing tasks only (add `--task <name>` to zoom in; `--no-color` for clean text).
4. `sling project files get <file_name>` — pull the YAML to read/edit locally.
5. Validate the fix locally with MCP `replication validate` / `pipeline validate`, then run it: `sling run -r users.yaml` or `sling run -p pipeline.yaml`.
6. `cat fixed.yaml | sling project files save <file_name> -f -` — push back.
7. `sling project jobs trigger <job_id> --wait` — verify green.

### Edit a platform file in place

```bash
sling project files get replications/users.yaml > users.yaml
# edit locally, validate via MCP, then sling run -r users.yaml
cat users.yaml | sling project files save replications/users.yaml -f -
```

For bulk work, `sling init` + `sling project sync` keeps a local directory mirrored with the platform instead of per-file get/save.

## Monitors

Monitors observe data without moving it: schema drift, freshness, row counts, column statistics, and anomaly detection. They are YAML files (`connection:` + `objects:`) saved under the project's `monitors/` directory and scheduled as jobs with `type: monitor`. They require the Advanced plan.

| Monitor topic | Doc |
|---------------|-----|
| Overview + quick start | https://docs.slingdata.io/sling-platform/monitors.md |
| YAML structure | https://docs.slingdata.io/sling-platform/monitors/structure.md |
| Object metrics & freshness | https://docs.slingdata.io/sling-platform/monitors/object-metrics.md |
| Column metrics & validation | https://docs.slingdata.io/sling-platform/monitors/column-metrics.md |
| Anomaly detection | https://docs.slingdata.io/sling-platform/monitors/anomaly-detection.md |
| Schema changes | https://docs.slingdata.io/sling-platform/monitors/schema-changes.md |

## Agents & Self-Hosting (docs index)

Jobs execute on agents in the customer's infrastructure — data never flows through the control plane. For deployment questions, fetch:

- Agents (deploy, pin via `SLING_AGENT_ID`): https://docs.slingdata.io/sling-platform/agents.md
- Self-hosting the platform: https://docs.slingdata.io/sling-platform/self-hosting.md
- Architecture: https://docs.slingdata.io/sling-platform/architecture.md
- Platform HTTP API: https://docs.slingdata.io/sling-platform/api.md

## Tips

- The platform never executes against the user's local `env.yaml` — connections are project-scoped. "connection not found" means missing on the platform, not locally.
- `files get` returns the file with `${VAR}` references preserved — do not interpolate them.
- When unsure of job field names, `jobs get` a known-good job and edit that JSON rather than composing from scratch.
- `jobs trigger --streams a,b` and `--full-refresh` override the job's saved streams/mode for that one run only.
