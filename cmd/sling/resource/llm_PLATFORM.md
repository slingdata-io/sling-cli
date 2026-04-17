# Sling Platform Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [Authentication](#2-authentication)
3. [`sling project init`](#3-sling-project-init)
4. [`sling project status`](#4-sling-project-status)
5. [`sling project sync`](#5-sling-project-sync)
6. [`sling project jobs list`](#6-sling-project-jobs-list)
7. [`sling project jobs status`](#7-sling-project-jobs-status)
8. [`sling project jobs trigger`](#8-sling-project-jobs-trigger)
9. [`sling project jobs get`](#9-sling-project-jobs-get)
10. [`sling project jobs save`](#10-sling-project-jobs-save)
11. [`sling project jobs delete`](#11-sling-project-jobs-delete)
12. [`sling project execs`](#12-sling-project-execs)
13. [`sling project files`](#13-sling-project-files)
14. [`sling project connections`](#14-sling-project-connections)
15. [Job Payload Reference](#15-job-payload-reference)
16. [Common Workflows](#16-common-workflows)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Introduction and Overview

### What is the Sling Platform?

The Sling Platform (hosted at `api.slingdata.io`, UI at `platform.slingdata.io`) is the commercial control plane for Sling. It stores projects, project files, scheduled jobs, execution history, monitors, and agents. The `sling project ...` subcommands in the CLI let you interact with a platform project from the terminal and from scripts — without touching the UI.

### When to use the `sling project` commands

- You have a Sling Platform project and want to push local YAML file changes up (`sync`).
- You want to script job lifecycle management (create, read, update, trigger) from CI or local automation.
- You want to inspect execution history or current job state without opening the UI.
- You want to round-trip a job's config as JSON (edit in your editor, save back).

### Core concepts

- **Project**: A workspace on the platform containing project files (YAML replications/pipelines/monitors/queries) and jobs.
- **Project token**: A scoped API token (36-character UUID) that authenticates requests for one project. Created in `Settings > API Tokens` on the platform UI.
- **Project file**: A YAML/SQL file stored in the project (e.g. `replications/hackernews.yaml`). Synced from local disk via `sling project sync`.
- **Job**: A scheduled or triggerable definition pointing at a project file, with its own `schedules`, `streams`, `config`, `notification_settings`, etc. Job IDs have the prefix `job_`.
- **Execution**: One run of a job. Execution IDs have the prefix `exec_`.

### Relationship to the rest of the CLI

`sling project` commands are **only** for interacting with the hosted platform. They have nothing to do with running data movement locally — for that use `sling run ...`. The platform commands are thin REST clients over `https://api.slingdata.io` (or a self-hosted URL if `SLING_PLATFORM_HOST` is set).

---

## 2. Authentication

All `sling project ...` commands require a project token via environment variable:

```bash
export SLING_PROJECT_TOKEN=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

A common place to put this is `~/.sling/env.yaml` under the `variables:` block, or export it in your shell profile. The token scopes every request to one specific project — you do not need to pass a project ID separately.

### Base URL

By default the CLI talks to `https://api.slingdata.io`. Override with:
- `SLING_PLATFORM_HOST=https://your-self-hosted.example.com` → self-hosted

### Failure modes

- Missing token → "did not provide the SLING_PROJECT_TOKEN environment variable".
- Invalid token → "invalid project token".
- Expired/revoked token → HTTP 401 from the platform, surfaced as a warning and a fatal error.

---

## 3. `sling project init`

Create a `.sling.json` marker in the current directory so the CLI knows it is inside a project. Subsequent commands will be scoped to this directory tree.

```bash
sling project init
```

Effects:
- Writes `.sling.json` in the current directory with `{"id":"","paths":[]}`.
- If you later edit `paths` to a list of subfolders, only those are used as the project root for `sync`.
- Idempotent: if `.sling.json` already exists anywhere up the tree, prints a message and exits without overwriting.

The `id` field is populated when `SLING_PROJECT_TOKEN` is validated — you do not need to set it manually.

---

## 4. `sling project status`

Print a summary of the linked platform project: project ID, name, organization, owner, plus a key/value table of status details (counts of files, jobs, executions, etc.).

```bash
sling project status
```

Uses `GET /project/get?type=status_details`. Token required.

---

## 5. `sling project sync`

Diff local project files against the platform's stored copies and push any that are new or locally newer.

```bash
sling project sync           # prompts before pushing
sling project sync --force   # skip the confirmation prompt
sling project sync -f        # short form of --force
```

Flags:
- `-f, --force` — push without the Y/N prompt. Useful in CI.
- `-d, --debug` — verbose logs.

Behavior:
- Fetches the remote file list via `POST /project/file/list` and compares `updated` timestamps to local mod times.
- Files only on the remote are **not** deleted locally, and vice versa — sync is one-way push-only.
- Creating a Sling YAML file remotely (via the platform UI) will auto-create a default job for it. Creating it via sync + save is the recommended CLI path.

---

## 6. `sling project jobs list`

Print all jobs in the project as a table (default) or JSON.

```bash
sling project jobs list                                   # all jobs, table
sling project jobs list --type replication               # only replication jobs
sling project jobs list --file-name replications/x.yaml  # filter by file name
sling project jobs list --name "Nightly"                 # filter by job name
sling project jobs list -o json | jq '.[].id'            # JSON for scripting
```

Flags:
- `--type <t>` — filter by job type: `replication`, `pipeline`, `query`, `monitor`.
- `--file-name <f>` — filter by exact file name match.
- `--name <n>` — filter by exact job name.
- `-o, --output json|table` — output format (default: `table`).

Columns (table mode): `ID`, `Name`, `File Name`, `Type`, `Status`, `Active`, `Executed`, `Scheduled`.

---

## 7. `sling project jobs status`

Per-job overview matching the platform home page: one row per job with the latest execution's status. Jobs with no runs in the last 60 days still appear with `-` placeholders.

```bash
sling project jobs status
sling project jobs status --name hacker           # substring match on job name
sling project jobs status --id job_0vnx           # substring match on job id
sling project jobs status -o json
```

Flags:
- `--id <s>` — substring filter on `job_id`.
- `--name <s>` — case-insensitive substring filter on job name.
- `-o, --output json|table` — output format (default: `table`).

Columns (table): `Name`, `Type`, `Active`, `Last Status`, `Last Run`. `Last Run` is a relative duration (`3h ago`, `5m ago`, `-` if never).

JSON mode returns the raw per-job records with fields: `job_id`, `job_name`, `job_type`, `file_name`, `active`, `job_status`, `exec_id`, `exec_status`, `timestamp` (unix seconds of latest exec), plus `schedules`/`timezone`/`next_run_at` when set.

Backed by `GET /project/dashboard?name=home_dashboard_job_history`; merged with `POST /project/job/list` to surface jobs that have never run.

---

## 8. `sling project jobs trigger`

Kick off a job run. Optionally override which streams run and the mode.

```bash
sling project jobs trigger job_0vnx9sjkjzmd95nh
sling project jobs trigger job_0vnx9sjkjzmd95nh --wait
sling project jobs trigger job_0vnx9sjkjzmd95nh -w
sling project jobs trigger job_0vnx9sjkjzmd95nh --streams users,orders
sling project jobs trigger job_0vnx9sjkjzmd95nh --full-refresh
sling project jobs trigger job_0vnx9sjkjzmd95nh --streams u --full-refresh --wait
```

Flags:
- `-w, --wait` — poll execution status every 5 seconds until terminal (Success/Error/Stalled/Cancelled/Timed Out). Prints `rows`, `bytes`, and `duration` on completion. Exits non-zero on non-Success terminal status.
- `--streams <s1,s2,...>` — comma-separated list of streams to run. Overrides the job's configured streams for this run.
- `--full-refresh` — run with `mode: full-refresh`. Overrides the job's configured mode for this run.

The positional argument must begin with `job_` or the CLI rejects it as invalid. On start, prints `Started Job (exec_id=exec_...)`.

---

## 9. `sling project jobs get`

Fetch a single job's full definition as JSON to stdout.

```bash
sling project jobs get job_0vnx9sjkjzmd95nh
sling project jobs get job_0vnx9sjkjzmd95nh > /tmp/job.json
sling project jobs get job_0vnx9sjkjzmd95nh | jq '.config'
```

Output is pretty-printed JSON of the `Job` model. The shape is a round-trip — you can edit the JSON and pass it directly to `sling project jobs save --file`.

Uses `GET /project/job/get?job_id=<id>`. The positional argument must begin with `job_`.

---

## 10. `sling project jobs save`

Create a job (if `id` is empty/missing) or update an existing one (if `id` is present). A single command handles both.

```bash
# Create: omit id
sling project jobs save --payload '{"name":"nightly","type":"replication","file_name":"replications/nightly.yaml","active":true,"schedules":["0 2 * * *"],"timezone":"UTC","config":{}}'

# Update from edited file
sling project jobs get job_abc123 > /tmp/job.json
# ... edit /tmp/job.json ...
sling project jobs save --file /tmp/job.json

# Short flags
sling project jobs save -p '{"name":"test","type":"replication","file_name":"r.yaml","config":{}}'
sling project jobs save -f /tmp/job.json

# Pipe from get (stdin via `-`)
sling project jobs get job_abc123 | sling project jobs save --file -
```

Flags (exactly one required):
- `-p, --payload <json>` — inline JSON object for the `Job`.
- `-f, --file <path>` — path to a JSON file. Use `-` to read from stdin.

Providing both or neither is an error.

### Required fields on create

At minimum: `name`, `type`, `file_name`, `config` (can be empty `{}`). `type` must be one of `replication`, `pipeline`, `query`, `monitor`.

### Response

Prints the saved `Job` JSON (including the server-assigned `id` for new jobs and the recomputed `scheduled` field if `schedules` changed).

### Plan-based restrictions

The platform enforces plan limits and will reject the save with an error:
- Free plan: cannot set `active: true`, cannot set `config.threads > 1`, cannot enable `config.retry`.
- Monitor jobs (`type: monitor`) require the Advanced plan.

### Uses

`POST /project/job/save` with body `{"job": {...}}`. The server calls `NewPrefixID("job")` to generate an ID when missing, then gorm `Save` upserts by primary key.

---

## 11. `sling project jobs delete`

Delete a job by ID.

```bash
sling project jobs delete job_abc123              # prompts Y/N
sling project jobs delete job_abc123 -f            # no prompt
sling project jobs delete job_abc123 --force       # no prompt
```

Flags:
- `-f, --force` — skip the Y/N confirmation prompt. Useful in CI.

**Admin-only on the server.** The server enforces admin-user permissions for deleting jobs; a standard project token not tied to an admin user will receive a 403. The CLI surfaces whatever error the server returns — it does not pre-check.

Uses `POST /project/job/delete` with body `{"job_id": "<id>"}`.

---

## 12. `sling project execs`

Manage and inspect executions.

### `execs list`

List recent executions with a job-name column, derived status, and optional time range. Newest first.

```bash
sling project execs list                                    # default: 10 rows, table
sling project execs list --status error --limit 5          # filter by status
sling project execs list --job-id job_abc123 -o json       # scope to one job, JSON
sling project execs list --since 24h                        # last 24 hours
sling project execs list --since 7d --status success
sling project execs list --since 2026-04-10 --until 2026-04-17
```

Flags:
- `--status <s>` — filter by status (`success|error|warning|running|queued|cancelled|stalled|skipped`). Pass-through to the server.
- `--job-id <id>` — filter to a specific job.
- `--since <t>` — start of time range. Accepts RFC3339, `YYYY-MM-DD`, or a duration (`24h`, `7d`, `90m`). Defaults to 7 days before `--until`.
- `--until <t>` — end of time range. Accepts RFC3339 or `YYYY-MM-DD`. Defaults to now.
- `--limit <n>` — max records (default 10, server-capped at 100).
- `-o, --output json|table` — output format (default `table`).

Columns (table): `Name`, `Exec ID`, `Job ID`, `Start Time`, `Rows`, `Bytes`, `Status`. Status is derived from per-state counts (Running > Queued > Stalled > Cancelled > Created > Success > Error). When the source run wasn't triggered from a saved job, `Name` falls back to the replication name or `cli-<md5>` for ad-hoc CLI runs.

### `execs status <exec-id>`

Fetch the full server-side status record for one execution as pretty-printed JSON.

```bash
sling project execs status exec_abc123
sling project execs status exec_abc123 | jq '.status_map'
```

Uses `GET /execution/list?filters=%7B%22exec_id%22%3A%22...%22%7D`.

### `execs cancel <exec-id>`

Cancel a running execution.

```bash
sling project execs cancel exec_abc123
```

Uses `POST /execution/cancel` with body `{"exec_id": "<id>"}`.

### `execs log <exec-id>`

Print the full log output captured during an execution. Each task/step's log is prefixed with a `=== <name> (status=...) ===` banner. The log contains ANSI color codes by default; use `--no-color` to strip them for grep/diff/save-to-file.

```bash
sling project execs log 3CUJmxllsG8YC1kDYGzU6jYjS5X                       # all streams, colored
sling project execs log 3CUJmxllsG8YC1kDYGzU6jYjS5X --no-color             # plain text
sling project execs log 3CUJmxllsG8YC1kDYGzU6jYjS5X --task users           # only the "users" stream
sling project execs log 3CUJmxllsG8YC1kDYGzU6jYjS5X --status error         # only errored tasks/steps
sling project execs log 3CUJmxllsG8YC1kDYGzU6jYjS5X -o json > tasks.json   # raw task records
```

Flags:
- `--task <s>` — filter to a single task/stream name (replication) or step id (pipeline). Replication names are matched client-side against `stream_name`; pipeline step ids are matched server-side.
- `--status <s>` — filter tasks/steps by status (`success|error|warning|running|...`). Server-side filter.
- `--no-color` — strip ANSI color escape sequences from the log output.
- `--type <t>` — force `replication` or `pipeline` routing. Auto-detected by default via the execution's `job_type`.
- `-o, --output text|json` — `text` (default) prints human-readable banners + log bodies; `json` emits the raw task/step records array (includes config, rows/bytes stats, timestamps, output).

Backed by `POST /execution/replication-tasks` for replication/query/monitor executions and `POST /execution/pipeline-steps` for pipelines. The log text lives in the `output` field of each task/step.

---

## 13. `sling project files`

Manage project files stored on the platform. For bulk push from local disk, prefer `sling project sync`.

### `files list`

```bash
sling project files list
sling project files list -o json | jq '.[].name'
```

Columns (table): `Name`, `Size`, `Updated`. `-o json` emits the full file records.

### `files get <name>`

Print a project file's body to stdout. Output is the raw file body, **not** JSON-wrapped, so shell redirection works:

```bash
sling project files get replications/hackernews.yaml
sling project files get replications/hackernews.yaml > /tmp/hackernews.yaml
```

### `files save <name>`

Create or update a project file. For Sling job files (replication/pipeline/monitor/query), the server parses and validates the content before saving, and auto-creates a default job for new files (the response includes `default_job_id`).

```bash
# inline body
sling project files save replications/foo.yaml --body 'source: PG
target: SF
streams:
  public.users: {}
'

# from a local file
sling project files save replications/foo.yaml -f ./local_repl.yaml

# from stdin
cat ./local.yaml | sling project files save replications/foo.yaml -f -

# create an empty directory marker
sling project files save replications/archive/ --dir

# JSON response (useful for default_job_id)
sling project files save replications/foo.yaml -f ./local.yaml -o json | jq .default_job_id
```

Flags:
- `-f, --file <path>` — read body from a local path (`-` means stdin).
- `-b, --body <text>` — inline body. Mutually exclusive with `--file`.
- `--dir` — create a directory marker instead of a file (body is ignored).
- `-o, --output json|table` — `json` prints the full file record; default prints `Saved file <name> (<n> bytes)` plus any `default_job_id` returned.

Backed by `POST /project/file/save` with `{"file": {"name", "body", "is_dir"}}`.

### `files delete <name>`

Hard-delete a file on the platform.

```bash
sling project files delete replications/old.yaml        # prompts Y/N
sling project files delete replications/old.yaml -f      # no prompt
```

Flags:
- `-f, --force` — skip the Y/N prompt.

Note: the server performs a hard delete immediately; the CLI prompt is the only safety net.

### `files rename <old-name> <new-name>`

Rename a file on the platform. The server also cascades the rename to any jobs referencing the old path.

```bash
sling project files rename replications/old.yaml replications/new.yaml
```

---

## 14. `sling project connections`

Inspect connections configured for the project on the platform (from `env.yaml` and/or `.env.sling`).

### `connections list`

```bash
sling project connections list
sling project connections list -o json
```

Columns (table): `Name`, `Type`, `Kind`, `Source`.

### `connections test <name>`

Test that a project connection is valid. Requires a running agent for most connection types (the request is forwarded to the connection's agent).

```bash
sling project connections test MY_POSTGRES
```

On success: prints `Connection MY_POSTGRES: valid`. On failure: exits non-zero with the server/agent's error message.

**Out of scope**: `connections create`/`update`/`delete` — the platform does not expose those via project tokens today.

---

## 15. Job Payload Reference

The full `Job` JSON shape accepted by `save` and returned by `get`:

```json
{
  "id": "job_0vnx9sjkjzmd95nh",
  "project_id": "proj_n18xna6rr1ixvfi7",
  "file_name": "replications/hackernews.yaml",

  "name": "Hacker News [incremental]",
  "type": "replication",
  "description": "",
  "status": "OK",
  "active": true,
  "agent_id": null,

  "streams": [],
  "tags": [],
  "schedules": ["0 * * * *"],
  "timezone": "UTC",
  "group": null,

  "config": {
    "mode": "incremental",
    "limit": null,
    "offset": null,
    "primary_key": null,
    "incremental_key": "id",
    "incremental_policy": "default",
    "incremental_interval": null,
    "range": null,
    "threads": 4,
    "variables": [
      {"key": "SLING_AGENT_ID", "val": "agent_xyz"}
    ],
    "policy": "queue",
    "retry": false,
    "retries": 2,
    "timeout": 3600,
    "binary_channel": "stable"
  },

  "notification_settings": {
    "on_success": false,
    "on_failure": true,
    "on_warning": true,
    "on_linger": false,
    "on_empty": false,
    "on_schema_change": false,
    "on_freshness": false,
    "on_anomaly": false,
    "emails": ["ops@example.com"],
    "include_log": true
  },

  "executed": "2026-04-17T13:59:37.618971Z",
  "scheduled": "2026-04-18T00:00:00Z",
  "created": "2026-04-10T09:00:00Z",
  "updated": "2026-04-17T13:59:37.619709Z"
}
```

### Field-by-field

| Field | Type | Notes |
|---|---|---|
| `id` | string | `job_`-prefixed. Omit or empty → create; present → update. |
| `project_id` | string | Set by server from token; you do not need to provide. |
| `file_name` | string | Path to the YAML file in the project (e.g. `replications/foo.yaml`). Must exist via `sync` first. |
| `name` | string | Human-readable job name. |
| `type` | enum | `replication` \| `pipeline` \| `query` \| `monitor`. Required. |
| `description` | string | Optional. |
| `status` | string | `OK` \| `Error` \| `Failing`. Defaults to `OK` on create. |
| `active` | bool | If true, runs on `schedules`. Requires paid plan. |
| `agent_id` | string? | Resolved from `config.variables["SLING_AGENT_ID"]`. `null` means any agent. |
| `streams` | string[] | Subset of streams to run. Empty = all streams in file. |
| `tags` | string[] | Free-form tags. |
| `schedules` | string[] | Cron expressions (any valid 5-field cron). Can be multiple. |
| `timezone` | string | IANA tz, e.g. `America/New_York`. Applied to all `schedules`. |
| `group` | string? | Concurrency group — jobs sharing a group run serially. |
| `config` | object | See below. |
| `config.mode` | string | `full-refresh` \| `incremental` \| `truncate` \| `snapshot` \| `backfill`. |
| `config.threads` | int | Parallelism. `>1` requires paid plan. |
| `config.retries` | int | Retry count on failure. |
| `config.timeout` | float | Seconds. |
| `config.policy` | enum | `cancel` \| `queue` \| `skip` — what to do when a new run starts while one is running. |
| `config.variables` | `[{key,val}]` | Runtime env vars. `SLING_AGENT_ID` here pins the job to an agent. |
| `config.binary_channel` | enum | `stable` \| `development`. |
| `notification_settings.*` | bool/string[] | Per-event email triggers. `emails` is a list of recipients. |
| `executed`, `scheduled`, `created`, `updated` | datetime | Server-maintained — any values you send are ignored. |

---

## 16. Common Workflows

### Bootstrap a new project from scratch

```bash
export SLING_PROJECT_TOKEN=...
cd ~/my-project
sling project init
# ... write replications/nightly.yaml ...
sling project sync -f
sling project jobs save --payload '{"name":"nightly","type":"replication","file_name":"replications/nightly.yaml","active":true,"schedules":["0 2 * * *"],"timezone":"UTC","config":{"mode":"incremental","threads":4}}'
```

### Edit a job's schedule

```bash
sling project jobs get job_abc123 > /tmp/j.json
# edit schedules[] in /tmp/j.json
sling project jobs save --file /tmp/j.json
```

### Toggle `active` via jq

```bash
sling project jobs get job_abc123 \
  | jq '.active = false' \
  | sling project jobs save --file -
```

### Promote config from one job to another

```bash
sling project jobs get job_src > /tmp/src.json
sling project jobs get job_dst \
  | jq --slurpfile src /tmp/src.json '.config = $src[0].config | .notification_settings = $src[0].notification_settings' \
  | sling project jobs save --file -
```

### Trigger + wait in CI

```bash
sling project jobs trigger job_abc123 --wait
# exits non-zero on non-Success, so CI fails the step
```

---

## 17. Troubleshooting

### "did not provide the SLING_PROJECT_TOKEN environment variable"

Set `SLING_PROJECT_TOKEN` in your shell or `~/.sling/env.yaml`. The token is a 36-character UUID from `Settings > API Tokens` on the platform UI.

### "invalid project token"

Token exists but the platform rejected it. Usually revoked, expired, or from a different environment (prod token against `ENV=staging` etc.).

### `save` fails with "invalid job type"

`type` is missing or not one of `replication`/`pipeline`/`query`/`monitor`. Required on create.

### `save` fails with "please upgrade your plan ..."

The project is on the Free plan and you are setting `active: true`, `config.threads > 1`, or `config.retry: true`. Upgrade or relax the config.

### `save` for a monitor job fails with "monitors require an Advanced plan"

Exactly what it says — Advanced plan required for `type: monitor`.

### `get` or `save` returns "error compiling next scheduled. Invalid expression?"

One of the entries in `schedules` is not a valid cron expression. Use 5-field cron (`minute hour day-of-month month day-of-week`).

### `trigger <id>` says "Did not provide job-id"

Positional argument must start with `job_`. If you passed an exec ID or mistyped the prefix, the CLI will not accept it.

### `save --file /tmp/x.json` says "invalid JSON payload"

The file is empty or malformed. A common cause: you tried to redirect `get` output into the file but `get` failed (e.g. token invalid), leaving a zero-byte file. Confirm with `wc -c /tmp/x.json` and re-run `get` with errors surfaced.

### Self-hosted platform

Set `SLING_PLATFORM_HOST=https://your-host.example.com` before any command. The token is validated against that host's `project/token/get` endpoint instead of `licensing.slingdata.io`.
