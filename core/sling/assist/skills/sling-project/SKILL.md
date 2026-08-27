---
name: sling-project
description: >
  Canonical Sling project layout — replications/, models/, pipelines/, four warehouse layers (raw, staging, intermediate, marts), global model names, and the replicate → build → check → log spine. Use when starting a project, choosing folders, or deciding the next setup step.
---

# Sling Project Layout

This skill is the project convention. Connections stay in `~/.sling/env.yaml`. Do not put secrets in the repo.

Run from the project root. Paths are root-relative. Do not use `../`.

Full docs: https://docs.slingdata.io/concepts/project-structure.md

## Canonical layout

```
my-project/
  replications/             # EL: sources → raw schemas
  models/                   # T: SQL models
    sling_build.yml
    staging/                # stg_<source>__<entity>
    intermediate/           # int_<entity>__<verb>
    marts/                  # fct_* / dim_*
    seeds/
  pipelines/                # replicate → build → check → log
    daily.yaml
  specs/                    # optional
  .gitignore
```

See [STRUCTURE.md](STRUCTURE.md) for folder, split, layer, seed, and macro decisions.

## Four layers

| Layer | Folder = Schema | Naming | Materialization | Rule |
|-------|-----------------|--------|-----------------|------|
| raw | `raw_<source>` | source-native | replication target | Replications write here. Nothing else does. |
| staging | `staging` | `stg_<source>__<entity>` | view | 1:1 with a raw table. Rename, cast, dedup. No joins. |
| intermediate | `intermediate` | `int_<entity>__<verb>` | view | Joins + logic. Never expose to consumers. |
| marts | `marts` | `fct_*`, `dim_*` | table / incremental | Public contract. Dashboards read only this. |

## Flow rules

1. Replications target `raw_*` schemas only.
2. Staging selects from raw only. Marts never touch raw.
3. Marts use `ref()` to staging or intermediate only.
4. BI grants apply to `marts` only. Put grants in mart post-hooks.
5. Model and seed names are **global**. Duplicates are a hard engine error (`validateUniqueNames` in `core/sling/build/project.go`). `stg_<source>__<entity>` prevents a collision when two sources share an entity name.
6. The `dev:` config applies to `sling build` only. Replications and pipelines always run against the real connections.
7. Pipeline spine uses real step types only: `replication`, `build`, `check`, `log`. Do not use `notify` — that type is not active.

## Naming rules

Folder → table mapping is in the `sling-build` skill ([STRUCTURE.md](../sling-build/STRUCTURE.md)) and https://docs.slingdata.io/concepts/build/structure.md#naming-rules. Do not re-derive it.

Project prefixes:

* `stg_<source>__<entity>` — staging. Two underscores after the source.
* `int_<entity>__<verb>` — intermediate.
* `fct_*` / `dim_*` — marts.

Why the source lives in the staging name: names are global. `stg_customers.sql` in two folders still collides. `stg_stripe__customers` and `stg_postgres__customers` do not.

## Pipeline spine

```yaml
steps:
  - type: log
    message: "Start daily run"

  - type: replication
    path: replications/stripe.yaml
    id: load_raw

  - type: build
    build: models
    prod: true
    id: transform

  - type: check
    check: state.transform.failed == 0
    failure_message: "Build reported failures"
    on_failure: abort

  - type: log
    message: "Done. build ok={state.transform.ok}"
```

Load `sling-replications`, `sling-build`, and `sling-pipelines` for YAML details.

## Jobs

`jobs:` in `sling_project.yml` names a file plus its overrides (`streams`, `mode`, `variables`).

* Run a job locally on demand: `sling run -j <key>` (or `sling run <key>`). No token is necessary.
* A local run is a rehearsal of the deployment. It applies the same overrides the platform will apply.
* `schedules:` fire on the platform after `sling project deploy`. A local run is always manual.
* Pipelines are the local driver for multi-step work. A job points at one replication or one pipeline.

```yaml
jobs:
  daily:
    file: pipelines/daily.yaml
    schedules: ["0 6 * * *"]   # platform only
  orders:
    file: replications/stripe.yaml
    streams: [charges]
    mode: incremental
```

## State-probe spec

Probe state before you change files. Then tell the user the next step. Every harness follows this ladder.

### Probe

1. `sling conns list` — named connections. Do **not** `cat` or read `env.yaml`.
2. `sling validate ./ -q` — validate project YAML. If the command is missing, inspect `replications/`, `models/sling_build.yml`, and `pipelines/` by hand.
3. Manifest check — do the canonical folders exist? Is there a `models/sling_build.yml`?

### Route (P3 ladder)

| Probe result | Next step |
|--------------|-----------|
| Zero connections | Scaffold connections. Load `sling-connections`. Do not ask for secrets in chat. |
| No project (no `replications/`, `models/`, or `pipelines/`) | Run `sling init` to scaffold the canonical layout above. |
| Project present | Lint, then `sling build models --compile`, then run. |

Always end with the next step on this ladder. Do not skip the probe.

## Full Documentation

| Topic | Doc |
|-------|-----|
| Project structure | https://docs.slingdata.io/concepts/project-structure.md |
| Zero to dashboard | https://docs.slingdata.io/examples/zero-to-dashboard.md |
| Build | https://docs.slingdata.io/concepts/build.md |
| Replications | https://docs.slingdata.io/concepts/replication.md |
| Pipelines | https://docs.slingdata.io/concepts/pipeline.md |
