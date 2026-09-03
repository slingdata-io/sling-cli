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

See [Structure decisions](#structure-decisions) below for folder, split, layer, seed, and macro decisions.

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
* `schedules:` fire on the platform once the folder is linked. A local run is always manual.
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
| Zero connections | Scaffold connections. Load [CONNECTIONS.md](CONNECTIONS.md). Do not ask for secrets in chat. |
| No project (no `replications/`, `models/`, or `pipelines/`) | Run `sling init` to scaffold the canonical layout above. |
| Project present | Lint, then `sling build compile models`, then `sling build run`. |

Always end with the next step on this ladder. Do not skip the probe.

## Structure decisions

Use this guide when the user asks where a file goes, whether to split a model, or when to add a seed or macro.

The canonical tree is above. Folder → table mapping is in [../sling-build/STRUCTURE.md](../sling-build/STRUCTURE.md).

### Folder

| Put here | When |
|----------|------|
| `replications/<source>.yaml` | One YAML per source system. Target `raw_<source>.*` only. |
| `models/staging/` | One model per raw table. No joins. |
| `models/intermediate/` | Joins or business logic that marts must not own. |
| `models/marts/` | Facts and dimensions that BI reads. |
| `models/seeds/` | Static CSV/JSON/Parquet lookups. |
| `pipelines/` | Orchestration. Daily spine lives here. |
| `specs/` | Custom API specs. Official specs use `spec: stripe` on the connection. |
| `~/.sling/env.yaml` | All connections. Never in the project. |

Do not add a `raw/` model folder. Raw tables come from replications, not from `sling build`.

Split a replication YAML when streams, modes, or schedules differ by source. Do not put two sources in one file.

### Split

Keep **one model per file**. Split when:

* A staging model covers more than one raw table — that is a join. Move the join to intermediate.
* A mart query mixes two grains (order vs order line) — make two marts.
* A file grows past one SELECT with a clear name — extract the shared logic to intermediate or a macro.

Do not split only to match a BI folder. Nested folders under `marts/` do not change the table name (`marts/core/fct_orders.sql` → `marts.fct_orders`). Model names are unique across the project.

### Layer

Promote down the list. Do not skip raw → marts.

| Move to | When |
|---------|------|
| staging | You rename, cast, or dedup a raw table. Still 1:1. |
| intermediate | You join, filter business logic, or union sources. Not for BI. |
| marts | A consumer (dashboard, reverse ETL) needs a stable contract. |

Keep a model in staging if it only casts and renames. Do not "help" with a join there.

Never select from `raw_*` in a mart. Insert an intermediate model if staging is not enough.

### Seed

Add a seed when the data is **static**, **small**, and **versioned in git**:

* Country codes, status maps, fiscal calendars
* Lookup tables that sources do not provide

Do not seed data that a replication can load. Do not seed large fact-like files.

Seed names share the global namespace with models. `country_codes.csv` collides with `country_codes.sql`.

### Macro promotion

Start with SQL in the model. Promote to a `.macros.sql` file when:

* Two or more models repeat the same expression
* The expression is a named business rule (cents → dollars, safe divide)

Scope:

| File | Available to |
|------|----------------|
| `models/utils.macros.sql` | All models |
| `models/staging/helpers.macros.sql` | `staging/` and children only |
| `models/marts/core/core_utils.macros.sql` | `marts/core/` and children only |

Do not put source-specific logic in a global macro. Keep Stripe-only helpers next to staging.

See https://docs.slingdata.io/concepts/build/macros.md for syntax.

## Full Documentation

| Topic | Doc |
|-------|-----|
| Project structure | https://docs.slingdata.io/concepts/project-structure.md |
| Zero to dashboard | https://docs.slingdata.io/examples/zero-to-dashboard.md |
| Build | https://docs.slingdata.io/concepts/build.md |
| Replications | https://docs.slingdata.io/concepts/replication.md |
| Pipelines | https://docs.slingdata.io/concepts/pipeline.md |
