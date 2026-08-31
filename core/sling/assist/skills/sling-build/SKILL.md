---
name: sling-build
description: >
  Build SQL models with Sling Build — `sling build`, `sling_build.yml`, model selectors, materialization modes, incremental ranges, and dbt compatibility. Use when asked to author or run SQL models / transformations.
---

# Sling Build — SQL Models with Dependency Resolution

`sling build` is Sling's lightweight SQL model builder with Jinja-style templating, dependency resolution from `ref()` calls, and incremental materializations. The single canonical reference is [https://docs.slingdata.io/llms.txt](https://docs.slingdata.io/llms.txt) — load that first if you need anything beyond the quick reference below.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took. Confirm first only for destructive resolutions (for example a mode that drops or rewrites existing tables).

| Decision | Resolve by | Default |
|----------|------------|---------|
| model name | request, existing SQL | `stg_<source>__<entity>` / `fct_<entity>` by layer |
| target conn | request, `sling_build.yml`, `connection.list` | `target:` in `sling_build.yml` |
| layer / folder | request, name prefix, STRUCTURE.md | staging |
| materialization | request, layer table | `view` (staging/intermediate); `full-refresh` (marts) |
| incremental key | request, columns | none unless mode is incremental |
| upstreams | request, `ref()` / `src()`, discover | raw/staging tables for that entity |

When you update a model, read the files first. Preserve incremental-state semantics unless asked otherwise.

## Project layout

A build project is any directory containing `sling_build.yml`. Models are `.sql` files — usually grouped under schema folders or, with `dbt_project: true`, under `models/` and `seeds/`.

In a full Sling project, put this tree under `models/` (see [PROJECT.md](../sling/PROJECT.md) in the `sling` skill). First folder = schema. Naming rules: [STRUCTURE.md](STRUCTURE.md).

```
models/
  sling_build.yml
  staging/          # views: stg_<source>__<entity>
    stg_stripe__users.sql
    stg_stripe__orders.sql
  intermediate/     # views: int_<entity>__<verb>
    int_orders__joined.sql
  marts/            # tables: fct_* / dim_*
    fct_orders.sql
```

| Layer | Folder = Schema | Naming | Materialization |
|-------|-----------------|--------|-----------------|
| raw | `raw_<source>` | source-native | replication target — not a build folder |
| staging | `staging` | `stg_<source>__<entity>` | view |
| intermediate | `intermediate` | `int_<entity>__<verb>` | view |
| marts | `marts` | `fct_*`, `dim_*` | table / incremental |

Staging selects from raw via `src()`. Marts use `ref()` to staging or intermediate only. Marts never touch raw.

## `src()` and `ref()`

`src()` is a passthrough. It does **not** add a `raw_` prefix.

```sql
-- two args: schema, table
select * from {{ src('raw_stripe', 'invoice') }}
-- result: select * from raw_stripe.invoice

-- one arg: schema.table
select * from {{ src('raw_stripe.invoice') }}
-- result: select * from raw_stripe.invoice
```

`source()` is an alias of `src()`.

`ref('name')` resolves a model or seed in this project to its full table name. An unknown name is a compile error.

```sql
select * from {{ ref('stg_orders') }}
-- result: select * from staging.stg_orders
```

Duplicate model names in the project are a hard error at load time.

## Front-matter

Put YAML in a `/** **/` block. It must be the first content in the file. Leading whitespace is allowed. Comments before the block are not. The engine treats the rest of the file as SQL.

You can also use `/* { ... } */` or `-- { ... }` with a brace object.

Front-matter does not rename the model. The model name is the file stem.

## sling_build.yml essentials

```yaml
target: MY_SNOWFLAKE
defaults:
  mode: full-refresh
dev:
  schema: dev_$USER
  target: MY_SNOWFLAKE_DEV
dbt_project: false   # set true for models/+seeds/ layout
```

## Model materialization modes

Canonical modes: `view`, `full-refresh`, `incremental`, `append`. Prefer these names.

- `view` — `CREATE OR REPLACE VIEW`.
- `full-refresh` — drop and rebuild every run (default). `table` is an accepted alias.
- `incremental` — append/merge based on `update_key` + `unique_key`.
- `append` — insert only. `snapshot` is a deprecated alias.
- `truncate` — truncate target then reload.

Per-model overrides go in a YAML front-matter block at the top of the SQL. Jinja `{%- config(...) -%}` is dbt-compat only — prefer YAML:

```sql
/**
mode: incremental
update_key: updated_at
unique_key: [id]
range:
  start: '2020-01-01'
  advance: 1mo
  lookback: 2d
**/

select * from {{ ref("stg_orders") }} where {{ incremental_where_cond() }}
```

Front-matter `range:` (`start`, `advance`, `lookback`) is the durable backfill window. Pair it with `update_key` and `unique_key`. Do not reverse-engineer the binary for this.

Read raw table columns before you write SQL (`sling conns discover CONN --columns` or MCP `database.get_columns`). Do not guess column names or types.

Never reference a SELECT alias inside the same query's JOIN or WHERE. Use the source column (`c_nationkey`, not `c.nation_key`).

## Common workflows

When the warehouse stores an authoritative total (for example `o_totalprice`), aggregate that column. Recompute from components only when asked.

| Goal                                | Command                                              |
|-------------------------------------|------------------------------------------------------|
| Compile (no execute)                | `sling build --compile`                              |
| Run a subset                        | `sling build -s stg_users,fct_orders`                |
| Run a model + downstream            | `sling build -s +stg_users`                          |
| Run models by tag                   | `sling build -s tag:daily`                           |
| List selected models (no execute)   | `sling build -s tag:daily --list`                    |
| Force full-refresh on incrementals  | `sling build --full-refresh`                         |
| Dev mode override                   | `sling build --schema dev_alice`                     |
| Force prod mode                     | `sling build --prod`                                 |
| Backfill range                      | `sling build -s fct_orders -r 2024-01-01,2024-12-31` |
| Pass variables                      | `sling build --vars '{start_date: 2024-01-01}'`      |
| Parallel model runs                 | `sling build --threads 4`                            |
| Skip seed loading                   | `sling build --no-seeds`                             |
| Stop on first failure               | `sling build --fail-fast`                            |
| Sub-project discovery               | `sling build -R` (scans immediate subdirectories)    |
| Create a project                    | `sling init` (writes `models/sling_build.yml`) |

Notes:
- Seeds (CSV files, in `seeds/` with `dbt_project: true`) load before models unless `--no-seeds` is passed.
- `--range` is a one-shot CLI backfill (`start,end[,step]`, e.g. `2024-01-01,2024-12-31,1mo`). Prefer front-matter `range:` for a durable window.
- `--schema` forces dev mode and cannot combine with `--prod`.
- One project root. Do not leave nested scratch projects (`probe/`, `.probe/`). Parent compile skips a subdirectory that has its own `sling_build.yml` unless you pass `-R`.
- Resolve the Gather first checklist before you write a new model. When you edit an incremental model, preserve saved-state semantics unless asked otherwise.

Definition of done: `sling build . --target X` (or `--test`) completes with 0 failures. Compile alone does not execute SQL, so it does not prove correctness.

## Use the MCP tools to validate

- `database.get_columns` / `database.get_schemata` — read columns before you write SQL.
- `database.query` — sanity-check the SQL against the target before committing.
- `connection.test` — make sure `target:` resolves before the first `sling build`.

When the user asks for a new model, prefer to:
1. Inspect upstream columns with `database.get_columns` (or `sling conns discover CONN --columns`).
2. Draft the SQL, including YAML frontmatter when materialization isn't `full-refresh`.
3. Run `sling build --compile -s <model>` to confirm the SQL renders cleanly.
4. Run `sling build . --target <conn>` (or `-s <model>`) until it completes with 0 failures.
5. Remove scratch files and nested probe projects before you finish.

## Full Documentation

Append `.md` paths to https://docs.slingdata.io to fetch raw markdown. Full index: https://docs.slingdata.io/llms.txt

| Topic | Doc |
|-------|-----|
| Build overview | https://docs.slingdata.io/concepts/build.md |
| Structure & naming | https://docs.slingdata.io/concepts/build/structure.md |
| Models | https://docs.slingdata.io/concepts/build/models.md |
| Modes | https://docs.slingdata.io/concepts/build/modes.md |
| Incremental | https://docs.slingdata.io/concepts/build/incremental.md |
| Selectors | https://docs.slingdata.io/concepts/build/selectors.md |
| Seeds | https://docs.slingdata.io/concepts/build/seeds.md |
| Macros | https://docs.slingdata.io/concepts/build/macros.md |
| Dev & Prod | https://docs.slingdata.io/concepts/build/dev-prod.md |
| Project layout (four layers) | https://docs.slingdata.io/concepts/project-structure.md |
