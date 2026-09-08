# Build structure and naming

Companion to the main build guide. Folder layout for a full ELT repo: https://docs.slingdata.io/concepts/project-structure.md

A build project is a directory with `sling_build.yml` plus `.sql` models (and optional seeds). In the canonical project that directory is `models/`.

## Folder = schema, file = table

The 1st folder level is the schema name. The file name is the table name. Nested folders organize files only; they do not change the table name. Root-level files use the `public` schema.

| File Path | Schema | Name | Full Table Name |
|-----------|--------|------|-----------------|
| `raw.sql` | public | raw | `public.raw` |
| `staging/stg_orders.sql` | staging | stg_orders | `staging.stg_orders` |
| `staging/country_codes.csv` | staging | country_codes | `staging.country_codes` |
| `marts/core/dim_customers.sql` | marts | dim_customers | `marts.dim_customers` |
| `marts/core/fct_orders.sql` | marts | fct_orders | `marts.fct_orders` |
| `seeds/status_map.json` | seeds | status_map | `seeds.status_map` |

Docs: https://docs.slingdata.io/concepts/build/structure.md#naming-rules

## Names are global

Model names must be unique across the whole project. Two `events.sql` files in different folders are a hard engine error at load:

```
duplicate model name 'events': found in both 'analytics/plausible/events.sql' and 'analytics/stripe/events.sql'
```

`ref()`, selectors, and `@name` accept the model name or the prod table (`analytics.events`).

## Database (three-part names)

Folders never set the database. Set it with the `database` key, at the same three levels as `schema`:

```yaml
# sling_build.yml
target: MY_SNOWFLAKE
defaults:
  database: ANALYTICS_DB
dev:
  schema: dev_${USER}
  database: SCRATCH_DB     # optional; falls back to defaults.database
```

A model overrides it in front-matter with `database: FIN_DB`.

Order: front-matter, then `dev.database` in dev mode, then `defaults.database`, else empty (two-part name).

Supported dialects: Snowflake, BigQuery, Databricks, Trino, DuckDB family, SQL Server family, Fabric. Other dialects give a compile error.

## Four layers (project convention)

| Layer | Folder = Schema | Naming | Materialization |
|-------|-----------------|--------|-----------------|
| raw | `raw_<source>` | source-native | replication target — not a build folder |
| staging | `staging` | `stg_<source>__<entity>` | view |
| intermediate | `intermediate` | `int_<entity>__<verb>` | view |
| marts | `marts` | `fct_*`, `dim_*` | table / incremental |

Staging selects from raw via `src()`. Marts use `ref()` to staging or intermediate only. Marts never touch raw.

## File classification

| Extension | Kind |
|-----------|------|
| `.sql` | Model |
| `.macros.sql` | Macro file (not executed) |
| `.csv` / `.json` / `.parquet` | Seed |
| Other | Ignored |

## Dev vs prod names

| File Path | Prod | Dev (`dev_fritz`) |
|-----------|------|-------------------|
| `staging/stg_orders.sql` | `staging.stg_orders` | `dev_fritz.stg_orders` |
| `marts/core/dim_customers.sql` | `marts.dim_customers` | `dev_fritz.dim_customers` |

Dev and prod names differ only by schema. Names are unique project-wide, so one dev schema never has a collision.

Dev mode is on when `dev:` is present in `sling_build.yml`. Prod mode writes folder-based schemas (CLI `--prod`, pipeline step `prod: true`). Forcing a schema (CLI `--schema`, step `schema:`) selects a dev schema.

## Nested configs

Recursive discovery is opt-in (CLI `-R`/`--recursive`, pipeline step `recursive: true`). Without it, only `<path>/sling_build.yml` loads.

A subdirectory with its own `sling_build.yml` is a nested project. Parent discovery skips it unless recursion is enabled (immediate children only). Do not leave scratch projects (`probe/`) inside a parent.
