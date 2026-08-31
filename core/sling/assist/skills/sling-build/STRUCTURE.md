# Build structure and naming

Companion to the main build guide. Folder layout for a full ELT repo: https://docs.slingdata.io/concepts/project-structure.md

A build project is a directory with `sling_build.yml` plus `.sql` models (and optional seeds). In the canonical project that directory is `models/`.

## Folder = schema

The 1st folder level is the schema name. Nested folders become an underscore-joined prefix on the model name. Root-level files use the `public` schema.

| File Path | Schema | Prefix | Name | Full Table Name |
|-----------|--------|--------|------|-----------------|
| `raw.sql` | public | | raw | `public.raw` |
| `staging/stg_orders.sql` | staging | | stg_orders | `staging.stg_orders` |
| `staging/country_codes.csv` | staging | | country_codes | `staging.country_codes` |
| `marts/core/dim_customers.sql` | marts | core | dim_customers | `marts.core_dim_customers` |
| `marts/core/fct_orders.sql` | marts | core | fct_orders | `marts.core_fct_orders` |
| `seeds/status_map.json` | seeds | | status_map | `seeds.status_map` |

Docs: https://docs.slingdata.io/concepts/build/structure.md#naming-rules

## Names are global

Model and seed names must be unique across the whole project, in every folder. Duplicate names are a hard engine error (`validateUniqueNames`).

`stg_customers.sql` in `staging/` and `marts/` still collides. Use `stg_<source>__<entity>` so two sources with the same entity do not collide.

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
| `staging/stg_orders.sql` | `staging.stg_orders` | `dev_fritz.staging_stg_orders` |
| `marts/core/dim_customers.sql` | `marts.core_dim_customers` | `dev_fritz.marts_core_dim_customers` |

Dev mode is on when `dev:` is present in `sling_build.yml`. Prod mode writes folder-based schemas (CLI `--prod`, pipeline step `prod: true`). Forcing a schema (CLI `--schema`, step `schema:`) selects a dev schema.

## Nested configs

Recursive discovery is opt-in (CLI `-R`/`--recursive`, pipeline step `recursive: true`). Without it, only `<path>/sling_build.yml` loads.

A subdirectory with its own `sling_build.yml` is a nested project. Parent discovery skips it unless recursion is enabled (immediate children only). Do not leave scratch projects (`probe/`) inside a parent.
