# Project structure decisions

Use this guide when the user asks where a file goes, whether to split a model, or when to add a seed or macro.

The canonical tree is in [SKILL.md](SKILL.md). Folder → table mapping is in [../sling-build/STRUCTURE.md](../sling-build/STRUCTURE.md).

## Folder

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

## Split

Keep **one model per file**. Split when:

* A staging model covers more than one raw table — that is a join. Move the join to intermediate.
* A mart query mixes two grains (order vs order line) — make two marts.
* A file grows past one SELECT with a clear name — extract the shared logic to intermediate or a macro.

Do not split only to match a BI folder. Nested folders under `marts/` become a name prefix (`marts/core/fct_orders.sql` → `marts.core_fct_orders`). Prefer a flat `marts/` unless the prefix is deliberate.

## Layer

Promote down the list. Do not skip raw → marts.

| Move to | When |
|---------|------|
| staging | You rename, cast, or dedup a raw table. Still 1:1. |
| intermediate | You join, filter business logic, or union sources. Not for BI. |
| marts | A consumer (dashboard, reverse ETL) needs a stable contract. |

Keep a model in staging if it only casts and renames. Do not "help" with a join there.

Never select from `raw_*` in a mart. Insert an intermediate model if staging is not enough.

## Seed

Add a seed when the data is **static**, **small**, and **versioned in git**:

* Country codes, status maps, fiscal calendars
* Lookup tables that sources do not provide

Do not seed data that a replication can load. Do not seed large fact-like files.

Seed names share the global namespace with models. `country_codes.csv` collides with `country_codes.sql`.

## Macro promotion

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
