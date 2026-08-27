# Change Capture (CDC)

CDC replicates row-level changes (inserts, updates, deletes) from a source database's transaction log. Unlike `incremental` mode, which polls the table, CDC captures every change, including deletes.

**Plan gate**: CDC requires a CLI Pro Max token or an Advanced Platform Plan.

## Supported Sources

| Source | Reads from | Setup guide |
|--------|-----------|-------------|
| PostgreSQL | WAL via logical replication publication | https://docs.slingdata.io/concepts/change-capture/postgres.md |
| MySQL / MariaDB | Binary log (`binlog_format=ROW`) | https://docs.slingdata.io/concepts/change-capture/mysql.md |
| SQL Server | CDC capture tables | https://docs.slingdata.io/concepts/change-capture/sql-server.md |
| Oracle | GoldenGate Data Streams | https://docs.slingdata.io/concepts/change-capture/oracle.md |
| MongoDB | Change streams (oplog) | https://docs.slingdata.io/concepts/change-capture/mongodb.md |

## Prerequisite: SLING_STATE

CDC stores its log position and snapshot progress in the `SLING_STATE` location. Set it **before** the first run. Without it, each run repeats the initial snapshot.

```bash
export SLING_STATE='MY_POSTGRES/sling_state'   # database table
export SLING_STATE='MY_AWS/sling_state'        # or file location
```

## Configuration

Use `mode: change-capture` in a normal replication file. CDC options go under `change_capture_options` (defaults + per-stream overrides):

```yaml
source: MY_SOURCE
target: MY_TARGET

defaults:
  mode: change-capture
  primary_key: [id]
  object: public.{stream_table}
  change_capture_options:
    run_max_events: 10000
    run_max_duration: 10m

streams:
  my_database.users:
  my_database.orders:
    change_capture_options:
      soft_delete: true       # keep deleted rows, marked _sling_synced_op='D'
```

## Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `run_max_events` | `10000` | Max change events per run, then save position and exit. One event = one log statement, so it can hold many rows. |
| `run_max_duration` | `10m` | Max wall-clock time per run. |
| `soft_delete` | `false` | Mark deletes with `_sling_synced_op='D'` instead of row removal. |
| `snapshot_start` | `now` | First-run log start: `now` or `beginning`. |
| `snapshot_chunk_size` | `100000` | Rows per chunk in the initial snapshot (needs integer-like PK; else single-shot read). |
| `snapshot_run_duration` | none | Time budget for the snapshot per run; resumes next run. |
| `replay_from` | — | Rewind position (timestamp, binlog position, GTID). Applied once per unique value. |
| `slot_level` | `shared`* | `shared` = one log reader for all streams (Postgres, MySQL); `stream` = one per table (all others). |
| `change_feed` | — | Name of the DBA-provisioned server-side object to read (see below). |
| `retry_attempts` | `3` | Retries on transient failures. |
| `retry_delay` | `5s` | Delay between retries. |

## Setup Is DBA-Owned (`change_feed`)

Sling only **reads** CDC. It does not run `CREATE PUBLICATION`, `sp_cdc_enable_*`, or `ALTER TABLE` on the source, so the connection role stays read-only and works on managed databases (RDS, Cloud SQL, Azure SQL). A DBA provisions the server-side object once; `change_feed` names it:

| Source | `change_feed` maps to |
|--------|-----------------------|
| PostgreSQL | Logical replication publication |
| SQL Server | CDC capture instance |
| Oracle | GoldenGate Data Stream (or `gg_stream` conn property) |
| MySQL / MongoDB | Not used (no named server object) |

If an object or grant is missing, Sling errors with the exact DDL for the DBA to run. See the per-source setup guides above for scripts and privileges.

## Behavior

- **First run**: chunked full snapshot with checkpoints, resumable after interruption. The log position is captured before the snapshot, so no change is lost.
- **Later runs**: read the log from the saved position, merge changes into the target, save the new position, exit. Runs are bounded — schedule them on a short interval (cron or Sling Platform).
- **Metadata columns** added to every CDC target table: `_sling_synced_at` (timestamptz), `_sling_synced_op` (`S`napshot/`I`nsert/`U`pdate/`D`elete), `_sling_cdc_seq` (bigint ordering).

## CDC vs Incremental

| | `change-capture` | `incremental` |
|---|---|---|
| Deletes | Yes | Only with `delete_missing` target option |
| Reads from | Transaction log | Table query on `update_key` |
| Source load | Minimal | Query per run |
| First load | Automatic snapshot | Normal first run |

If CDC is unavailable (plan or source support), `incremental` + `delete_missing: soft|hard` is the closest alternative.

## Full Documentation

See https://docs.slingdata.io/concepts/change-capture.md for the complete reference.
