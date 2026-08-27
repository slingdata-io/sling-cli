---
name: sling-connections
description: >
  Manage database, file system, and API connections for Sling. Use when setting up connections, testing connectivity, discovering tables/files, or configuring credentials.
---

# Connection Management

Connections are named endpoints for databases, file systems, and APIs. Sling stores them in `~/.sling/env.yaml`.

## Gather first

Resolve each row before you write YAML, in this order: the user's request, existing files, discovery (MCP / CLI), then the default in the table. Ask the user only for rows that stay unresolved, one at a time, with a proposed default. If every row resolves, do not ask — build, and state the assumptions you took. Confirm first only for destructive resolutions (for example a mode that drops or rewrites existing tables).

| Decision | Resolve by | Default |
|----------|------------|---------|
| connector type | request, URL scheme | none — propose from the URL or docs |
| required props | request, connector docs | scaffold `${VAR}` refs |
| secret handling | always | `${VAR}` + `sling conns set`; never secrets in chat |

## Connection Types

| Type | Examples | Kind |
|------|----------|------|
| Database | postgres, mysql, snowflake, bigquery | `database` |
| Datalake | iceberg, ducklake, athena, s3 tables | `database` |
| File System | s3, gcs, azure, sftp, local | `file` |
| API | Custom REST APIs via specs | `api` |

## MCP Operations

### List Connections
```json
{"action": "list", "input": {}}
```

### Test Connection
```json
{
  "action": "test",
  "input": {
    "connection": "MY_POSTGRES",
    "debug": true
  }
}
```

### Discover Streams
```json
{
  "action": "discover",
  "input": {
    "connection": "MY_POSTGRES",
    "pattern": "public.*",
    "columns": true
  }
}
```

### Create Connection
```json
{
  "action": "set",
  "input": {
    "name": "MY_POSTGRES",
    "properties": {
      "type": "postgres",
      "host": "localhost",
      "user": "myuser",
      "database": "mydb"
    }
  }
}
```

## CLI Operations

```bash
sling conns list                              # List all
sling conns test MY_POSTGRES --debug          # Test
sling conns discover MY_POSTGRES --pattern "public.*"  # Discover
sling conns exec MY_POSTGRES "SELECT 1"       # Execute SQL (query is positional)
```

## Configuration Methods

### 1. Environment Variable
```bash
export MY_POSTGRES='host=localhost user=postgres dbname=mydb'
```

### 2. URL Format
```bash
export MY_POSTGRES='postgresql://user:pass@host:5432/dbname'
```

### 3. YAML in env.yaml
```yaml
# ~/.sling/env.yaml
connections:
  MY_POSTGRES:
    type: postgres
    host: localhost
    user: postgres
    password: ${PG_PASSWORD}
    database: mydb

  MY_S3:
    type: s3
    bucket: my-bucket
    access_key_id: ${AWS_ACCESS_KEY}
    secret_access_key: ${AWS_SECRET_KEY}

  MY_API:
    type: api
    spec: stripe
    secrets:
      api_key: ${STRIPE_API_KEY}
```

## Common Connection Properties

### Database Connections

| Property | Description |
|----------|-------------|
| `type` | Database type (postgres, mysql, etc.) |
| `host` | Hostname or IP |
| `port` | Port number |
| `user` | Username |
| `password` | Password |
| `database` | Database name |
| `schema` | Default schema |
| `ssh_tunnel` | SSH tunnel URL |

### File System Connections

| Property | Description |
|----------|-------------|
| `type` | File system type (s3, gcs, etc.) |
| `bucket` | Bucket/container name |
| `access_key_id` | Access key |
| `secret_access_key` | Secret key |
| `region` | Cloud region |

### API Connections

| Property | Description |
|----------|-------------|
| `type` | Must be `api` |
| `spec` | API spec name or path |
| `secrets` | Authentication credentials |
| `inputs` | Optional configuration |

## Discovery Patterns

```bash
sling conns discover MY_PG --pattern "public.*"           # All in schema
sling conns discover MY_PG --pattern "sales.customer_*"   # With prefix
sling conns discover MY_S3 --pattern "data/*.csv"         # Files
sling conns discover MY_S3 --pattern "**/*.parquet" --recursive
sling conns discover MY_PG --pattern "public.users" --columns
```

## Secrets Boundary

These rules are mandatory for agents:

1. Never ask for credentials in chat.
2. Scaffold `${VAR}` refs and give the connector deep link. The user fills the values.
3. Never `cat` or read env.yaml. `sling validate env.yaml` and `conns list` are the only machine views.
4. Use this loop: test → sanitized error → user edits → retest.
   - CLI: `sling conns test NAME --debug`
   - Footer: `sling assist --id <id>` — investigates that failure directly
   - MCP: `connection.test` (`{"action":"test","input":{"connection":"NAME","debug":true}}`)

Do not print secret values. Do not write real passwords into YAML.

Deep links (lowercase connector name):

```
https://docs.slingdata.io/connections/database-connections/<name>.md
https://docs.slingdata.io/connections/datalake-connections/<name>.md
https://docs.slingdata.io/connections/file-connections/<name>.md
https://docs.slingdata.io/connections/api-connections/<name>.md
```

## Security Best Practices

1. **Use environment variables** for sensitive credentials (`${PG_PASSWORD}` form)
2. **Never commit** env.yaml with real credentials to git
3. **Use IAM roles** when possible (AWS, GCP)
4. **Test connections** before running replications
5. **Use SSH tunnels** for database access through bastion hosts

## Troubleshooting

### Connection refused
- Check host/port accessibility: `telnet host port`
- Verify firewall rules
- Confirm database is running

### Authentication failed
- Verify username/password
- Check user permissions
- Review database logs

### SSL/TLS errors
- Add `sslmode=disable` for testing (not production)
- Verify certificate validity

## Per-Connector Documentation

Each connector has its own guide with the exact properties and examples. Fetch the matching URL (lowercase connector name):

```
https://docs.slingdata.io/connections/database-connections/<name>.md
https://docs.slingdata.io/connections/datalake-connections/<name>.md
https://docs.slingdata.io/connections/file-connections/<name>.md
https://docs.slingdata.io/connections/api-connections/<name>.md
```

Examples: `.../database-connections/snowflake.md`, `.../datalake-connections/iceberg.md`, `.../file-connections/sftp.md`, `.../api-connections/stripe.md`.

## Full Documentation

See https://docs.slingdata.io/connections.md for complete reference.
