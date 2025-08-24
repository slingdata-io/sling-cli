# Sling Connection Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [MCP Tool Integration](#2-mcp-tool-integration)
3. [Connection Types Overview](#3-connection-types-overview)
4. [Database Connections](#4-database-connections)
5. [File/Storage Connections](#5-filestorage-connections)
6. [Datalake Connections](#6-datalake-connections)
7. [API Connections](#7-api-connections)
8. [Connection Management](#8-connection-management)
9. [Discovery Operations](#9-discovery-operations)
10. [Environment and Configuration](#10-environment-and-configuration)
11. [Integration with Replications](#11-integration-with-replications)
12. [Best Practices](#12-best-practices)
13. [Examples and Common Patterns](#13-examples-and-common-patterns)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Introduction and Overview

### What are Sling Connections?

Sling connections are configurations that define how to access and interact with various data sources and destinations. They provide a unified interface for connecting to over 40 different systems including databases, file systems, cloud storage, and APIs.

### Key Benefits

- **Unified Interface**: Single API for all connection types
- **Security**: Centralized credential management
- **Flexibility**: Support for multiple authentication methods
- **Discovery**: Built-in schema and metadata discovery
- **Testing**: Connection validation and health checks
- **Integration**: Seamless use with replications and pipelines

### When to Use Connections

- Setting up data sources and destinations for replications
- Exploring database schemas and file systems
- Running queries and data operations
- Managing credentials securely
- Discovering available data streams

### Core Concepts

- **Connection Name**: Unique identifier for the connection (typically UPPERCASE)
- **Connection Type**: The specific system type (e.g., postgres, s3, snowflake)
- **Properties**: Configuration parameters specific to each connection type
- **Environment**: Where credentials and configurations are stored
- **Location String**: Format for referencing connection paths (`CONNECTION_NAME/path/to/resource`)

---

## 2. MCP Tool Integration

The Sling MCP provides three main tools for working with connections:

- **`connection`** - General connection management operations
- **`database`** - Database-specific operations (requires separate documentation call)
- **`file_system`** - File system operations (requires separate documentation call)

### Connection Tool

The `connection` tool provides core connection management through various actions:

#### Available Actions

- `docs` - Get comprehensive connection documentation
- `list` - List all available connections
- `set` - Create or update a connection
- `test` - Test connection validity
- `discover` - Discover tables/files in a connection

#### Basic Tool Usage

```json
{
  "tool": "connection",
  "action": "action_name",
  "input": {
    "parameter1": "value1",
    "parameter2": "value2"
  }
}
```

#### Getting Documentation

```json
{
  "tool": "connection",
  "action": "docs",
  "input": {}
}
```

### Database and File System Tools

For database operations (queries, schema exploration) and file system operations (file listing, copying), use separate tools:

- **Database Operations**: Use `database` tool - call `database` with `action: "docs"` for full documentation
- **File System Operations**: Use `file_system` tool - call `file_system` with `action: "docs"` for full documentation

---

## 3. Connection Types Overview

Sling supports 41+ different connection types across four categories:

### Database Connections (24 types)
- **Relational**: PostgreSQL, MySQL, MariaDB, SQLServer, Oracle, SQLite
- **Cloud Warehouses**: Snowflake, BigQuery, Redshift, Databricks
- **Analytics**: ClickHouse, DuckDB, MotherDuck, StarRocks, Trino, Proton
- **NoSQL**: MongoDB, ElasticSearch, Prometheus
- **Specialized**: Azure Table, BigTable, Exasol, D1

### File/Storage Connections (13 types)
- **Cloud Storage**: AWS S3, Google Cloud Storage, Azure Blob
- **S3-Compatible**: MinIO, DigitalOcean Spaces, Cloudflare R2, Wasabi, B2
- **File Transfer**: FTP, SFTP
- **Cloud Drives**: Google Drive
- **Local**: Local file system

### Datalake Connections (4 types)
- **Query Engines**: Athena, DuckLake
- **Table Formats**: Iceberg

### API Connections
- **Custom APIs**: User-defined API specifications in YAML format

---

## 4. Database Connections

Navigate or fetch the content of the connector from the below respective URL to learn more about it.

**PostgreSQL (`postgres`)** -> https://docs.slingdata.io/connections/database-connections/postgres

**MySQL (`mysql`)** -> https://docs.slingdata.io/connections/database-connections/mysql

**Snowflake (`snowflake`)** -> https://docs.slingdata.io/connections/database-connections/snowflake

**BigQuery (`bigquery`)** -> https://docs.slingdata.io/connections/database-connections/bigquery

**SQL Server (`sqlserver`)** -> https://docs.slingdata.io/connections/database-connections/sqlserver

**Oracle (`oracle`)** -> https://docs.slingdata.io/connections/database-connections/oraclese

**ClickHouse (`clickhouse`)** -> https://docs.slingdata.io/connections/database-connections/clickhouse

**DuckDB (`duckdb`)** -> https://docs.slingdata.io/connections/database-connections/duckdb

**Redshift (`redshift`)** -> https://docs.slingdata.io/connections/database-connections/redshift

**Databricks (`databricks`)** -> https://docs.slingdata.io/connections/database-connections/databricks

**MongoDB (`mongodb`)** -> https://docs.slingdata.io/connections/database-connections/mongodb

**MariaDB (`mariadb`)** -> https://docs.slingdata.io/connections/database-connections/mariadb

**SQLite (`sqlite`)** -> https://docs.slingdata.io/connections/database-connections/sqlite

**MotherDuck (`motherduck`)** -> https://docs.slingdata.io/connections/database-connections/motherduck

**ElasticSearch (`elasticsearch`)** -> https://docs.slingdata.io/connections/database-connections/elasticsearch

**Prometheus (`prometheus`)** -> https://docs.slingdata.io/connections/database-connections/prometheus

**StarRocks (`starrocks`)** -> https://docs.slingdata.io/connections/database-connections/starrocks

**Trino (`trino`)** -> https://docs.slingdata.io/connections/database-connections/trino

**Proton (`proton`)** -> https://docs.slingdata.io/connections/database-connections/proton

**Azure Table (`azuretable`)** -> https://docs.slingdata.io/connections/database-connections/azuretable

**BigTable (`bigtable`)** -> https://docs.slingdata.io/connections/database-connections/bigtable

**Exasol (`exasol`)** -> https://docs.slingdata.io/connections/database-connections/exasol

**D1 (`d1`)** -> https://docs.slingdata.io/connections/database-connections/d1

---

## 5. File/Storage Connections

Navigate or fetch the content of the connector from the below respective URL to learn more about it.

**AWS S3 (`s3`)** -> https://docs.slingdata.io/connections/file-connections/s3

**Google Cloud Storage (`gs`)** -> https://docs.slingdata.io/connections/file-connections/gs

**Azure Blob Storage (`azure`)** -> https://docs.slingdata.io/connections/file-connections/azure

**MinIO (`minio`)** -> https://docs.slingdata.io/connections/file-connections/minio

**DigitalOcean Spaces (`dospaces`)** -> https://docs.slingdata.io/connections/file-connections/dospaces

**Cloudflare R2 (`r2`)** -> https://docs.slingdata.io/connections/file-connections/r2

**FTP (`ftp`)** -> https://docs.slingdata.io/connections/file-connections/ftp

**SFTP (`sftp`)** -> https://docs.slingdata.io/connections/file-connections/sftp

**Google Drive (`gdrive`)** -> https://docs.slingdata.io/connections/file-connections/gdrive

**Wasabi (`wasabi`)** -> https://docs.slingdata.io/connections/file-connections/wasabi

**Backblaze B2 (`b2`)** -> https://docs.slingdata.io/connections/file-connections/b2

**Local File System (`local`)** -> https://docs.slingdata.io/connections/file-connections/local

---

## 6. Datalake Connections

Navigate or fetch the content of the connector from the below respective URL to learn more about it.

**Amazon Athena (`athena`)** -> https://docs.slingdata.io/connections/datalake-connections/athena

**Apache Iceberg (`iceberg`) & AWS S3 Tables (`s3tables`)** -> https://docs.slingdata.io/connections/datalake-connections/iceberg

**DuckLake (`ducklake`)** -> https://docs.slingdata.io/connections/datalake-connections/ducklake

---

## 7. API Connections

API connections in Sling use custom YAML specifications to define how to interact with REST APIs.

### API Connection Properties
**Properties:**
- `type` (required) - Must be "api"
- `spec` (required) - Path to API specification file (`file:///path/to/spec.yaml`)
- `secrets` - Object containing API credentials and secrets

### API Specification Structure

API specifications define:
- **Authentication**: Basic, OAuth2, API keys, custom headers
- **Endpoints**: URL patterns, methods, parameters
- **Pagination**: Cursor, offset, or page-based pagination
- **Response Processing**: JSON path extraction, data transformation
- **Rate Limiting**: Request throttling and retry logic

### Example API Connection Setup

```json
{
  "action": "set",
  "input": {
    "name": "MY_API",
    "properties": {
      "type": "api",
      "spec": "file:///path/to/my_api_spec.yaml",
      "secrets": {
        "api_key": "your-api-key",
        "client_id": "your-client-id",
        "client_secret": "your-client-secret"
      }
    }
  }
}
```

---

## 8. Connection Management

### Listing Connections

Get all available connections across all sources (env.yaml, dbt profiles, environment variables):

```json
{
  "action": "list",
  "input": {}
}
```

**Returns:** JSON array with connection details including name, type, and source.

### Setting Connections

Create or update a connection in the Sling environment file:

```json
{
  "action": "set",
  "input": {
    "name": "MY_POSTGRES",
    "properties": {
      "type": "postgres",
      "host": "localhost",
      "user": "myuser",
      "database": "mydb",
      "password": "mypass",
      "port": 5432
    }
  }
}
```

**Important Notes:**
- Check existing connections with `list` before overwriting
- Sensitive credentials should be manually set in `~/.sling/env.yaml`
- The tool will provide the env.yaml file path after setting

### Testing Connections

Validate that a connection works properly:

```json
{
  "action": "test",
  "input": {
    "connection": "MY_POSTGRES",
    "debug": true,
    "trace": false
  }
}
```

**Parameters:**
- `connection` (required) - Connection name to test
- `debug` - Enable debug logging
- `trace` - Enable trace logging
- `endpoints` - Array of specific endpoints to test (API connections only)
- `limit` - Maximum records to process during test

---

## 9. Discovery Operations

### Basic Discovery

Discover tables, files, or endpoints in a connection:

```json
{
  "action": "discover",
  "input": {
    "connection": "MY_POSTGRES",
    "pattern": "public.*",
    "recursive": false,
    "columns": false
  }
}
```

**Parameters:**
- `connection` (required) - Connection name
- `pattern` - Filter pattern (e.g., `schema.prefix_*`, `dir/*.csv`, `*/*/*.parquet`)
- `recursive` - List files recursively (file connections)
- `columns` - Include column-level metadata

**Pattern Examples:**
- Database: `public.*`, `schema.table_name`, `*.users`
- Files: `dir/*.csv`, `data/**/*.json`, `logs/2024/*.parquet`

### Column-Level Discovery

Get detailed column information:

```json
{
  "action": "discover",
  "input": {
    "connection": "MY_POSTGRES",
    "pattern": "public.users",
    "columns": true
  }
}
```

**Returns:** Detailed column metadata including names, types, and positions.

---

## 10. Environment and Configuration

### Sling Environment File

Connections are primarily stored in `~/.sling/env.yaml`:

```yaml
connections:
  MY_POSTGRES:
    type: postgres
    host: localhost
    user: myuser
    database: mydb
    password: ${PG_PASSWORD}
    port: 5432
  
  MY_S3:
    type: s3
    bucket: my-bucket
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}

variables:
  global_var1: "value1"
  global_var2: "value2"
```

### Environment Variables

Connections can also be defined as environment variables:

```bash
# URL format
export MY_POSTGRES='postgresql://user:pass@host:5432/db'

# YAML format
export MY_S3='{type: s3, bucket: my-bucket, access_key_id: key, secret_access_key: secret}'
```

### DBT Profiles Integration

Sling automatically reads DBT profiles from `~/.dbt/profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: myuser
      password: mypass
      port: 5432
      dbname: mydb
      schema: public
```

### Credential Precedence

1. Sling env.yaml file (`~/.sling/env.yaml`)
2. Environment variables
3. DBT profiles (`~/.dbt/profiles.yml`)

### Location String Format

The location string format is used throughout Sling for referencing connection resources:

**Database Objects:**
- `CONNECTION_NAME/schema.table`
- `CONNECTION_NAME/database.schema.table`

**File System Objects:**
- `CONNECTION_NAME/path/to/file.txt`
- `CONNECTION_NAME/folder/` (for directories)
- `local/relative/path` (relative paths)
- `local//absolute/path` (absolute paths - note double slash)

---

## 11. Integration with Replications

Connections are the foundation of Sling replications, defining source and target systems.

### Basic Replication Structure

```yaml
source: MY_POSTGRES
target: MY_S3

defaults:
  mode: full-refresh

streams:
  public.users:
    object: users.csv
  public.orders:
    object: orders.parquet
```

### Connection Variables in Replications

Access connection properties in replication configurations:

```yaml
source: MY_POSTGRES
target: MY_S3

defaults:
  target_options:
    file_path: "exports/{source.schema}.{stream.object}.csv"

streams:
  public.*:
```

---

## 12. Best Practices

### Security

1. **Never hardcode credentials** in connection configurations
2. **Use environment variables** for sensitive values (`${PASSWORD}`)
3. **Rotate credentials regularly** and update connections
4. **Use least-privilege access** - only grant necessary permissions
5. **Enable SSL/TLS** for database connections where available
6. **Use SSH tunnels** for secure database access over untrusted networks

### Connection Naming

1. **Use UPPERCASE names** for consistency (automatic conversion)
2. **Use descriptive names** that indicate purpose: `PROD_POSTGRES`, `STAGING_S3`
3. **Include environment** in the name: `DEV_DB`, `PROD_DW`
4. **Group related connections** with prefixes: `SALES_DB`, `SALES_S3`

### Performance

1. **Test connections** before using in replications
2. **Use appropriate connection pooling** settings for databases
3. **Configure timeouts** appropriately for your network
4. **Use regions closest** to your data for cloud storage
5. **Enable compression** where supported

### Maintenance

1. **Document connection purposes** and owners
2. **Regular testing** of critical connections
3. **Monitor connection health** in production
4. **Version control** env.yaml file (without secrets)
5. **Backup connection configurations**

---

## 13. Examples and Common Patterns

### Database to Database Replication

```json
// Set source database
{
  "action": "set",
  "input": {
    "name": "SOURCE_PG",
    "properties": {
      "type": "postgres",
      "host": "source-db.com",
      "user": "reader",
      "database": "production"
    }
  }
}

// Set target warehouse
{
  "action": "set",
  "input": {
    "name": "TARGET_SNOWFLAKE",
    "properties": {
      "type": "snowflake",
      "account": "myaccount",
      "user": "loader",
      "database": "analytics",
      "warehouse": "LOAD_WH"
    }
  }
}
```

### File to Database Loading

```json
// Set S3 source
{
  "action": "set",
  "input": {
    "name": "DATA_S3",
    "properties": {
      "type": "s3",
      "bucket": "data-lake",
      "use_environment": true
    }
  }
}

// Discover available files
{
  "action": "discover",
  "input": {
    "connection": "DATA_S3",
    "pattern": "raw/2024/*/*.parquet"
  }
}
```

### API Data Extraction

```json
// Set up API connection
{
  "action": "set",
  "input": {
    "name": "SALESFORCE_API",
    "properties": {
      "type": "api",
      "spec": "file:///configs/salesforce_spec.yaml",
      "secrets": {
        "client_id": "your-client-id",
        "client_secret": "your-client-secret",
        "username": "api-user@company.com",
        "password": "password-with-token"
      }
    }
  }
}

// Test API endpoints
{
  "action": "test",
  "input": {
    "connection": "SALESFORCE_API",
    "endpoints": ["accounts", "contacts"],
    "limit": 10,
    "debug": true
  }
}
```

### SSH Tunnel Database Access

```json
{
  "action": "set",
  "input": {
    "name": "SECURE_DB",
    "properties": {
      "type": "postgres",
      "host": "internal-db.local",
      "user": "appuser",
      "database": "production",
      "ssh_tunnel": "ssh://tunnel-user@bastion.company.com:22",
      "ssh_private_key": "/path/to/private/key"
    }
  }
}
```

---

## 14. Troubleshooting

### Connection Test Failures

**Symptom:** Connection test fails with timeout or connection refused

**Solutions:**
1. Verify host and port are correct
2. Check network connectivity and firewall rules
3. Confirm the service is running
4. Test with debug/trace logging enabled
5. Verify SSL/TLS configuration

**Debug Example:**
```json
{
  "action": "test",
  "input": {
    "connection": "MY_DB",
    "debug": true,
    "trace": true
  }
}
```

### Authentication Failures

**Symptom:** "Access denied" or "Authentication failed"

**Solutions:**
1. Verify username and password
2. Check if account is locked or expired
3. Confirm user has necessary permissions
4. For cloud services, verify region settings
5. Check if MFA is required

### SSL/TLS Issues

**Symptom:** "SSL connection error" or certificate warnings

**Solutions:**
1. Set appropriate `sslmode` for database connections
2. Verify certificate chain is complete
3. Check if self-signed certificates are being used
4. Update connection to use correct SSL settings

### File System Access Issues

**Symptom:** "Access denied" or "Path not found"

**Solutions:**
1. Verify bucket/container exists
2. Check IAM permissions and access policies
3. Confirm region settings for cloud storage
4. Test with anonymous access if appropriate
5. Verify file/directory paths are correct

### Discovery Returns No Results

**Symptom:** Empty results from discovery operations

**Solutions:**
1. Check pattern syntax and wildcards
2. Verify schema/path exists
3. Confirm user has read permissions
4. Try broader patterns (e.g., `*.*` instead of specific schemas)

### Performance Issues

**Symptom:** Slow connection tests or operations

**Solutions:**
1. Check network latency to target system
2. Adjust timeout settings
3. Verify system isn't under heavy load
4. Consider using connection pooling
5. Use regional endpoints for cloud services

### Environment Configuration Issues

**Symptom:** Connections not found or credentials not working

**Solutions:**
1. Check `~/.sling/env.yaml` file exists and has correct format
2. Verify environment variables are set correctly
3. Check file permissions on configuration files
4. Ensure DBT profiles are in the correct location
5. Use `connection` tool with `list` action to see which connections are detected

### Common Error Messages

**"Invalid Connection name: X. Make sure it is created."**
- Connection doesn't exist or name is wrong
- Use `connection` tool with `list` action to see available connections
- Create the connection with `connection` tool and `set` action

**"could not init database connection"**
- Database-specific connection error
- Enable debug logging to see detailed error
- Verify connection properties are correct

**"could not init file connection - connection must be a file/storage type"**
- Trying to use file operations on a database connection
- Verify connection type matches the operation

### Getting Help

When troubleshooting:
1. Use debug/trace logging to get detailed error information
2. Test connections individually before using in replications
3. Verify credentials and permissions outside of Sling first
4. Check official documentation for specific connection types
5. Use the `connection` tool with `docs` action for the latest information

---

This comprehensive guide covers all aspects of Sling connection management for LLM/AI usage. For database operations, use the `database` tool with the `docs` action. For file system operations, use the `file_system` tool with the `docs` action.