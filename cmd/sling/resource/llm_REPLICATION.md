# Sling Replication Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [Quick Start Guide](#2-quick-start-guide)
3. [Replication Structure](#3-replication-structure)
4. [Modes and Strategies](#4-modes-and-strategies)
5. [Source Configuration](#5-source-configuration)
6. [Target Configuration](#6-target-configuration)
7. [Column Management](#7-column-management)
8. [Data Transformations](#8-data-transformations)
9. [Variables and Dynamic Configuration](#9-variables-and-dynamic-configuration)
10. [Wildcards and Patterns](#10-wildcards-and-patterns)
11. [Hooks and Automation](#11-hooks-and-automation)
12. [Examples - Database to Database](#12-examples---database-to-database)
13. [Examples - File to Database](#13-examples---file-to-database)
14. [Examples - Database to File](#14-examples---database-to-file)
15. [Advanced Topics](#15-advanced-topics)
16. [MCP Tool Integration](#16-mcp-tool-integration)
17. [Best Practices](#17-best-practices)
18. [Troubleshooting](#18-troubleshooting)

---

## 1. Introduction and Overview

### What are Sling Replications?

Sling replications are YAML or JSON configuration files that define data movement operations from source to target systems. They are the best way to use Sling in a reusable, scalable manner, allowing you to define multiple data streams with shared configuration patterns.

### Key Benefits

- **Reusability**: Define defaults that apply to multiple streams
- **Scalability**: Handle multiple tables/files in a single configuration
- **Flexibility**: Override defaults on a per-stream basis
- **Pattern Matching**: Use wildcards to match multiple sources
- **Automation**: Built-in parallel processing and error handling

### When to Use Replications

- Moving multiple tables between databases
- Processing multiple files from a directory
- Applying consistent transformations across datasets
- Implementing incremental data pipelines
- Standardizing data movement workflows

### Core Concepts

- **Source**: The origin system (database, file system, cloud storage)
- **Target**: The destination system (database, file system, cloud storage)
- **Streams**: Individual data sources (tables, files) to replicate
- **Defaults**: Shared configuration applied to all streams
- **Modes**: How data is loaded (full-refresh, incremental, etc.)

### MCP Tool Integration

The Sling MCP tool provides these replication commands:
- `replication/docs` - Get documentation
- `replication/parse` - Parse and validate configuration
- `replication/compile` - Compile configuration with validation
- `replication/run` - Execute the replication

---

## 2. Quick Start Guide

### Essential MCP Commands

```json
// Get replication documentation
{
  "action": "docs",
  "input": {}
}

// Parse a replication file
{
  "action": "parse",
  "input": {
    "file_path": "/path/to/replication.yaml"
  }
}

// Run a replication
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "mode": "incremental"
  }
}
```

### Basic Replication Structure

```yaml
source: SOURCE_CONNECTION
target: TARGET_CONNECTION

defaults:
  mode: full-refresh
  object: '{target_schema}.{stream_schema}_{stream_table}'

streams:
  schema.table1:
  schema.table2:
    mode: incremental
    primary_key: [id]

env:
  SLING_THREADS: 3
```

### Simple Database-to-Database Example

```yaml
source: MY_POSTGRES
target: MY_SNOWFLAKE

defaults:
  mode: full-refresh
  object: 'warehouse.{stream_schema}_{stream_table}'

streams:
  # Single table
  public.customers:
    
  # All tables in schema
  public.*:
  
  # Exclude specific table
  public.sensitive_data:
    disabled: true

env:
  SLING_THREADS: 5
```

### Simple File-to-Database Example

```yaml
source: LOCAL
target: POSTGRES

defaults:
  mode: full-refresh
  object: 'staging.{stream_file_name}'
  source_options:
    format: csv
    header: true

streams:
  'file://data/customers.csv':
  'file://data/products.csv':
  'data/*.csv':  # All CSV files in directory

env:
  SLING_THREADS: 3
```

---

## 3. Replication Structure

### Root Level Keys

```yaml
# Required keys
source: <connection_name>
target: <connection_name>
streams: <streams_map>

# Optional keys
defaults: <default_stream_config>
hooks: <replication_hooks>
env: <environment_variables>
```

### Stream Configuration Map

Each stream accepts the following configuration:

```yaml
streams:
  stream_name:
    # Target specification
    object: <target_table_or_file>
    
    # Load behavior
    mode: full-refresh | incremental | truncate | snapshot | backfill
    disabled: true | false
    
    # Key columns
    primary_key: [column1, column2]
    update_key: column_name
    
    # Column management
    columns: {column_name: data_type}
    select: [column1, column2] # or [-column3, -column4] to exclude columns
    
    # Data source
    sql: <custom_sql_query>
    files: [file1.csv, file2.csv]
    where: <sql_where_clause>
    single: true | false
    
    # Transformations
    transforms: <transform_config>
    
    # Options
    source_options: <source_options_map>
    target_options: <target_options_map>
    
    # Hooks
    hooks: <stream_hooks>
    
    # Description
    description: <stream_description>
```

### Using Defaults

The `defaults` section allows you to define common configuration that applies to all streams:

```yaml
defaults:
  mode: incremental
  object: '{target_schema}.{stream_schema}_{stream_table}'
  primary_key: [id]
  source_options:
    empty_as_null: false
  target_options:
    column_casing: snake

streams:
  # Inherits all defaults
  schema.table1:
  
  # Overrides specific defaults
  schema.table2:
    mode: full-refresh
    target_options:
      column_casing: upper
```

### Configuration Inheritance

- Stream-level configuration overrides defaults
- Arrays and objects are replaced entirely (not merged)
- Use defaults for common patterns, stream-level for exceptions

---

## 4. Modes and Strategies

### Available Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `full-refresh` | Drop and recreate target table | Complete data replacement |
| `incremental` | Merge/upsert new data | Ongoing data updates |
| `truncate` | Clear table, preserve structure | Refresh while keeping DDL |
| `snapshot` | Append with timestamp | Historical data tracking |
| `backfill` | Load specific date/ID ranges | Historical data recovery |

### Incremental Mode Strategies

| Primary Key | Update Key | Strategy | Description |
|-------------|------------|----------|-------------|
| ✅ | ✅ | New Data Upsert | Only load records after max(update_key) |
| ✅ | ❌ | Full Data Upsert | Load all data, upsert by primary key |
| ❌ | ✅ | Append Only | Only load records after max(update_key) |

### Incremental with Custom SQL

Use placeholders for incremental loading:

```yaml
streams:
  custom_query:
    sql: |
      SELECT * FROM transactions 
      WHERE created_at > coalesce({incremental_value}, '1900-01-01')
      AND status = 'completed'
    update_key: created_at
    mode: incremental
```

**Placeholder Options:**
- `{incremental_where_cond}` - Complete WHERE condition
- `{incremental_value}` - Just the incremental value

### Backfill Mode

Load historical data in chunks:

```yaml
streams:
  large_table:
    mode: backfill
    update_key: created_date
    source_options:
      range: '2023-01-01,2023-12-31'
      chunk_size: 7d  # Process 7 days at a time
```

---

## 5. Source Configuration

### Database Sources

```yaml
defaults:
  source_options:
    limit: 100000  # Row limit
    empty_as_null: false
```

### File Sources

#### CSV Files
```yaml
defaults:
  source_options:
    format: csv
    header: true
    delimiter: ','
    encoding: utf8
    skip_blank_lines: true
```

#### JSON Files
```yaml
defaults:
  source_options:
    format: json
    jmespath: 'data[*]'  # Extract nested data
    flatten: true
```

#### Excel Files
```yaml
defaults:
  source_options:
    format: xlsx
    sheet: 'Sheet1'  # or 'Sheet1!A1:H100'
    header: true
```

#### Parquet Files
```yaml
defaults:
  source_options:
    format: parquet
    compression: auto
```

### Encoding Options

Handle international characters:

```yaml
source_options:
  encoding: utf8 | latin1 | windows1252 | windows1250 | utf16
```

### Compression Support

```yaml
source_options:
  compression: auto | gzip | zip | snappy | zstd
```

### Custom SQL Sources

```yaml
streams:
  analytics_view:
    sql: |
      SELECT 
        customer_id,
        SUM(order_total) as total_spent,
        COUNT(*) as order_count
      FROM orders 
      WHERE order_date >= '2024-01-01'
      GROUP BY customer_id
    object: analytics.customer_summary
```

### JMESPath for JSON

Extract specific data from complex JSON:

```yaml
streams:
  'api_response.json':
    source_options:
      jmespath: 'results[*].{id: id, name: name, email: contact.email}'
      flatten: true
```

---

## 6. Target Configuration

### Database Targets

#### Column Casing
```yaml
defaults:
  target_options:
    column_casing: snake  # source | target | snake | upper | lower | normalize
```

#### Column Typing
```yaml
defaults:
  target_options:
    column_typing:
      string:
        length_factor: 2  # Double string lengths
        max_length: 8000
      decimal:
        min_precision: 10
        max_precision: 38
```

#### Table Management
```yaml
defaults:
  target_options:
    add_new_columns: true
    adjust_column_type: false
    use_bulk: true
    direct_insert: true  # Use when user does not want a temp table.
```

#### Custom Table DDL
```yaml
defaults:
  target_options:
    table_ddl: 'CREATE TABLE {stream_table} ({col_types}) ENGINE=MergeTree'
```

#### Table Keys
```yaml
defaults:
  target_options:
    table_keys:
      primary: [id]
      index: [customer_id, created_at]
      cluster: [region]  # BigQuery/Snowflake
      partition: [date_column]  # PostgreSQL/ClickHouse
```

### File Targets

#### CSV Export
```yaml
defaults:
  target_options:
    format: csv
    header: true
    delimiter: ','
    encoding: utf8
```

#### Parquet Export
```yaml
defaults:
  target_options:
    format: parquet
    compression: snappy
    file_max_rows: 1000000
```

#### JSON Export
```yaml
defaults:
  target_options:
    format: jsonlines  # or json
    compression: gzip
```

#### File Splitting
```yaml
defaults:
  target_options:
    file_max_rows: 500000
    file_max_bytes: 50000000
```

---

## 7. Column Management

### Selecting Columns

```yaml
streams:
  customer_data:
    # Include only specific columns
    select: [id, name, email, created_at]
  
  sensitive_data:
    # Exclude specific columns
    select: [-ssn, -credit_card, -password]
```

### Column Type Casting

```yaml
defaults:
  columns:
    id: bigint
    amount: decimal(10,2)
    status: string(50)
    created_at: datetime

streams:
  special_table:
    columns:
      '*': string  # Cast all columns to string
      id: bigint   # Override for specific column
```

### Data Type Options

- `bigint` - Large integers
- `integer` - Standard integers  
- `decimal(precision, scale)` - Fixed-point decimals
- `string(length)` - Variable character strings
- `text(length)` - Large text fields
- `bool` - Boolean values
- `datetime` - Date and time values
- `json` - JSON data

### Column Constraints

```yaml
columns:
  age: 'integer | value >= 0 AND value <= 120'
  email: 'string(255) | value IS NOT NULL'
  amount: 'decimal(10,2) | value > 0'
```

### Schema Evolution

Sling automatically handles:
- Adding new columns from source
- Preserving existing data when columns are removed from source
- Type adjustments (when `adjust_column_type: true`)

---

## 8. Data Transformations

### Modern Transform Syntax (v1.4.17+)

#### Array Transforms (Global)
Apply to all columns:

```yaml
transforms: ['trim_space', 'remove_diacritics']
```

#### Map Transforms (Column-Specific)
Apply to specific columns:

```yaml
transforms:
  name: ['trim_space', 'upper']
  email: ['lower']
  phone: ['replace_non_printable']
```

#### Staged Transforms (Most Powerful)
Multi-stage with expressions:

```yaml
transforms:
  # Stage 1: Clean text fields
  - name: 'trim_space(value)'
    email: 'lower(value)'
  
  # Stage 2: Create computed columns
  - full_name: 'record.first_name + " " + record.last_name'
    email_hash: 'hash(record.email, "md5")'
  
  # Stage 3: Conditional logic
  - customer_tier: 'record.total_spent >= 1000 ? "premium" : "standard"'
    discount: 'record.customer_tier == "premium" ? 0.15 : 0.05'
```

### Available Functions (50+)

See https://docs.slingdata.io/concepts/functions for complete list.

#### String Functions
- `upper(value)` - Convert to uppercase
- `lower(value)` - Convert to lowercase
- `trim_space(value)` - Remove whitespace
- `replace(value, old, new)` - Replace text
- `substring(value, start, length)` - Extract substring
- `hash(value, algorithm)` - Hash value (md5, sha256)

#### Numeric Functions
- `int_parse(value)` - Convert to integer
- `float_parse(value)` - Convert to float
- `greatest(val1, val2, ...)` - Maximum value
- `least(val1, val2, ...)` - Minimum value

#### Date Functions
- `now()` - Current timestamp
- `date_parse(value, format)` - Parse date string
- `date_format(value, format)` - Format date
- `date_add(date, interval, unit)` - Add time interval

#### Utility Functions
- `coalesce(val1, val2, ...)` - First non-null value
- `cast(value, type)` - Type conversion
- `uuid()` - Generate UUID
- `if(condition, true_val, false_val)` - Conditional

### Complex Transform Examples

#### Customer Data Processing
```yaml
streams:
  customers:
    transforms:
      # Stage 1: Clean and normalize
      - first_name: 'trim_space(upper(value))'
        email: 'lower(trim_space(value))'
        phone: 'replace(value, "[^0-9]", "")'
      
      # Stage 2: Create computed fields
      - full_name: 'record.first_name + " " + record.last_name'
        email_domain: 'substring(record.email, strpos(record.email, "@") + 1)'
      
      # Stage 3: Business logic
      - customer_type: |
          record.total_orders >= 50 ? "vip" : (
            record.total_orders >= 10 ? "regular" : "new"
          )
```

### Legacy Transforms (Pre v1.4.17)

Still supported for backwards compatibility:

```yaml
transforms:
  - trim_space
  - replace_accents
  - replace_non_printable
  - empty_as_null
```

---

## 9. Variables and Dynamic Configuration

### Runtime Variables

Automatically replaced at execution:

#### Table/Stream Variables
- `{stream_name}` - Full stream name
- `{stream_schema}` - Source schema name
- `{stream_table}` - Source table name
- `{stream_full_name}` - Fully qualified source name

#### Connection Variables
- `{source_name}` - Source connection name
- `{target_name}` - Target connection name
- `{target_schema}` - Target default schema

#### Target Variables
- `{object_name}` - Target object name
- `{object_schema}` - Target object schema
- `{object_table}` - Target object table

#### File Variables
- `{stream_file_name}` - File name without extension
- `{stream_file_folder}` - Parent directory
- `{stream_file_ext}` - File extension
- `{stream_file_path}` - Full file path

### Timestamp Variables

- `{run_timestamp}` - Execution timestamp (2006_01_02_150405)
- `{YYYY}` - 4-digit year
- `{MM}` - 2-digit month
- `{DD}` - 2-digit day
- `{HH}` - 2-digit hour (24-hour)
- `{ISO8601}` - ISO 8601 format

### Partition Variables (Parquet Files)

Requires `update_key` specification:

- `{part_year}` - Year partition
- `{part_month}` - Month partition
- `{part_year_month}` - Combined year-month
- `{part_day}` - Day partition
- `{part_week}` - Week partition
- `{part_hour}` - Hour partition

```yaml
streams:
  transactions:
    object: 'data/{part_year}/{part_month}/{stream_table}'
    update_key: transaction_date
    target_options:
      format: parquet
```

### Environment Variables

#### In Configuration Files
```yaml
env:
  TARGET_SCHEMA: '${TARGET_SCHEMA}'  # From environment
  BATCH_SIZE: 10000
  SLING_THREADS: 5
```

#### In Object Names
```yaml
defaults:
  object: '{target_schema}.{stream_schema}_{stream_table}_{YYYY}_{MM}_{DD}'

env:
  target_schema: '${PROD_SCHEMA}'  # From environment
```

### Global Sling Variables

Set via `env` section or environment:

```yaml
env:
  SLING_THREADS: 10          # Parallel stream processing
  SLING_RETRIES: 2           # Retry failed streams
  SLING_LOADED_AT_COLUMN: true    # Add _sling_loaded_at timestamp
  SLING_STREAM_URL_COLUMN: true   # Add _sling_stream_url for files
```

---

## 10. Wildcards and Patterns

### Schema Wildcards

Match all tables in a schema:

```yaml
streams:
  # All tables in public schema
  public.*:
  
  # All tables in multiple schemas with prefix/suffix
  sales.marketing_*:   # use a prefix
  inventory.*_latest:  # use a suffix
  
  # Exclude specific tables
  public.*:
  public.temp_table:
    disabled: true
```

### File Wildcards

Match multiple files:

```yaml
streams:
  # All CSV files in directory
  'data/*.csv':
  
  # All JSON files recursively
  'data/**/*.json':
  
  # Specific pattern
  'logs/access_log_*.txt':
  
  # Multiple patterns
  '*.csv':
  '*.xlsx':
```

### Pattern Matching Examples

```yaml
source: POSTGRES
target: SNOWFLAKE

defaults:
  mode: incremental
  object: 'warehouse.{stream_schema}_{stream_table}'

streams:
  # All user-related tables
  public.user_*:
    primary_key: [user_id]
  
  # All tables except sensitive ones
  public.*:
  public.passwords:
    disabled: true
  public.credit_cards:
    disabled: true
  
  # Specific tables with custom config
  public.large_table:
    source_options:
      chunk_size: 12h
```

### Single Stream Mode

Treat wildcards as single stream:

```yaml
streams:
  # Process all files as one stream
  'logs/*.json':
    single: true
    object: combined_logs
    source_options:
      format: jsonlines
```

---

## 11. Hooks and Automation

### Hook Types

#### Replication-Level Hooks
Execute at replication start/end:

```yaml
hooks:
  start:
    - type: command
      command: 'echo "Starting replication at $(date)"'
    
    - type: query
      connection: MY_DB
      sql: 'UPDATE job_status SET status = "running" WHERE job_id = "repl_001"'
  
  end:
    - type: command
      command: 'echo "Replication completed at $(date)"'
```

#### Stream-Level Hooks
Execute before/after each stream:

```yaml
defaults:
  hooks:
    pre:
      - type: query
        connection: TARGET_DB
        sql: 'CREATE SCHEMA IF NOT EXISTS {target_schema}'
    
    post:
      - type: query
        connection: TARGET_DB
        sql: 'ANALYZE TABLE {object_name}'

streams:
  important_table:
    hooks:
      post:
        - type: command
          command: 'python validate_data.py {object_name}'
```

### Available Hook Types

See https://docs.slingdata.io/concepts/hooks for complete list.

- `command` - Execute shell commands
- `query` - Run SQL queries
- `http` - Make HTTP requests
- `copy` - Copy files
- `delete` - Delete files
- `write` - Write content to files

### Hook Examples

```yaml
hooks:
  start:
    # Notification
    - type: http
      url: 'https://slack.com/api/chat.postMessage'
      method: POST
      headers:
        Authorization: 'Bearer ${SLACK_TOKEN}'
      body: |
        {
          "channel": "#data-pipeline",
          "text": "Starting data replication"
        }
  
  end:
    # Cleanup
    - type: command
      command: 'rm -f /tmp/temp_files_*'
    
    # Update status
    - type: query
      connection: METADATA_DB
      sql: |
        INSERT INTO replication_log (job_id, status, completed_at)
        VALUES ('repl_001', 'completed', NOW())
```

---

## 12. Examples - Database to Database

### Full Refresh Replication

```yaml
source: POSTGRES_PROD
target: SNOWFLAKE_DW

defaults:
  mode: full-refresh
  object: 'analytics.{stream_schema}_{stream_table}'
  target_options:
    column_casing: snake

streams:
  # Single table
  public.customers:
  
  # All sales tables
  sales.*:
    object: 'sales_data.{stream_table}'
  
  # Large table with chunking
  public.transactions:
    source_options:
      chunk_count: 10

env:
  SLING_THREADS: 5
  SLING_LOADED_AT_COLUMN: true
```

### Incremental Replication

```yaml
source: MYSQL_APP
target: POSTGRES_DW

defaults:
  mode: incremental
  object: 'warehouse.{stream_table}'
  primary_key: [id]
  update_key: updated_at
  
  source_options:
    empty_as_null: false
  
  target_options:
    column_casing: snake
    add_new_columns: true

streams:
  # Standard incremental
  app.users:
  app.orders:
    primary_key: [order_id]
  
  # Append-only (no primary key)
  app.events:
    primary_key: []
    update_key: created_at
  
  # Custom SQL with incremental
  app.user_summary:
    sql: |
      SELECT 
        user_id,
        COUNT(*) as order_count,
        MAX(created_at) as last_order_date
      FROM orders 
      WHERE updated_at > coalesce({incremental_value}, '1900-01-01')
      GROUP BY user_id
    update_key: last_order_date

env:
  SLING_THREADS: 3
  SLING_RETRIES: 2
```

### Backfill Historical Data

```yaml
source: POSTGRES_OLD
target: BIGQUERY_DW

defaults:
  mode: backfill
  object: 'historical.{stream_table}'
  update_key: created_date
  
  source_options:
    range: '2020-01-01,2023-12-31'
    chunk_size: 30d  # 30-day chunks

streams:
  transactions:
    primary_key: [transaction_id]
  
  user_activity:
    source_options:
      chunk_size: 7d  # Smaller chunks for large table

env:
  SLING_THREADS: 2  # Conservative for historical loads
```

### Multi-Schema Replication

```yaml
source: ORACLE_ERP
target: REDSHIFT_DW

defaults:
  mode: incremental
  primary_key: [id]
  update_key: last_modified
  object: '{stream_schema}_data.{stream_table}'
  
  target_options:
    column_casing: lower
    table_keys:
      sort: [id, last_modified]

streams:
  # Finance schema
  finance.*:
    target_options:
      table_keys:
        sort: [account_id, transaction_date]
  
  # HR schema  
  hr.*:
    object: 'human_resources.{stream_table}'
  
  # Sensitive table exclusions
  hr.salaries:
    disabled: true
  finance.audit_log:
    disabled: true

env:
  SLING_THREADS: 8
```

---

## 13. Examples - File to Database

### CSV Import

```yaml
source: LOCAL
target: POSTGRES

defaults:
  mode: full-refresh
  object: 'staging.{stream_file_name}'
  
  source_options:
    format: csv
    header: true
    encoding: utf8
    empty_as_null: true
    skip_blank_lines: true

streams:
  'data/customers.csv':
    columns:
      customer_id: bigint
      email: string(255)
      created_at: datetime
  
  'data/products.csv':
    target_options:
      column_casing: snake
  
  # All CSV files in directory
  'imports/*.csv':

env:
  SLING_THREADS: 3
```

### JSON File Processing

```yaml
source: S3_BUCKET
target: SNOWFLAKE

defaults:
  mode: incremental
  object: 'json_data.{stream_file_name}'
  primary_key: [id]
  
  source_options:
    format: json
    flatten: true
    jmespath: 'data[*]'

streams:
  # API response files
  's3://bucket/api_responses/*.json':
    transforms:
      - id: 'cast(value, "string")'
        timestamp: 'date_parse(value, "2006-01-02T15:04:05Z")'
  
  # Nested JSON structure
  's3://bucket/complex_data.json':
    source_options:
      jmespath: 'results[*].{id: id, name: name, details: metadata}'

env:
  SLING_THREADS: 5
```

### Excel File Import

```yaml
source: LOCAL
target: MYSQL

defaults:
  mode: truncate
  object: 'excel_imports.{stream_file_name}'
  
  source_options:
    format: xlsx
    header: true

streams:
  'reports/monthly_sales.xlsx':
    source_options:
      sheet: 'Sales Data'
  
  'reports/inventory.xlsx':
    source_options:
      sheet: 'Current Stock!A1:H1000'
    columns:
      quantity: integer
      price: decimal(10,2)

env:
  SLING_THREADS: 2
```

### Incremental File Loading

```yaml
source: SFTP_SERVER
target: POSTGRES

defaults:
  mode: incremental
  object: 'file_data.{stream_file_name}'
  update_key: file_date
  primary_key: [record_id]
  
  source_options:
    format: csv
    header: true

streams:
  # Daily files with date in filename
  'daily_reports/sales_*.csv':
    transforms:
      - file_date: 'date_parse(record.filename_date, "2006-01-02")'
        record_id: 'record.id + "_" + record.filename_date'

env:
  SLING_STREAM_URL_COLUMN: true  # Track source file
  SLING_THREADS: 3
```

### Parquet File Processing

```yaml
source: AZURE_BLOB
target: BIGQUERY

defaults:
  mode: full-refresh
  object: 'parquet_data.{stream_file_name}'
  
  source_options:
    format: parquet
    compression: auto

streams:
  # Partitioned parquet files
  'data/year=*/month=*/*.parquet':
    transforms:
      - year: 'int_parse(substring(record._file_path, strpos(record._file_path, "year=") + 5, 4))'
        month: 'int_parse(substring(record._file_path, strpos(record._file_path, "month=") + 6, 2))'

env:
  SLING_THREADS: 10
```

---

## 14. Examples - Database to File

### CSV Export

```yaml
source: POSTGRES
target: LOCAL

defaults:
  mode: full-refresh
  object: 'exports/{stream_table}_{YYYY}_{MM}_{DD}.csv'
  
  target_options:
    format: csv
    header: true
    encoding: utf8

streams:
  public.customers:
    select: [id, name, email, created_at]  # Exclude sensitive data
  
  public.orders:
    where: "created_at >= '2024-01-01'"  # Filter data
  
  # Large table with file splitting
  public.transactions:
    target_options:
      file_max_rows: 100000  # Split into multiple files

env:
  SLING_THREADS: 4
```

### Parquet Export with Partitioning

```yaml
source: SNOWFLAKE
target: S3_BUCKET

defaults:
  mode: full-refresh
  update_key: created_date
  object: 's3://bucket/data/{stream_table}/{part_year}/{part_month}/'
  
  target_options:
    format: parquet
    compression: snappy
    file_max_rows: 1000000

streams:
  warehouse.sales:
    where: "created_date >= '2023-01-01'"
  
  warehouse.customers:
    # No partitioning
    object: 's3://bucket/reference/customers.parquet'

env:
  SLING_THREADS: 6
```

### JSON Export

```yaml
source: MONGODB
target: GCS

defaults:
  mode: full-refresh
  object: 'gs://bucket/exports/{stream_table}_{ISO8601}.jsonlines'
  
  target_options:
    format: jsonlines
    compression: gzip

streams:
  # Collections
  users:
  orders:
  products:

env:
  SLING_THREADS: 3
```

### Incremental Export

```yaml
source: MYSQL
target: AZURE_BLOB

defaults:
  mode: incremental
  update_key: updated_at
  object: 'exports/{stream_table}/year={part_year}/month={part_month}/'
  
  target_options:
    format: parquet
    file_max_rows: 500000

streams:
  app.user_activities:
    where: "updated_at >= '2024-01-01'"
  
  app.transactions:
    primary_key: [transaction_id]

env:
  SLING_THREADS: 4
```

---

## 15. Advanced Topics

### Parallel Processing

Control concurrency with `SLING_THREADS`:

```yaml
env:
  SLING_THREADS: 10  # Process up to 10 streams simultaneously
```

**Guidelines:**
- Database sources: 5-15 threads depending on connection capacity
- File sources: 10-50 threads depending on I/O capacity. Great for Cloud Storage.
- Consider target database connection limits

### Error Handling and Retries

```yaml
env:
  SLING_RETRIES: 3           # Retry failed streams 3 times
  SLING_RETRY_DELAY: 30s     # Wait 30 seconds between retries
```

### Performance Optimization

#### Direct Insert Mode
Skip temporary tables for append-only scenarios:

```yaml
defaults:
  target_options:
    direct_insert: true  # Only works with certain modes
```

#### Bulk Loading
Use external tools when available:

```yaml
defaults:
  target_options:
    use_bulk: true  # Uses bcp, sqlldr, mysql client
```

#### Chunking Large Tables
Split large tables for parallel processing:

```yaml
streams:
  large_table:
    source_options:
      chunk_count: 20  # Split into 20 chunks
      # OR
      chunk_size: 6h   # Split by time intervals
```

### Advanced Transformations

Complex business logic with staged transforms:

```yaml
streams:
  customer_analytics:
    transforms:
      # Stage 1: Data cleansing
      - email: 'lower(trim_space(value))'
        phone: 'replace(value, "[^0-9+]", "")'
        name: 'trim_space(value)'
      
      # Stage 2: Computed metrics
      - days_since_signup: 'date_diff(now(), record.created_at, "day")'
        lifetime_value: 'coalesce(record.total_spent, 0) * 1.2'
      
      # Stage 3: Segmentation
      - customer_segment: |
          record.lifetime_value >= 10000 ? "enterprise" : (
            record.lifetime_value >= 1000 ? "professional" : (
              record.days_since_signup <= 30 ? "new" : "standard"
            )
          )
      
      # Stage 4: Risk scoring
      - risk_score: |
          (record.failed_payments * 0.3) + 
          (record.days_since_last_login * 0.1) + 
          (record.support_tickets * 0.2)
```

---

## 16. MCP Tool Integration

### Using Replication Commands

#### Get Documentation
```json
{
  "action": "docs",
  "input": {}
}
```

#### Parse Configuration
```json
{
  "action": "parse",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "working_dir": "/optional/work/dir"
  }
}
```

#### Validate Configuration
```json
{
  "action": "compile",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "select_streams": ["table1", "table2"],  // Optional
    "working_dir": "/optional/work/dir"
  }
}
```

#### Execute Replication
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "select_streams": ["specific_table"],     // Optional: run specific streams
    "working_dir": "/project/directory",      // Optional: change directory
    "range": "2024-01-01,2024-01-31",       // Optional: backfill range
    "mode": "incremental",                    // Optional: override mode
    "env": {                                  // Optional: environment variables
      "CUSTOM_VAR": "value",
      "SLING_THREADS": "5"
    }
  }
}
```

### Combining with Connection Tools

First check connections:
```json
{
  "action": "list",
  "input": {}
}
```

Then test connections:
```json
{
  "action": "test",
  "input": {
    "connection": "MY_SOURCE_DB",
    "debug": true
  }
}
```

Finally run replication:
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml"
  }
}
```

### Development Workflow

1. **Parse** configuration for syntax validation
2. **Compile** to check connections and streams
3. **Test** on subset of streams first
4. **Run** full replication
5. **Monitor** with debug/trace options

---

## 17. Best Practices

### Configuration Organization

#### Use Meaningful Names
```yaml
# Good
source: prod_postgres_crm
target: dw_snowflake_analytics

# Avoid
source: db1
target: db2
```

#### Structure with Defaults
```yaml
# Define common patterns in defaults
defaults:
  mode: incremental
  object: 'warehouse.{stream_schema}_{stream_table}'
  primary_key: [id]
  update_key: updated_at
  
  target_options:
    column_casing: snake
    add_new_columns: true

# Override only when necessary
streams:
  public.users:           # Uses all defaults
  
  public.logs:           # Override for specific needs
    mode: full-refresh
    primary_key: []
```

#### Organize by Purpose
```yaml
# Group related streams
streams:
  # Core business data
  public.customers:
  public.orders:
  public.products:
  
  # Analytics data
  analytics.user_metrics:
  analytics.sales_summary:
  
  # Logging data
  logs.application_logs:
    mode: full-refresh
```

### Performance Best Practices

#### Optimize Thread Usage
- Start with 5 threads and adjust based on performance
- Database sources: Monitor connection pool limits
- File sources: Consider I/O capacity

#### Use Appropriate Modes
- `full-refresh`: Small, frequently changing tables
- `incremental`: Large tables with clear update patterns
- `truncate`: When you need to preserve table structure

#### Leverage Chunking
```yaml
streams:
  large_table:
    source_options:
      chunk_size: 12h  # For time-based data
      # OR
      chunk_count: 10  # For evenly distributed data
```

### Security Considerations

#### Sensitive Data Handling
```yaml
streams:
  users:
    select: [-password, -ssn, -credit_card]  # Exclude sensitive columns
    transforms:
      - email: 'hash(value, "sha256")'       # Hash PII
```

#### Connection Security
- Store connection strings in environment variables
- Use secure authentication methods
- Rotate credentials regularly

#### Data Encryption
```yaml
target_options:
  encryption: true        # Enable if supported by target
  compression: gzip       # Compress sensitive data
```

### Monitoring and Observability

#### Add Metadata Columns
```yaml
env:
  SLING_LOADED_AT_COLUMN: true    # Track load timestamps
  SLING_STREAM_URL_COLUMN: true   # Track source file paths (for file sources)
```

#### Use Hooks for Monitoring
```yaml
hooks:
  start:
    - type: http
      url: 'https://monitoring.company.com/api/jobs/start'
      body: '{"job": "data_replication", "status": "started"}'
  
  end:
    - type: http
      url: 'https://monitoring.company.com/api/jobs/complete'
      body: '{"job": "data_replication", "status": "completed"}'
```

### Testing Strategies

#### Test with Subsets First
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "select_streams": ["small_test_table"]
  }
}
```

#### Use Development Targets
```yaml
# Development configuration
target: DEV_DATABASE

defaults:
  object: 'dev_{stream_table}'
```

#### Validate Data Quality
```yaml
streams:
  critical_table:
    hooks:
      post:
        - type: query
          connection: TARGET_DB
          sql: |
            SELECT 
              COUNT(*) as row_count,
              COUNT(DISTINCT id) as unique_ids
            FROM {object_name}
            HAVING COUNT(*) < 1000  -- Fail if too few rows
```

---

## 18. Troubleshooting

### Common Issues and Solutions

#### Connection Errors
```
Error: failed to connect to source database
```

**Solutions:**
1. Test connection first:
```json
{
  "action": "test",
  "input": {
    "connection": "MY_SOURCE_DB",
    "debug": true
  }
}
```

2. Check connection string format
3. Verify network access and firewall rules
4. Confirm authentication credentials

#### Schema/Table Not Found
```
Error: table "schema.table" does not exist
```

**Solutions:**
1. List available streams:
```json
{
  "action": "discover",
  "input": {
    "connection": "MY_SOURCE_DB",
    "pattern": "schema.*"
  }
}
```

2. Check case sensitivity
3. Verify permissions

#### Type Conversion Errors
```
Error: cannot convert value to target type
```

**Solutions:**
1. Add explicit type casting:
```yaml
columns:
  problematic_column: string  # Force to string first
  
transforms:
  - problematic_column: 'cast(value, "integer")'  # Then convert
```

2. Handle null values:
```yaml
transforms:
  - amount: 'coalesce(value, 0)'
```

#### Memory Issues
```
Error: out of memory processing large dataset
```

**Solutions:**
1. Reduce batch size:
```yaml
env:
  SLING_BATCH_SIZE: 10000
```

2. Enable chunking:
```yaml
streams:
  large_table:
    source_options:
      chunk_size: 6h
```

3. Use file splitting:
```yaml
target_options:
  file_max_rows: 100000
```

### Debug Options

#### Enable Debug Logging
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "env": {
      "DEBUG": "true"
    }
  }
}
```

#### Parse Configuration
Always validate before running:
```json
{
  "action": "parse",
  "input": {
    "file_path": "/path/to/replication.yaml"
  }
}
```

#### Compile Configuration
Check connections and streams:
```json
{
  "action": "compile",
  "input": {
    "file_path": "/path/to/replication.yaml"
  }
}
```

### Performance Issues

#### Slow Performance
1. **Check thread usage:**
```yaml
env:
  SLING_THREADS: 10  # Increase if system can handle
```

2. **Enable bulk loading:**
```yaml
target_options:
  use_bulk: true
```

3. **Use direct insert for append-only:**
```yaml
target_options:
  direct_insert: true  # Skip temporary tables (if not incremental or backfill)
```

#### High Memory Usage
1. **Reduce batch sizes:**
```yaml
env:
  SLING_BATCH_SIZE: 5000
```

2. **Use file-based temporary storage:**
```yaml
target_options:
  file_max_rows: 50000
```

### Data Quality Issues

#### Missing Data
1. **Check incremental logic:**
```yaml
streams:
  my_table:
    sql: |
      SELECT * FROM my_table 
      WHERE updated_at > coalesce({incremental_value}, '1900-01-01')
```

2. **Verify primary/update keys:**
```yaml
streams:
  my_table:
    primary_key: [id]      # Ensure exists and is unique
    update_key: updated_at  # Ensure has values
```

#### Duplicate Data
1. **Check primary key configuration:**
```yaml
streams:
  my_table:
    primary_key: [id, version]  # Use composite if needed
```

2. **Use appropriate mode:**
```yaml
streams:
  my_table:
    mode: incremental  # Not full-refresh for updates
```

### Error Recovery

#### Resume Failed Replications
Sling automatically tracks progress, but you can:

1. **Run specific streams:**
```json
{
  "action": "run",
  "input": {
    "file_path": "/path/to/replication.yaml",
    "select_streams": ["failed_stream"]
  }
}
```

2. **Use retry configuration:**
```yaml
env:
  SLING_RETRIES: 3
  SLING_RETRY_DELAY: 60s
```

This comprehensive guide provides everything needed to effectively use Sling replications with the MCP tool, from basic concepts to advanced troubleshooting techniques.