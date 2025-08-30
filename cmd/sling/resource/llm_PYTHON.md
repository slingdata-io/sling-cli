# Sling + Python Guide for LLMs

## Overview
Sling is a powerful data movement and transformation tool that provides Python bindings for seamless integration. The Python package (`sling`) wraps the Sling CLI to enable efficient data transfers between databases, files, and Python data structures.

## Installation

```bash
# Basic installation
pip install sling

# With Apache Arrow support for high-performance streaming
pip install sling[arrow]
```

## Core Classes

### 1. Sling Class - Primary Interface
The `Sling` class mirrors all CLI functionalities and provides the most flexible interface.

```python
from sling import Sling, Mode, Format, Compression
from sling.options import SourceOptions, TargetOptions
```

#### Key Parameters:
- **Source**: `src_conn`, `src_stream`, `src_options`
- **Target**: `tgt_conn`, `tgt_object`, `tgt_options`
- **Transforms**: `select`, `where`, `transforms`, `columns`
- **Mode**: `mode` (full-refresh, incremental, truncate, snapshot, backfill)
- **Limits**: `limit`, `offset`, `range`
- **Keys**: `primary_key`, `update_key`
- **Config**: `env`, `debug`, `trace`
- **Python-specific**: `input` (data input from Python)

### 2. Replication Class
For managing multi-stream data replications.

```python
from sling import Replication, ReplicationStream
```

### 3. Pipeline Class
For executing sequential data processing steps.

```python
from sling import Pipeline, Hook
```

## Common Usage Patterns

### 1. Basic Data Transfer
```python
# Database to database
sling = Sling(
    src_conn="postgres",
    src_stream="public.users",
    tgt_conn="snowflake",
    tgt_object="analytics.users",
    mode="full-refresh"
)
sling.run()

# File to database
sling = Sling(
    src_stream="file:///path/to/data.csv",
    tgt_conn="postgres",
    tgt_object="public.imported_data"
)
sling.run()

# SQL query as source
sling = Sling(
    src_conn="mysql",
    src_stream="select * from orders where created_at > '2024-01-01'",
    tgt_conn="snowflake",
    tgt_object="staging.recent_orders"
)
sling.run()
```

### 2. Python Data Input
```python
# From list of dictionaries
data = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25}
]
sling = Sling(
    input=data,
    tgt_conn="postgres",
    tgt_object="public.users"
)
sling.run()

# From pandas DataFrame
import pandas as pd
df = pd.DataFrame(data)
sling = Sling(
    input=df,
    tgt_conn="snowflake",
    tgt_object="public.users"
)
sling.run()

# From polars DataFrame
import polars as pl
df = pl.DataFrame(data)
sling = Sling(
    input=df,
    tgt_conn="duckdb",
    tgt_object="users"
)
sling.run()
```

### 3. Streaming Output
```python
# Stream data as Python dictionaries
sling = Sling(
    src_conn="postgres",
    src_stream="select * from large_table"
)
for record in sling.stream():
    # Process each record
    print(record)
    
# High-performance Arrow streaming
reader = sling.stream_arrow()
for batch in reader:
    # Process Arrow RecordBatch
    df = batch.to_pandas()
    process_batch(df)
```

### 4. Data Transformation
```python
# Column selection
sling = Sling(
    src_conn="postgres",
    src_stream="users",
    select=["id", "name", "email"],
    tgt_conn="snowflake",
    tgt_object="users_subset"
)

# Filtering with WHERE clause
sling = Sling(
    src_conn="mysql",
    src_stream="orders",
    where="status = 'completed' and amount > 100",
    tgt_conn="postgres",
    tgt_object="completed_orders"
)

# Column transformations
transforms = [{
    "email": "lower(value)",
    "full_name": "concat(record.first_name, ' ', record.last_name)",
    "created_date": "date(record.created_at)"
}]
sling = Sling(
    src_conn="postgres",
    src_stream="users",
    transforms=transforms,
    tgt_conn="snowflake",
    tgt_object="transformed_users"
)

# Column type casting
columns = {
    "id": "bigint",
    "price": "decimal(10,2)",
    "created_at": "timestamp"
}
sling = Sling(
    src_stream="file://data.csv",
    columns=columns,
    tgt_conn="postgres",
    tgt_object="products"
)
```

### 5. Advanced Options
```python
# Source options
src_opts = SourceOptions(
    format=Format.CSV,
    delimiter="|",
    header=True,
    null_if="NULL",
)

# Target options
tgt_opts = TargetOptions(
    format=Format.PARQUET,
    compression=Compression.ZSTD,
    file_max_rows=100000,
    column_casing="snake"
)

sling = Sling(
    src_stream="file://input.csv",
    src_options=src_opts,
    tgt_object="file://output.parquet",
    tgt_options=tgt_opts
)
sling.run()
```

### 6. Incremental Loading
```python
# Using primary key for incremental updates
sling = Sling(
    src_conn="postgres",
    src_stream="events",
    tgt_conn="snowflake",
    tgt_object="events",
    mode="incremental",
    primary_key=["event_id"],
    update_key="updated_at"
)
sling.run()
```

### 7. Replication Configuration
```python
from sling import Replication, ReplicationStream

# Create replication with multiple streams
replication = Replication(
    source="postgres",
    target="snowflake",
    streams={
        "public.users": ReplicationStream(
            mode="full-refresh",
            object="analytics.users"
        ),
        "public.orders": ReplicationStream(
            mode="incremental",
            object="analytics.orders",
            primary_key=["order_id"],
            update_key="modified_at"
        )
    },
    debug=True
)
replication.run()

# Load from YAML file
replication = Replication(file_path="replication.yaml")
replication.run()
```

## Environment Variables
```python
# Set connection environment variables
import os

os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost:5432/db"
os.environ["SNOWFLAKE_URL"] = "snowflake://user:pass@account/database/schema"

# Use connection names in Sling
sling = Sling(
    src_conn="POSTGRES",
    src_stream="users",
    tgt_conn="SNOWFLAKE",
    tgt_object="users"
)

# Pass environment variables to Sling
env = {
    "SLING_LOADED_AT_COLUMN": "timestamp",
    "SLING_STREAM_URL_COLUMN": "true"
}
sling = Sling(
    src_conn="postgres",
    src_stream="events",
    tgt_conn="snowflake",
    tgt_object="events",
    env=env
)
```

## Debugging

```python
# Enable debug output
sling = Sling(
    src_conn="postgres",
    src_stream="users",
    tgt_conn="snowflake",
    tgt_object="users",
    debug=True  # Shows detailed execution info
)

# Enable trace-level debugging
sling = Sling(
    src_conn="postgres",
    src_stream="users",
    tgt_conn="snowflake",
    tgt_object="users",
    trace=True  # Shows even more detailed info
)
```

## Additional Resources
- Documentation: https://docs.slingdata.io
- GitHub: https://github.com/slingdata-io/sling-python
- Examples: https://docs.slingdata.io/examples/sling-python