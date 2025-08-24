# Sling Database Connection MCP Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [MCP Database Tool Integration](#2-mcp-database-tool-integration)
3. [Database Operations](#3-database-operations)
4. [Schema and Metadata Operations](#4-schema-and-metadata-operations)
5. [Query Operations](#5-query-operations)
6. [Common Usage Patterns](#6-common-usage-patterns)
7. [Database-Specific Examples](#7-database-specific-examples)
8. [Best Practices](#8-best-practices)
9. [Troubleshooting Database Operations](#9-troubleshooting-database-operations)

---

## 1. Introduction and Overview

### What are Database Operations?

Database operations in Sling provide comprehensive functionality for interacting with database connections, including schema exploration, metadata retrieval, and read-only query execution. These operations work with all supported database types through a unified interface.

### Key Capabilities

- **Schema Discovery**: Explore database structure at schema, table, and column levels
- **Metadata Retrieval**: Get detailed information about tables, columns, and data types
- **Query Execution**: Run read-only SQL queries with results in structured format
- **Cross-Database Support**: Works with 24+ database types with consistent interface
- **Safety**: Built-in protection against destructive operations

### Supported Database Types

- **Relational**: PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite
- **Cloud Warehouses**: Snowflake, BigQuery, Redshift, Databricks
- **Analytics**: ClickHouse, DuckDB, MotherDuck, StarRocks, Trino, Proton
- **NoSQL**: MongoDB, ElasticSearch, Prometheus
- **Specialized**: Azure Table, BigTable, Exasol, D1

---

## 2. MCP Database Tool Integration

The `database` tool provides database-specific operations through various actions. All operations require an existing database connection to be configured.

### Available Actions

- `docs` - Get comprehensive database documentation
- `get_schemata` - Get database structure (schema/table/column level)
- `get_schemas` - Get list of schema names
- `get_columns` - Get column metadata for a table
- `query` - Execute read-only SQL queries

### Basic Tool Usage

```json
{
  "action": "action_name",
  "input": {
    "connection": "DATABASE_CONNECTION_NAME",
    "parameter1": "value1",
    "parameter2": "value2"
  }
}
```

### Getting Documentation

```json
{
  "action": "docs",
  "input": {}
}
```

**Important**: Database operations require a Pro token and are rate-limited.

---

## 3. Database Operations

### Prerequisites

Before using database operations:

1. **Connection Setup**: Database connection must be configured using the `connection` tool
2. **Connection Testing**: Verify connection works with `connection` tool `test` action
3. **Permissions**: Database user must have SELECT permissions on target objects
4. **Token**: Operations require a valid Sling CLI Pro token

### Operation Categories

#### Schema and Structure Operations
- `get_schemata` - Multi-level schema exploration
- `get_schemas` - Simple schema list
- `get_columns` - Detailed column information

#### Data Access Operations
- `query` - Read-only SQL execution

---

## 4. Schema and Metadata Operations

### Getting Database Schemata

Retrieve database structure at different levels of detail:

```json
{
  "action": "get_schemata",
  "input": {
    "connection": "MY_POSTGRES",
    "level": "table",
    "schema_name": "public",
    "table_names": ["users", "orders"]
  }
}
```

**Parameters:**
- `connection` (required) - Database connection name
- `level` (required) - Level of detail: `schema`, `table`, or `column`
- `schema_name` (optional) - Specific schema (required for table/column levels if not fetching all schemas)
- `table_names` (optional) - Specific tables to focus on (relevant for table/column levels)

**Levels Explained:**

#### Schema Level
Lists all schemas/databases in the connection:
```json
{
  "action": "get_schemata",
  "input": {
    "connection": "MY_POSTGRES",
    "level": "schema"
  }
}
```

#### Table Level
Lists tables in specified schema(s):
```json
{
  "action": "get_schemata",
  "input": {
    "connection": "MY_POSTGRES",
    "level": "table",
    "schema_name": "public"
  }
}
```

#### Column Level
Lists columns in specified tables:
```json
{
  "action": "get_schemata",
  "input": {
    "connection": "MY_POSTGRES",
    "level": "column",
    "schema_name": "public",
    "table_names": ["users", "orders"]
  }
}
```

### Getting Schema Names

Get a simple list of schema names:

```json
{
  "action": "get_schemas",
  "input": {
    "connection": "MY_POSTGRES"
  }
}
```

**Parameters:**
- `connection` (required) - Database connection name

**Returns:** Array of schema names available in the database.

### Getting Column Metadata

Get detailed column information for a specific table:

```json
{
  "action": "get_columns",
  "input": {
    "connection": "MY_POSTGRES",
    "table_name": "public.users"
  }
}
```

**Parameters:**
- `connection` (required) - Database connection name
- `table_name` (required) - Fully qualified table name (e.g., `schema.table` or just `table`)

**Returns:** Detailed column metadata including:
- Column names
- Data types
- Nullability
- Default values
- Position/order
- Constraints

---

## 5. Query Operations

### Executing SQL Queries

Run read-only SQL queries on database connections:

```json
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT * FROM public.users LIMIT 10",
    "limit": 100,
    "transient": false
  }
}
```

**Parameters:**
- `connection` (required) - Database connection name
- `query` (required) - SQL query to execute
- `limit` (optional) - Maximum rows to return (default: 100)
- `transient` (optional) - Use transient connection (default: false)

### Query Safety and Restrictions

**WARNING: Only use this tool for SELECT queries and other read-only operations. Never execute destructive queries such as DELETE, DROP, TRUNCATE, ALTER, UPDATE, INSERT, or any other data modification operations.**

#### Allowed Operations
- `SELECT` statements
- `SHOW` statements
- `DESCRIBE` or `DESC` statements
- `EXPLAIN` statements
- Views and CTEs (Common Table Expressions)
- Read-only functions and procedures

#### Prohibited Operations
- `DELETE` - Data deletion
- `DROP` - Object deletion
- `TRUNCATE` - Table truncation
- `ALTER` - Schema modifications
- `UPDATE` - Data modification
- `INSERT` - Data insertion
- `CREATE` - Object creation
- `GRANT`/`REVOKE` - Permission changes

#### If Destructive Operations Are Needed
If a destructive operation is deemed necessary:
1. **DO NOT execute it directly**
2. Formulate the required SQL statement
3. **Return it to the USER for manual review and execution**

### Query Best Practices

1. **Always use LIMIT clause** to minimize data retrieval
2. **Select only necessary columns** to reduce computation and data transfer
3. **Use appropriate WHERE clauses** to filter data efficiently
4. **Test queries with small limits** before increasing data volume
5. **Consider query performance** and database load

### Example Queries

#### Basic Data Exploration
```json
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT * FROM public.users LIMIT 5",
    "limit": 5
  }
}
```

#### Analytical Query
```json
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT status, COUNT(*) as count FROM orders WHERE created_date >= '2024-01-01' GROUP BY status ORDER BY count DESC LIMIT 20",
    "limit": 20
  }
}
```

#### Metadata Query
```json
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' LIMIT 50",
    "limit": 50
  }
}
```

#### Using Transient Connection
```json
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT version()",
    "transient": true
  }
}
```

---

## 6. Common Usage Patterns

### Database Exploration Workflow

1. **List Schemas**
```json
{
  "action": "get_schemas",
  "input": {
    "connection": "MY_DB"
  }
}
```

2. **Explore Tables in Schema**
```json
{
  "action": "get_schemata",
  "input": {
    "connection": "MY_DB",
    "level": "table",
    "schema_name": "production"
  }
}
```

3. **Examine Table Structure**
```json
{
  "action": "get_columns",
  "input": {
    "connection": "MY_DB",
    "table_name": "production.users"
  }
}
```

4. **Sample Data**
```json
{
  "action": "query",
  "input": {
    "connection": "MY_DB",
    "query": "SELECT * FROM production.users LIMIT 3"
  }
}
```

### Data Analysis Workflow

1. **Understand Data Volume**
```json
{
  "action": "query",
  "input": {
    "connection": "MY_DB",
    "query": "SELECT COUNT(*) as total_records FROM sales.orders"
  }
}
```

2. **Explore Data Distribution**
```json
{
  "action": "query",
  "input": {
    "connection": "MY_DB",
    "query": "SELECT status, COUNT(*) as count FROM sales.orders GROUP BY status ORDER BY count DESC LIMIT 10"
  }
}
```

3. **Check Data Quality**
```json
{
  "action": "query",
  "input": {
    "connection": "MY_DB",
    "query": "SELECT COUNT(*) as nulls FROM sales.orders WHERE customer_id IS NULL"
  }
}
```

### Cross-Database Pattern

When working with multiple databases:

```json
// Explore source database
{
  "action": "get_schemata",
  "input": {
    "connection": "SOURCE_DB",
    "level": "table",
    "schema_name": "public"
  }
}

// Check target database structure
{
  "action": "get_schemata",
  "input": {
    "connection": "TARGET_DW",
    "level": "schema"
  }
}

// Compare data samples
{
  "action": "query",
  "input": {
    "connection": "SOURCE_DB",
    "query": "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM public.users"
  }
}
```

---

## 7. Database-Specific Examples

### PostgreSQL

```json
// Get table sizes
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size FROM pg_tables WHERE schemaname = 'public' ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC LIMIT 10"
  }
}

// Check indexes
{
  "action": "query",
  "input": {
    "connection": "MY_POSTGRES",
    "query": "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'users' LIMIT 20"
  }
}
```

### MySQL

```json
// Get database size
{
  "action": "query",
  "input": {
    "connection": "MY_MYSQL",
    "query": "SELECT table_schema, ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'DB Size in MB' FROM information_schema.tables GROUP BY table_schema"
  }
}
```

### Snowflake

```json
// Get warehouse info
{
  "action": "query",
  "input": {
    "connection": "MY_SNOWFLAKE",
    "query": "SHOW WAREHOUSES"
  }
}

// Check table clustering
{
  "action": "query",
  "input": {
    "connection": "MY_SNOWFLAKE",
    "query": "SELECT * FROM information_schema.tables WHERE table_schema = 'PUBLIC' LIMIT 10"
  }
}
```

### BigQuery

```json
// Get dataset info
{
  "action": "query",
  "input": {
    "connection": "MY_BIGQUERY",
    "query": "SELECT schema_name, catalog_name FROM INFORMATION_SCHEMA.SCHEMATA LIMIT 20"
  }
}

// Check table partitioning
{
  "action": "query",
  "input": {
    "connection": "MY_BIGQUERY",
    "query": "SELECT table_name, partition_id, total_rows FROM `project.dataset.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = 'my_table' LIMIT 10"
  }
}
```

### ClickHouse

```json
// Get system information
{
  "action": "query",
  "input": {
    "connection": "MY_CLICKHOUSE",
    "query": "SELECT * FROM system.databases LIMIT 20"
  }
}

// Check table engines
{
  "action": "query",
  "input": {
    "connection": "MY_CLICKHOUSE",
    "query": "SELECT database, name, engine, total_rows FROM system.tables WHERE database = 'default' LIMIT 20"
  }
}
```

---

## 8. Best Practices

### Query Performance

1. **Use LIMIT Always**: Include LIMIT clause to prevent large data transfers
2. **Select Specific Columns**: Avoid SELECT * for performance
3. **Filter Early**: Use WHERE clauses to reduce data processing
4. **Index Awareness**: Consider query execution plans for complex queries
5. **Connection Reuse**: Avoid transient connections for multiple queries

### Security and Safety

1. **Read-Only Principle**: Never execute modifying operations
2. **Permission Verification**: Ensure database user has minimal necessary permissions
3. **Query Review**: Review complex queries before execution
4. **Sensitive Data**: Be careful with personally identifiable information (PII)
5. **Connection Limits**: Be mindful of database connection limits

### Error Handling

1. **Start Simple**: Begin with basic queries and add complexity
2. **Syntax Validation**: Understand database-specific SQL dialects
3. **Connection Testing**: Verify connection works before complex operations
4. **Graceful Degradation**: Have fallback approaches for failed operations

### Resource Management

1. **Query Timeouts**: Be aware of database query timeout settings
2. **Result Size Limits**: Consider memory constraints for large result sets
3. **Concurrent Operations**: Avoid overwhelming database with simultaneous queries
4. **Connection Pooling**: Understand connection pool implications

---

## 9. Troubleshooting Database Operations

### Common Query Errors

**Syntax Errors**
- Verify SQL syntax for specific database dialect
- Check table and column name spelling
- Ensure proper quoting of identifiers

**Permission Errors**
- Verify database user has SELECT permissions
- Check schema access permissions
- Confirm connection user credentials

**Connection Errors**
- Test basic connection with `connection` tool
- Verify database is accessible and running
- Check network connectivity and firewall settings

### Schema Discovery Issues

**No Schemas Returned**
- Check database user permissions
- Verify connection is to correct database instance
- Some databases may not support INFORMATION_SCHEMA

**Missing Tables or Columns**
- Confirm user has access to target objects
- Check if objects exist in specified schema
- Verify case sensitivity requirements

**Slow Schema Operations**
- Large databases may take time for full discovery
- Consider filtering with table_names parameter
- Use more specific schema_name values

### Performance Issues

**Slow Queries**
- Add or reduce LIMIT clause
- Simplify WHERE conditions
- Check database performance and load
- Consider query optimization

**Timeout Errors**
- Reduce query complexity or data volume
- Check database timeout settings
- Consider breaking large operations into smaller parts

**Memory Issues**
- Reduce LIMIT parameter
- Select fewer columns
- Filter data more aggressively

### Connection-Specific Issues

**PostgreSQL**
- Check `pg_hba.conf` for access control
- Verify SSL settings if required
- Consider connection pooling settings

**MySQL**
- Verify user has proper host permissions
- Check MySQL timeout settings
- Consider max_connections limit

**Snowflake**
- Ensure warehouse is running
- Check role permissions and privileges
- Verify account identifier is correct

**BigQuery**
- Confirm project ID and dataset access
- Check service account permissions
- Verify billing is enabled for project

### Getting Help

When troubleshooting database operations:

1. **Use Connection Test**: Verify basic connectivity with `connection` tool
2. **Enable Debug Logging**: Use debug flags in connection testing
3. **Start Simple**: Begin with basic queries and build complexity
4. **Check Documentation**: Reference database-specific documentation
5. **Verify Permissions**: Ensure appropriate database access rights

### Error Message Patterns

**"could not init database connection"**
- Basic connection failure
- Check connection configuration and credentials
- Verify database server is accessible

**"permission denied"**
- Insufficient database permissions
- Grant appropriate SELECT permissions to database user
- Check schema and table access rights

**"table or view does not exist"**
- Object not found or access denied
- Verify object names and schema
- Check if user has visibility to objects

**"connection refused"**
- Network or server connectivity issue
- Verify host, port, and network access
- Check if database service is running

---

This comprehensive guide covers all aspects of database operations using the Sling MCP `database` tool. All operations are designed to be safe, read-only, and provide structured access to database resources across multiple database platforms.