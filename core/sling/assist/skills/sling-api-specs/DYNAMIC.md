# Dynamic Endpoints

Generate endpoints at runtime from discovery/catalog APIs. Useful when endpoints aren't known ahead of time.

## When to Use

- API has a catalog/discovery endpoint listing available resources
- Schema or table names are dynamic
- Resources are user-created and vary between accounts

## Basic Structure

```yaml
dynamic_endpoints:
  - setup:
      # Fetch list of resources
      - request:
          url: "{state.base_url}/catalog"
        response:
          processors:
            - expression: 'jmespath(response.json, "tables")'
              output: "state.table_list"

    iterate: "state.table_list"
    into: "state.current_table"

    endpoint:
      name: "{state.current_table.name}"
      description: "Table: {state.current_table.description}"
      state:
        table_id: "{state.current_table.id}"
      request:
        url: "{state.base_url}/data/{state.table_id}"
      response:
        records:
          jmespath: "rows[]"
          primary_key: ["id"]
```

## Execution Order

1. Authentication runs first
2. Dynamic endpoint setup sequences execute
3. Endpoints are generated from iteration
4. Static endpoints execute
5. Generated endpoints execute (respecting dependencies)

## Example: Discover Tables

```yaml
name: "Dynamic Tables API"

authentication:
  type: "static"
  headers:
    Authorization: "Bearer {secrets.api_token}"

defaults:
  state:
    base_url: "https://api.example.com/v1"

dynamic_endpoints:
  - setup:
      - request:
          url: "{state.base_url}/schemas"
        response:
          processors:
            - expression: 'response.json.schemas'
              output: "state.schemas"

    iterate: "state.schemas"
    into: "state.current_schema"

    endpoint:
      name: "schema_{state.current_schema.name}"
      description: "Data from {state.current_schema.name}"
      state:
        schema_id: "{state.current_schema.id}"
        schema_name: "{state.current_schema.name}"
      request:
        url: "{state.base_url}/schemas/{state.schema_id}/data"
        parameters:
          limit: 1000
      response:
        records:
          jmespath: "records[]"
          primary_key: "{state.current_schema.primary_key}"
```

## Static Iteration

Iterate over a static list without setup:

```yaml
dynamic_endpoints:
  - iterate: '["sales", "inventory", "customers"]'
    into: "state.report_type"

    endpoint:
      name: "report_{state.report_type}"
      request:
        url: "{state.base_url}/reports/{state.report_type}"
      response:
        records:
          jmespath: "data[]"
```

## Multiple Dynamic Endpoint Groups

```yaml
dynamic_endpoints:
  # Group 1: User tables
  - setup:
      - request:
          url: "{state.base_url}/user-tables"
        response:
          processors:
            - expression: 'response.json.tables'
              output: "state.user_tables"

    iterate: "state.user_tables"
    into: "state.table"

    endpoint:
      name: "user_{state.table.name}"
      request:
        url: "{state.base_url}/user-data/{state.table.id}"

  # Group 2: System tables
  - setup:
      - request:
          url: "{state.base_url}/system-tables"
        response:
          processors:
            - expression: 'response.json.tables'
              output: "state.system_tables"

    iterate: "state.system_tables"
    into: "state.table"

    endpoint:
      name: "system_{state.table.name}"
      request:
        url: "{state.base_url}/system-data/{state.table.id}"
```

## Endpoint Template Fields

All endpoint fields support template expressions:

| Field | Template Support |
|-------|------------------|
| `name` | Yes |
| `description` | Yes |
| `state.*` | Yes |
| `request.*` | Yes |
| `response.records.primary_key` | Yes |
| All other fields | Yes |

## Dynamic Primary Key

Set primary key based on discovered schema:

```yaml
endpoint:
  name: "{state.table.name}"
  response:
    records:
      jmespath: "data[]"
      primary_key: "{state.table.key_columns}"  # Array from discovery
```

## Naming Requirements

- Generated endpoint names must be unique
- Use prefixes to avoid collisions: `table_`, `schema_`, `report_`
- Names can include state variables: `"{state.prefix}_{state.name}"`

## Filtering Discovered Items

Filter in the setup step before generating endpoints:

```yaml
dynamic_endpoints:
  - setup:
      - request:
          url: "{state.base_url}/tables"
        response:
          processors:
            # Filter to only include active tables
            - expression: 'filter(response.json.tables, "value.status == \"active\"")'
              output: "state.tables"

    iterate: "state.tables"
    into: "state.table"

    endpoint:
      name: "{state.table.name}"
```

## Combining with Static Endpoints

Dynamic and static endpoints work together:

```yaml
dynamic_endpoints:
  - iterate: "state.discovered_tables"
    into: "state.table"
    endpoint:
      name: "table_{state.table.name}"
      # ...

endpoints:
  # Static endpoints
  metadata:
    request:
      url: "{state.base_url}/metadata"

  summary:
    depends_on: ["table_*"]  # Wait for all dynamic endpoints
    request:
      url: "{state.base_url}/summary"
```

## Tips

- Keep setup sequences fast - they run before any data extraction
- Use descriptive prefixes for generated endpoint names
- Log discovered items during development with `log()` function
- Test with a single item first before full discovery

## Related Topics

- [ENDPOINTS.md](ENDPOINTS.md) - Endpoint configuration
- [VARIABLES.md](VARIABLES.md) - State variable access in templates
- [FUNCTIONS.md](FUNCTIONS.md) - `filter()`, `jmespath()` functions
