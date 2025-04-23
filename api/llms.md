# Sling API Specification Guide for LLMs

This guide explains how to create API specifications for Sling. Follow this document along with the target API's official documentation to build a properly formatted API spec.

## Overview

Sling API specs are YAML files that define how to interact with REST APIs. They specify authentication, endpoints, request formation, pagination, and response processing.

## Basic Structure

```yaml
name: "API Name"
description: "Description of the API"
authentication:
  type: "bearer|basic|oauth2"
  # Authentication details...
defaults:
  # Default settings for all endpoints
endpoints:
  - name: "endpoint_name"
    description: "Endpoint description"d
    request:
      url: "https://api.example.com/resource"
      method: "GET"
    response:
      records:
        jmespath: "data[]"
```

## Full Structure

```yaml
# Name of the API
name: "Example API"

# Description of what the API does
description: "This API provides access to example data"

# Authentication configuration for accessing the API
authentication:
  # Type of authentication: "bearer", "basic", "oauth2", or empty for none
  type: "bearer"
  
  # Bearer token for authentication
  token: "${secrets.api_token}"
  
  # Basic authentication credentials
  username: "${secrets.username}"
  password: "${secrets.password}"
  
  # OAuth2 configuration
  authentication_url: "https://auth.example.com/oauth/token"
  client_id: "${secrets.client_id}"
  client_secret: "${secrets.client_secret}"
  scopes: ["read", "write"]
  redirect_uri: "https://app.example.com/callback"
  refresh_token: "${secrets.refresh_token}"
  refresh_on_expire: true
  
  # OAuth2 flow type
  flow: "authorization_code"

# Default settings applied to all endpoints
defaults:
  # Default request configuration
  request:
    url: "https://api.example.com/v1"
    method: "GET"
    headers:
      Accept: "application/json"
    timeout: 30

# List of API endpoints to interact with
endpoints:
  - # Name of this endpoint
    name: "list_users"
    
    # Description of what this endpoint does
    description: "Retrieve a list of users"
    
    # State variables for this endpoint
    state:
      page: 1
      limit: 100
    
    # Variables to persist between API runs
    sync: ["last_updated"]
    
    # HTTP request configuration
    request:
      # Full URL or path relative to defaults.request.url
      url: "users"
      
      # HTTP method: GET, POST, PUT, PATCH, DELETE, etc.
      method: "GET"
      
      # Request timeout in seconds
      timeout: 30
      
      # HTTP headers to include with request
      headers:
        Content-Type: "application/json"
        Authorization: "Bearer ${auth.token}"
      
      # URL query parameters
      parameters:
        page: "${state.page}"
        limit: "${state.limit}"
        sort: "created_at"
      
      # Request body for POST/PUT/PATCH requests
      payload:
        user:
          name: "Example User"
          email: "user@example.com"
      
      # Expression to loop through items
      loop: "state.items"
      
      # Rate limiting (requests per second)
      rate: 5
      
      # Maximum concurrent requests
      concurrency: 3
    
    # Pagination configuration
    pagination:
      # Updated state for next page request
      next_state:
        page: "${state.page + 1}"
      
      # Expression that evaluates to true when pagination should stop
      stop_condition: "length(response.records) < ${state.limit} || response.json.meta.next_page == null"
    
    # Response processing configuration
    response:
      # How to extract records from response
      records:
        # JMESPath expression to extract records array from response
        jmespath: "data.users[]"
        
        # Field(s) that uniquely identify each record
        primary_key: ["id"]
        
        # Field used for incremental updates
        update_key: "updated_at"
        
        # Maximum number of records to process (useful for testing)
        limit: 1000
      
      # Post-processing transformations
      processors:
        - # Expression to evaluate on each record or response
          expression: "record.created_at"
          
          # Where to store the result
          output: "record.last_created_at"
          
          # If we want to aggregate values across records (maximum, minimum, flatten, first, last)
          aggregation: "maximum"
      
      # Rules for handling specific response conditions
      rules:
        - # Action to take: retry, continue, stop, fail
          action: "retry"
          
          # Condition that triggers this rule
          condition: "response.status >= 500"
          
          # Maximum retry attempts
          max_attempts: 3
          
          # Backoff strategy: none, constant, linear, exponential, jitter
          backoff: "exponential"
          
          # Base duration in seconds for backoff
          backoff_base: 2
          
          # Error message for logging/debugging
          message: "Server error occurred"
    
    # Configuration for dynamic endpoints
    dynamic:
      # Expression that returns array to iterate over
      iterate: "state.resource_ids"
      
      # Variable to store current iteration value
      into: "state.current_id"
```

## Authentication

Supported authentication types:

```yaml
authentication:
  # Basic Auth
  type: "basic"
  username: "${secrets.username}"
  password: "${secrets.password}"
  
  # Bearer Token
  type: "bearer"
  token: "${secrets.api_token}"
  
  # OAuth2 (placeholder - implementation varies)
  type: "oauth2"
  client_id: "${secrets.client_id}"
  client_secret: "${secrets.client_secret}"
```

Variable references use `${section.key}` syntax to access:
- `env`: Environment variables
- `secrets`: Sensitive credentials
- `state`: Persistent state between requests
- `auth`: Authentication data

## Endpoints

Each endpoint defines a specific API resource:

```yaml
endpoints:
  - name: "list_users"
    description: "Retrieve users from the API"
    request:
      url: "https://api.example.com/users"
      method: "GET"
      headers:
        Content-Type: "application/json"
        Accept: "application/json"
      parameters:
        limit: 100
        offset: 0
    response:
      records:
        jmespath: "data.users[]"
        primary_key: ["id"]
```

## Request Configuration

```yaml
request:
  url: "https://api.example.com/resource/${state.resource_id}"
  method: "GET|POST|PUT|PATCH|DELETE"
  headers:
    Authorization: "Bearer ${auth.token}"
    Content-Type: "application/json"
  parameters:
    page: 1
    per_page: 100
  payload: 
    key1: "value1"
    key2: "value2"
  timeout: 30  # seconds
  rate: 10     # max requests per second
  concurrency: 5  # max concurrent requests
```

## Pagination

Control how to navigate through multiple result pages:

```yaml
pagination:
  next_state:
    page: "${response.json.meta.next_page}"
  stop_condition: "response.json.meta.next_page == null"
```

## Response Processing

Extract and transform data from responses:

```yaml
response:
  records:
    jmespath: "data.items[]"  # JMESPath expression to extract records
    primary_key: ["id"]       # Primary key field(s)
    limit: 1000               # Optional limit for testing
  
  processors:
    - expression: "record.created_at"
      output: "state.last_timestamp"
      aggregation: "maximum"  # aggregation types: maximum, minimum, flatten, first, last
  
  rules:
    - action: "retry"        # retry, continue, stop, fail
      condition: "response.status >= 500"
      max_attempts: 3
      backoff: "exponential"  # none, constant, linear, exponential, jitter
      backoff_base: 2         # base delay in seconds
    - action: "fail"
      condition: "response.status == 403"
      message: "Authorization failed"
```

## Dynamic Endpoints

For APIs that require iterating through resources:

```yaml
dynamic:
  iterate: "state.resource_ids" # an array of IDs
  into: "state.current_id"
```

```yaml
endpoints:
	- dynamic:
			iterate: state.objects
			into: state.object
		
		name: ${ state.object.id }
		
		request:
			url: ${ state.object.url }
			method: GET
		
		response:
			processors:
				- expression: date_parse(record.timestamp, "auto")
					output: record.timestamp_parsed
```

## Common Patterns and Examples

### Extracting Data with JMESPath

```yaml
response:
  records:
    jmespath: "data.results[]"  # Array of objects
```

### Date-based Incremental Sync

```yaml

state:
  updated_since: ${ coalesce(state.last_sync_date, "2021-01-01") }

# array of state variable name to sync on start/end of process
# first time, the value is null (therefore it is best to use
# coalesce to have a fallback value
sync: [ last_sync_date ]

request:
  parameters:
    updated_since: "${ state.updated_since }"
response:
  processors:
    - expression: "record.updated_at"
      output: "state.last_sync_date"
      aggregation: "maximum"
```

### Pagination Examples

#### Offset-based pagination:
```yaml
pagination:
  next_state:
    offset: "${state.offset + 100}"
  stop_condition: "length(response.records) < 100"
```

#### Page-based pagination:
```yaml
pagination:
  next_state:
    page: "${state.page + 1}"
  stop_condition: "response.json.page == response.json.total_pages"
```

#### Cursor-based pagination:
```yaml
pagination:
  next_state:
    cursor: "${response.json.next_cursor}"
  stop_condition: "response.json.next_cursor == null"
```

## Template for New API Specs

```yaml
name: "API Name"
description: "API Description"

authentication:
  type: "bearer|basic|oauth2"
  # Authentication details...

defaults:
  request:
    url: "https://api.base.url"
    headers:
      Accept: "application/json"

endpoints:
  - name: "endpoint_name"
    description: "Endpoint description"
    request:
      url: "resource_path"
      method: "GET"
      parameters:
        param1: "value1"
    pagination:
      next_state:
        page: "${state.page + 1}"
      stop_condition: "response.json.next_page == null"
    response:
      records:
        jmespath: "data[]"
        primary_key: ["id"]
      processors:
        - expression: "record.updated_at"
          output: "state.last_updated"
          aggregation: "maximum"
      rules:
        - action: "retry"
          condition: "response.status >= 500"
          max_attempts: 3

```

---

## Expression Functions

You can use the following functions in expressions within your API spec.

See [here](https://github.com/slingdata-io/sling-cli/blob/main/core/dbio/api/functions_test.go) for extensive amounts of test examples.
See [here](https://github.com/slingdata-io/sling-cli/blob/main/core/dbio/api/functions.go) for the functions source code.

### String Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `upper(string)` | Converts string to uppercase | `string`: Input string | Uppercase string | `upper("hello")` → "HELLO" |
| `lower(string)` | Converts string to lowercase | `string`: Input string | Lowercase string | `lower("HELLO")` → "hello" |
| `substring(string, start[, end])` | Extracts part of a string | `string`: Input string<br>`start`: Start index<br>`end`: Optional end index | Substring | `substring("hello world", 0, 5)` → "hello" |
| `replace(string, pattern, replacement)` | Replaces occurrences of pattern | `string`: Input string<br>`pattern`: String to replace<br>`replacement`: Replacement string | Modified string | `replace("hello", "l", "x")` → "hexxo" |
| `trim(string)` | Removes whitespace from start and end | `string`: Input string | Trimmed string | `trim(" hello ")` → "hello" |
| `contains(string, substring)` | Checks if string contains substring | `string`: Input string<br>`substring`: String to find | Boolean | `contains("hello world", "world")` → true |
| `split(string, separator)` | Splits string into array | `string`: Input string<br>`separator`: Delimiter | Array of strings | `split("a,b,c", ",")` → ["a", "b", "c"] |
| `join(array, separator)` | Joins array elements into string | `array`: Array of values<br>`separator`: Delimiter | String | `join(["a", "b", "c"], ", ")` → "a, b, c" |
| `length(string|array|map)` | Gets length of string/array/map | `value`: String, array or map | Number | `length("hello")` → 5 |

### Numeric Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `int_parse(value)` | Converts value to integer | `value`: Value to convert | Integer | `int_parse("42")` → 42 |
| `float_parse(value)` | Converts value to float | `value`: Value to convert | Float | `float_parse("42.5")` → 42.5 |
| `int_format(value, format)` | Formats integer with specification | `value`: Number<br>`format`: Format string | Formatted string | `int_format(42, "05")` → "00042" |
| `float_format(value, format)` | Formats float with specification | `value`: Number<br>`format`: Format string | Formatted string | `float_format(42.5678, ".2")` → "42.57" |
| `range(start, end[, step])` | Creates array of integers | `start`: First number<br>`end`: Last number<br>`step`: Optional increment | Array of integers | `range(1, 5)` → [1, 2, 3, 4, 5] |
| `greatest(array)` or `greatest(val1, val2, ...)` | Finds maximum value | `array` or multiple values | Maximum value | `greatest(1, 5, 3)` → 5 |
| `least(array)` or `least(val1, val2, ...)` | Finds minimum value | `array` or multiple values | Minimum value | `least(1, 5, 3)` → 1 |
| `is_greater(val1, val2)` | Checks if first value is greater | `val1`, `val2`: Values to compare | Boolean | `is_greater(5, 3)` → true |
| `is_less(val1, val2)` | Checks if first value is less | `val1`, `val2`: Values to compare | Boolean | `is_less(3, 5)` → true |

### Date Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `date_parse(string[, format])` | Parses string to date | `string`: Date string<br>`format`: Optional format string or "auto" | Date object | `date_parse("2022-01-01", "auto")` |
| `date_format(date, format)` | Formats date to string | `date`: Date object<br>`format`: Format string | Formatted string | `date_format(date_parse("2022-01-01"), "%Y-%m-%d")` → "2022-01-01" |
| `date_add(date, duration[, unit])` | Adds time to date | `date`: Date object<br>`duration`: Time to add<br>`unit`: Optional unit | Date object | `date_add(date_parse("2022-01-01"), 1, "day")` |
| `date_diff(date1, date2[, unit])` | Calculates time between dates | `date1`, `date2`: Date objects<br>`unit`: Optional unit | Number | `date_diff(date_parse("2022-01-02"), date_parse("2022-01-01"), "day")` → 1 |
| `date_trunc(date, unit)` | Truncates date to specified unit | `date`: Date object<br>`unit`: Time unit | Date object | `date_trunc(date_parse("2022-01-15"), "month")` → "2022-01-01" |
| `date_extract(date, part)` | Extracts part from date | `date`: Date object<br>`part`: Part to extract | Number | `date_extract(date_parse("2022-01-15"), "day")` → 15 |
| `date_last(date[, period])` | Gets last day of period | `date`: Date object<br>`period`: Optional period | Date object | `date_last(date_parse("2022-01-15"))` → "2022-01-31" |
| `date_first(date[, period])` | Gets first day of period | `date`: Date object<br>`period`: Optional period | Date object | `date_first(date_parse("2022-01-15"))` → "2022-01-01" |
| `now()` | Gets current date and time | None | Current date | `now()` |

The `format` argument used in `date_format()` and `date_parse()` adheres to the [strftime](https://linux.die.net/man/3/strftime) standard (same as python).
Sling uses [timefmt-go](https://github.com/itchyny/timefmt-go) internally for this.

#### Basic Auto-Detection

```yaml
# Automatically detects the date format
- expression: date_parse("2022-05-15", "auto")
  output: state.parsed_date
```

#### Specific Format Parsing

```yaml
# Parses date using the specified format
- expression: date_parse("05/15/2022", "%m/%d/%Y")
  output: state.parsed_date
```

#### Parsing ISO 8601 Timestamps

```yaml
# Parses ISO 8601 timestamp with timezone
- expression: date_parse("2022-05-15T14:30:45Z", "auto")
  output: state.parsed_timestamp
```

#### Parsing from API Response

```yaml
# Converts string dates in records to date objects
response:
  processors:
    - expression: date_parse(record.created_at, "auto")
      output: record.created_date
```

#### Basic Date Formatting

```yaml
# Formats date as "2022-05-15"
- expression: date_format(date_parse("2022-05-15", "auto"), "%Y-%m-%d")
  output: state.formatted_date
```

#### Different Format Patterns

```yaml
# Formats date as "15/05/2022"
- expression: date_format(date_parse("2022-05-15", "auto"), "%d/%m/%Y")
  output: state.european_date

# Formats date as "May 15, 2022"
- expression: date_format(date_parse("2022-05-15", "auto"), "%B %d, %Y")
  output: state.long_date

# Formats date as "20220515"
- expression: date_format(date_parse("2022-05-15", "auto"), "%Y%m%d")
  output: state.compact_date
```

#### Formatting Times

```yaml
# Formats as "14:30:45"
- expression: date_format(date_parse("2022-05-15T14:30:45Z", "auto"), "%H:%M:%S")
  output: state.time_only

# Formats as "2022-05-15 02:30 PM"
- expression: date_format(date_parse("2022-05-15T14:30:45Z", "auto"), "%Y-%m-%d %I:%M %p")
  output: state.datetime_12h
```

#### Formatting Dates for API Requests

```yaml
# Format current date minus 7 days in required API format
request:
  parameters:
    start_date: "${date_format(date_add(now(), -7, 'day'), '%Y-%m-%d')}"
    end_date: "${date_format(now(), '%Y-%m-%d')}"
```

#### Incremental Sync Using Dates

We can use the `sync` keys to indicate which state variables need to be stored and retrieved for the next run (ideal for incremental replications).

```yaml
state:
  last_sync: "${coalesce(state.last_sync_date, '2020-01-01')}"

# `sync` defines the state variables to be persistent in your SLING_STATE connection 
# at the end of the run, and retrieved at the start of next run
sync: [ last_sync_date ]

request:
  parameters:
    # Format the saved state date in the format the API requires
    updated_since: "${date_format(date_parse(state.last_sync, 'auto'), '%Y-%m-%dT%H:%M:%SZ')}"

response:
  processors:
    # Update the sync state with the latest date from records
    - expression: record.updated_at
      output: state.last_sync_date
      aggregation: maximum
```

#### Date Calculations in Filters

```yaml
request:
  parameters:
    # Create a date range for last month
    start_date: "${date_format(date_first(date_add(now(), -1, 'month'), 'month'), '%Y-%m-%d')}"
    end_date: "${date_format(date_last(date_add(now(), -1, 'month'), 'month'), '%Y-%m-%d')}"
```


### Value Handling Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `coalesce(val1, val2, ...)` | Returns first non-null value | Multiple values | First non-null value | `coalesce(null, "default")` → "default" |
| `value(val1, val2, ...)` | Returns first non-empty value | Multiple values | First non-empty value | `value("", "default")` → "default" |
| `require(val, error_msg)` | Ensures value is not null | `val`: Value to check<br>`error_msg`: Error message | The value or error | `require(someVar, "Variable required")` |
| `cast(value, type)` | Converts value to specified type | `value`: Value to convert<br>`type`: Target type | Converted value | `cast("42", "int")` → 42 |
| `try_cast(value, type)` | Tries to convert, returns null on failure | `value`: Value to convert<br>`type`: Target type | Converted value or null | `try_cast("test", "int")` → null |
| `element(array, index)` | Gets element at index from array | `array`: Source array<br>`index`: Position | Element | `element(["a", "b", "c"], 1)` → "b" |
| `is_null(value)` | Checks if value is null | `value`: Value to check | Boolean | `is_null(null)` → true |
| `is_empty(value)` | Checks if value is empty | `value`: Value to check | Boolean | `is_empty("")` → true |
| `if(condition, then_val, else_val)` | Conditional expression | `condition`: Boolean condition<br>`then_val`: Value if true<br>`else_val`: Value if false | Selected value | `if(5 > 3, "yes", "no")` → "yes" |

### Collection Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `keys(map)` | Gets keys from map | `map`: Map object | Array of keys | `keys({"a": 1, "b": 2})` → ["a", "b"] |
| `values(map)` | Gets values from map | `map`: Map object | Array of values | `values({"a": 1, "b": 2})` → [1, 2] |
| `jmespath(object, expression)` | Evaluates JMESPath expression | `object`: Data object<br>`expression`: JMESPath query | Query result | `jmespath({"foo": {"bar": "baz"}}, "foo.bar")` → "baz" |
| `get_path(object, path)` | Gets value at specified path | `object`: Data object<br>`path`: Dot notation path | Value at path | `get_path({"foo": {"bar": "baz"}}, "foo.bar")` → "baz" |
| `filter(array, expression)` | Filters array using expression | `array`: Array to filter<br>`expression`: Filter condition | Filtered array | `filter([1, 2, 3], "value > 1")` → [2, 3] |
| `map(array, expression)` | Maps array using expression | `array`: Array to transform<br>`expression`: Transformation | Transformed array | `map([1, 2, 3], "value * 2")` → [2, 4, 6] |
| `sort(array[, descending])` | Sorts array elements | `array`: Array to sort<br>`descending`: Optional boolean | Sorted array | `sort([3, 1, 2])` → [1, 2, 3] |

### Encoding/Decoding Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `encode_url(string)` | URL encodes a string | `string`: String to encode | Encoded string | `encode_url("hello world")` → "hello+world" |
| `decode_url(string)` | URL decodes a string | `string`: String to decode | Decoded string | `decode_url("hello+world")` → "hello world" |
| `encode_base64(string)` | Base64 encodes a string | `string`: String to encode | Encoded string | `encode_base64("hello")` → "aGVsbG8=" |
| `decode_base64(string)` | Base64 decodes a string | `string`: String to decode | Decoded string | `decode_base64("aGVsbG8=")` → "hello" |
| `hash(string[, algorithm])` | Creates hash of string | `string`: Input string<br>`algorithm`: Hash algorithm | Hash string | `hash("hello", "md5")` → "5d41402abc4b2a76b9719d911017c592" |

### Utility Functions

| Function | Description | Parameters | Returns | Example |
|----------|-------------|------------|---------|---------|
| `uuid()` | Generates random UUID | None | UUID string | `uuid()` → "550e8400-e29b-41d4-a716-446655440000" |
| `equals(val1, val2)` | Checks if values are equal | `val1`, `val2`: Values to compare | Boolean | `equals("test", "test")` → true |
| `log(message)` | Logs a message | `message`: Message to log | The message | `log("Processing item")` → "Processing item" |
| `regex_match(string, pattern)` | Checks if string matches pattern | `string`: Input string<br>`pattern`: Regex pattern | Boolean | `regex_match("abc123", "\\d+")` → true |
| `regex_extract(string, pattern[, group])` | Extracts matches from string | `string`: Input string<br>`pattern`: Regex pattern<br>`group`: Optional group index | Matches or specific group | `regex_extract("hello world", "(hello) (world)", 1)` → "hello" |
