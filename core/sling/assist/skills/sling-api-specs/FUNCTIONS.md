# Expression Functions

Functions available within `{...}` expressions. Double-quoted strings are preferred; single-quoted SQL-style literals (`'%Y-%m-%d'`) are also accepted.

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `upper(string)` | Uppercase | `upper("hello")` → "HELLO" |
| `lower(string)` | Lowercase | `lower("HELLO")` → "hello" |
| `substring(str, start[, end])` | Extract substring | `substring("hello", 0, 3)` → "hel" |
| `replace(str, pattern, repl)` | Replace pattern | `replace("hello", "l", "x")` → "hexxo" |
| `trim(string)` | Remove whitespace | `trim(" hello ")` → "hello" |
| `contains(str, substr)` | Check substring | `contains("hello", "ell")` → true |
| `split(str, sep)` | Split to array | `split("a,b,c", ",")` → ["a","b","c"] |
| `split_part(str, sep, idx)` | Get split part (1-based) | `split_part("a,b,c", ",", 2)` → "b" |
| `join(array, sep)` | Join array | `join(["a","b"], ", ")` → "a, b" |
| `length(value)` | Length of string/array/map | `length("hello")` → 5 |

## Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `int_parse(value)` | Convert to integer | `int_parse("42")` → 42 |
| `float_parse(value)` | Convert to float | `float_parse("42.5")` → 42.5 |
| `int_format(val, fmt)` | Format integer (Go) | `int_format(42, "%05d")` → "00042" |
| `float_format(val, fmt)` | Format float (Go) | `float_format(42.5, "%.2f")` → "42.50" |
| `greatest(array\|vals...)` | Maximum value | `greatest(1, 5, 3)` → 5 |
| `least(array\|vals...)` | Minimum value | `least(1, 5, 3)` → 1 |
| `is_greater(a, b)` | Check a > b | `is_greater(5, 3)` → true |
| `is_less(a, b)` | Check a < b | `is_less(3, 5)` → true |

## Date Functions

Uses strftime format conventions.

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Current time | `now()` |
| `date_parse(str[, fmt])` | Parse string to time | `date_parse("2024-01-15", "auto")` |
| `date_format(date, fmt)` | Format time to string | `date_format(now(), "%Y-%m-%d")` |
| `date_add(date, dur[, unit])` | Add duration | `date_add(now(), -7, "day")` |
| `date_diff(d1, d2[, unit])` | Difference between dates | `date_diff(d1, d2, "hour")` → 24.0 |
| `date_trunc(date, unit)` | Truncate to unit start | `date_trunc(now(), "month")` |
| `date_extract(date, part)` | Extract part | `date_extract(now(), "year")` → 2024 |
| `date_last(date[, period])` | Last day of period | `date_last(now(), "month")` |
| `date_first(date[, period])` | First day of period | `date_first(now(), "month")` |
| `range(start, end[, step])` | Array of time objects | `range(d1, d2, "1d")` |

**Units**: year, month, week, day, hour, minute, second

### Date Examples

```yaml
# 7 days ago
date_add(now(), -7, "day")

# First day of current month
date_first(now(), "month")

# Format for API
date_format(now(), "%Y-%m-%dT%H:%M:%SZ")

# Parse auto-detect
date_parse("2024-01-15T10:30:00Z", "auto")

# Date range iteration
range(date_add(now(), -7, "day"), now(), "1d")
```

## Value Handling Functions

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(v1, v2, ...)` | First non-null | `coalesce(null, "default")` → "default" |
| `ifnull(v1, v2, ...)` | Alias of coalesce | `ifnull(null, "default")` → "default" |
| `null_if(val, sentinel[, …])` | Null if equal | `null_if("N/A", "N/A")` → null |
| `nullif(val, sentinel[, …])` | Alias of null_if | `nullif(x, "")` → null |
| `value(v1, v2, ...)` | First non-empty | `value("", "default")` → "default" |
| `require(val[, msg])` | Ensure not null | `require(secrets.key, "Key required")` |
| `cast(val, type)` | Convert type | `cast("42", "int")` → 42 |
| `try_cast(val, type)` | Convert or null | `try_cast("abc", "int")` → null |
| `element(array, idx)` | Get by index (0-based) | `element(["a","b"], 1)` → "b" |
| `is_null(value)` | Check null | `is_null(state.val)` → true/false |
| `is_empty(value)` | Check empty | `is_empty("")` → true |
| `if(cond, then, else)` | Conditional | `if(x > 0, "pos", "neg")` |
| `iif(cond, then, else)` | Alias of if | `iif(x > 0, "pos", "neg")` |
| `equals(a, b)` | Deep equality | `equals(response.status, 200)` |

**Cast types**: int, float, string, bool

## Collection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `array(v1, v2, ...)` | Create array | `array(1, 2, 3)` → [1,2,3] |
| `object(k1, v1, k2, v2, ...)` | Create object | `object("a", 1)` → {"a":1} |
| `keys(map)` | Get keys | `keys({"a":1})` → ["a"] |
| `values(map)` | Get values | `values({"a":1})` → [1] |
| `exists(coll, item)` | Check existence | `exists({"a":1}, "a")` → true |
| `jmespath(obj, expr)` | JMESPath query | `jmespath(data, "items[].id")` |
| `jq(obj, expr)` | jq filter (returns array) | `jq(data, ".items[].id")` |
| `get_path(obj, path)` | Dot notation access | `get_path(data, "a.b[0]")` |
| `object_rename(map, old, new, ...)` | Rename keys | `object_rename(m, "a", "x")` |
| `object_delete(map, k1, ...)` | Delete keys | `object_delete(m, "a")` |
| `object_casing(map, case)` | Transform key casing | `object_casing(m, "snake")` |
| `object_merge(m1, m2, ...)` | Merge maps | `object_merge(m1, m2)` |
| `object_zip(keys, vals)` | Create from arrays | `object_zip(["a"], [1])` → {"a":1} |
| `filter(array, expr)` | Filter array | `filter([1,2,3], "value > 1")` → [2,3] |
| `map(array, expr)` | Transform array | `map([1,2,3], "value * 2")` → [2,4,6] |
| `sort(array[, desc])` | Sort array | `sort([3,1,2])` → [1,2,3] |
| `chunk(array\|queue, size)` | Split into chunks | `chunk(queue.ids, 50)` |

**object_casing types**: snake, camel, upper, lower

### Collection Examples

```yaml
# Select fields
object("id", record.id, "name", record.name)

# Flatten nested (jmespath)
jmespath(record, "{id: id, email: user.email}")

# Flatten nested (jq) — returns array, use [0] for single record
jq(record, "{id, email: .user.email}")[0]

# Filter active items
filter(records, "value.status == \"active\"")

# Process chunks
chunk(queue.ids, 50)
```

## Encoding Functions

| Function | Description | Example |
|----------|-------------|---------|
| `encode_url(string)` | URL encode | `encode_url("a b")` → "a%20b" |
| `decode_url(string)` | URL decode | `decode_url("a%20b")` → "a b" |
| `encode_base64(string)` | Base64 encode | `encode_base64("hello")` |
| `decode_base64(string)` | Base64 decode | `decode_base64("aGVsbG8=")` |
| `hash(str[, algo])` | Hash string | `hash("hello", "sha256")` |

**Hash algorithms**: md5, sha1, sha256

## Utility Functions

| Function | Description | Example |
|----------|-------------|---------|
| `uuid()` | Generate UUID | `uuid()` → "..." |
| `log(message)` | Log and return | `log("Debug: " + record.id)` |
| `regex_match(str, pattern)` | Check regex match | `regex_match("img.jpg", ".*\\.jpg")` |
| `regex_extract(str, pat[, idx])` | Extract regex group | `regex_extract("id=123", "id=(\\d+)", 1)` → "123" |

## Common Patterns

### Default Values

```yaml
# Use coalesce for null handling
state:
  cursor: '{coalesce(sync.cursor, null)}'
  since: '{coalesce(sync.last_date, "2024-01-01")}'
```

### Date Formatting

```yaml
# ISO 8601 for API
parameters:
  updated_since: '{date_format(state.since, "%Y-%m-%dT%H:%M:%SZ")}'

# Date only
parameters:
  date: '{date_format(state.current_date, "%Y-%m-%d")}'
```

### Conditional Logic

```yaml
# If-then-else
expression: 'if(is_null(record.email), "none", lower(record.email))'

# Check before access
expression: 'if(!is_null(record.meta), record.meta.tags, array())'
```

### String Manipulation

```yaml
# Extract domain
expression: 'split_part(record.email, "@", 2)'

# Build URL
url: '{join([state.base_url, "users", state.user_id], "/")}'
```

### Array Processing

```yaml
# Filter and transform
iterate:
  over: 'filter(map(queue.items, "value.id"), "value != null")'
```

## Related Topics

- [VARIABLES.md](VARIABLES.md) - Variable scopes in expressions
- [PROCESSORS.md](PROCESSORS.md) - Using functions in processors
- [PAGINATION.md](PAGINATION.md) - Functions in stop conditions
