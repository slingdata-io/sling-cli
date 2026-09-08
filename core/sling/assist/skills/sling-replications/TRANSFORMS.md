# Data Transformation Functions

Sling provides 50+ built-in functions for data transformation. The `transforms` key is set in replication `defaults` or per stream (or under `source_options` for named transforms). The same function library is used in expressions everywhere: hook/step `check` and `if` conditions, pipeline variables, and API specs.

## Transform Syntax

### Array (Global)
Apply to all columns:
```yaml
transforms: ['trim_space', 'remove_diacritics']
```

### Map (Column-Specific)
Apply to specific columns:
```yaml
transforms:
  name: ['trim_space', 'upper']
  email: ['lower']
```

### Staged (Multi-Stage)
Multiple stages with expressions:
```yaml
transforms:
  - name: 'trim_space(value)'
    email: 'lower(value)'

  - full_name: 'record.first_name + " " + record.last_name'

  - tier: 'record.total_spent >= 1000 ? "premium" : "standard"'
```

## String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `upper(s)` | Uppercase | `upper("hello")` -> "HELLO" |
| `lower(s)` | Lowercase | `lower("HELLO")` -> "hello" |
| `trim(s)` | Remove whitespace | `trim(" hi ")` -> "hi" |
| `replace(s, old, new)` | Replace text | `replace("a-b", "-", "_")` -> "a_b" |
| `substring(s, start, end)` | Extract substring | `substring("hello", 0, 2)` -> "he" |
| `split(s, sep)` | Split to array | `split("a,b", ",")` -> ["a","b"] |
| `split_part(s, sep, i)` | Get split part | `split_part("a,b,c", ",", 1)` -> "b" |
| `join(arr, sep)` | Join array | `join(["a","b"], "-")` -> "a-b" |
| `length(s)` | String length | `length("hello")` -> 5 |
| `contains(s, sub)` | Check contains | `contains("hello", "ell")` -> true |
| `snake(s)` | Snake case | `snake("Hello World")` -> "hello_world" |
| `slugify(s)` | URL slug | `slugify("Hello World!")` -> "hello-world" |
| `remove_diacritics(s)` | Remove accents | `remove_diacritics("café")` -> "cafe" |
| `replace_accents(s)` | Alias of remove_diacritics | `replace_accents("café")` -> "cafe" |
| `replace_non_printable(s)` | Clean string | Removes control chars |
| `replace_0x00(s)` | Remove null bytes | Cleans null chars |

## Numeric Functions

| Function | Description | Example |
|----------|-------------|---------|
| `int_parse(v)` | Parse integer | `int_parse("42")` -> 42 |
| `float_parse(v)` | Parse float | `float_parse("3.14")` -> 3.14 |
| `bool_parse(v)` | Parse boolean | `bool_parse("true")` -> true |
| `int_format(n, fmt)` | Format integer | `int_format(42, "%05d")` -> "00042" |
| `float_format(n, fmt)` | Format float | `float_format(3.14, "%.1f")` -> "3.1" |
| `greatest(...)` | Maximum value | `greatest(1, 5, 3)` -> 5 |
| `least(...)` | Minimum value | `least(1, 5, 3)` -> 1 |
| `is_greater(a, b)` | Compare | `is_greater(5, 3)` -> true |
| `is_less(a, b)` | Compare | `is_less(3, 5)` -> true |
| `int_range(start, end)` | Generate range | `int_range(1, 3)` -> [1,2,3] |

## Date Functions

| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Current time | Returns datetime |
| `date_parse(s, fmt)` | Parse date | `date_parse("2024-01-15", "auto")` |
| `date_format(d, fmt)` | Format date | `date_format(now(), "%Y-%m-%d")` |
| `date_add(d, n, unit)` | Add duration | `date_add(now(), -7, "day")` |
| `date_diff(d1, d2, unit)` | Difference | `date_diff(d1, d2, "hour")` |
| `date_trunc(d, unit)` | Truncate | `date_trunc(now(), "month")` |
| `date_extract(d, part)` | Extract part | `date_extract(now(), "year")` |
| `date_first(d, period)` | First of period | `date_first(now(), "month")` |
| `date_last(d, period)` | Last of period | `date_last(now(), "month")` |
| `date_range(s, e, step)` | Date range | `date_range("2024-01-01", "2024-01-03", "1d")` |
| `range(s, e[, step])` | Alias of date_range (time array) | `range(d1, d2, "1d")` |
| `date_timezone(d, tz)` | Convert TZ | `date_timezone(now(), "America/New_York")` |

**Date units**: year, month, week, day, hour, minute, second

**Format syntax**: Uses strftime (e.g., `%Y-%m-%d %H:%M:%S`)

## Value Functions

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(...)` | First non-null | `coalesce(null, "default")` |
| `ifnull(...)` | Alias of coalesce | `ifnull(null, "default")` |
| `null_if(v, s[, …])` | Null if equal | `null_if("N/A", "N/A")` |
| `nullif(v, s[, …])` | Alias of null_if | `nullif(x, "")` |
| `first_valid(...)` | First non-empty | `first_valid("", "value")` |
| `is_null(v)` | Check null | `is_null(value)` |
| `is_empty(v)` | Check empty | `is_empty("")` -> true |
| `cast(v, type)` | Convert type | `cast("42", "int")` |
| `try_cast(v, type)` | Safe convert | `try_cast("abc", "int")` -> null |
| `if(cond, t, f)` | Conditional | `if(x > 0, "pos", "neg")` |
| `iif(cond, t, f)` | Alias of if | `iif(x > 0, "pos", "neg")` |
| `equals(a, b)` | Deep equality | `equals(obj1, obj2)` |
| `require(v, msg)` | Assert non-null | `require(v, "Required!")` |

## Collection Functions

| Function | Description | Example |
|----------|-------------|---------|
| `array(...)` | Create array | `array(1, 2, 3)` |
| `object(k, v, ...)` | Create object | `object("a", 1, "b", 2)` |
| `keys(map)` | Get keys | `keys({"a": 1})` -> ["a"] |
| `values(map)` | Get values | `values({"a": 1})` -> [1] |
| `exists(col, item)` | Check exists | `exists([1,2], 2)` -> true |
| `element(arr, i)` | Get element | `element([1,2,3], 1)` -> 2 |
| `filter(arr, expr)` | Filter array | `filter([1,2,3], "element > 1")` |
| `sort(arr, desc?)` | Sort array | `sort([3,1,2])` -> [1,2,3] |
| `pluck(arr, key)` | Extract field | `pluck([{a:1},{a:2}], "a")` -> [1,2] |
| `jmespath(obj, expr)` | JMESPath query | `jmespath(data, "items[].name")` |
| `jp(obj, expr)` | Alias of jmespath | `jp(data, "items[].name")` |
| `jq(obj, expr)` | jq filter (returns array) | `jq(data, ".items[].name")` |
| `get_path(obj, path)` | Get by path | `get_path(obj, "a.b[0]")` |
| `object_merge(...)` | Merge objects | `object_merge({a:1}, {b:2})` |
| `object_delete(obj, keys...)` | Delete keys | `object_delete(obj, "a")` |
| `object_rename(obj, map)` | Rename keys | `object_rename(obj, {"a":"b"})` |
| `object_zip(keys, vals)` | Zip to object | `object_zip(["a"], [1])` |
| `object_casing(obj, casing)` | Recase keys | `object_casing(obj, "snake")` |
| `explode(arr)` | Explode array to rows | Used in staged transforms |
| `chunk(arr, size)` | Split chunks | `chunk([1,2,3,4], 2)` |

## Encoding Functions

| Function | Description | Example |
|----------|-------------|---------|
| `encode_url(s)` | URL encode | `encode_url("a b")` -> "a%20b" |
| `decode_url(s)` | URL decode | `decode_url("a%20b")` -> "a b" |
| `encode_base64(s)` | Base64 encode | `encode_base64("hi")` |
| `decode_base64(s)` | Base64 decode | `decode_base64("aGk=")` |
| `hash(s, algo)` | Hash string | `hash("hello", "sha256")` |
| `json_parse(s)` | Parse JSON | `json_parse('{"a":1}')` |

## Utility Functions

| Function | Description | Example |
|----------|-------------|---------|
| `uuid()` | Generate UUID | Returns UUID v4 |
| `log(msg)` | Log message | Prints and returns msg |
| `regex_match(s, pat)` | Test pattern | `regex_match("a1", "\\d")` |
| `regex_extract(s, pat, i)` | Extract match | `regex_extract("id=123", "id=(\\d+)", 1)` |
| `regex_replace(s, pat, r)` | Replace pattern | `regex_replace("a1b2", "\\d", "X")` |
| `type_of(v)` | Get type | `type_of(42)` -> "integer" |
| `parse_ms_uuid(s)` | Parse MS UUID timestamp | Extracts time from MS-style UUID |
| `pretty_table(rows)` | Format as table string | Debug / log helper |
| `conn_property(name)` | Connection property | Reads from active connection |
| `machine_stats()` | Host stats object | Runtime diagnostics |

## Transform Examples

### Clean and Normalize
```yaml
transforms:
  - name: 'trim_space(upper(value))'
    email: 'lower(trim_space(value))'
    phone: 'regex_replace(value, "[^0-9]", "")'
```

### Computed Fields
```yaml
transforms:
  - full_name: 'record.first_name + " " + record.last_name'
    domain: 'split_part(record.email, "@", 1)'
    age: 'date_diff(now(), record.birth_date, "year")'
```

### Conditional Logic
```yaml
transforms:
  - status: 'record.active ? "active" : "inactive"'
    tier: |
      record.total >= 1000 ? "gold" : (
        record.total >= 500 ? "silver" : "bronze"
      )
```

### Hash/Anonymize
```yaml
transforms:
  - email_hash: 'hash(record.email, "sha256")'
    masked_ssn: '"XXX-XX-" + substring(record.ssn, -4)'
```

## Full Documentation

See https://docs.slingdata.io/concepts/functions.md for complete reference.
