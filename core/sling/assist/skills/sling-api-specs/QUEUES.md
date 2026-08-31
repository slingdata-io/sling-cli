# Queues and Iteration

Queues pass data between endpoints. Use them to chain API calls.

## Queue Declaration

Declare queues at the top level:

```yaml
queues:
  - user_ids
  - order_ids
```

## Producer → Consumer Pattern

Canonical parent-child: the producer extracts records (`records.jmespath`) before `output: queue.*`. The child sets `depends_on`, `iterate.over: queue.*`, and its own `records.jmespath`.

```yaml
queues:
  - user_ids

endpoints:
  # Producer: fetch users and send IDs to queue
  users:
    request:
      url: "{state.base_url}/users"
    response:
      records:
        jmespath: "data[]"
        primary_key: [id]
      processors:
        - expression: "record.id"
          output: "queue.user_ids"

  # Consumer: iterate over queue and fetch details
  posts:
    depends_on: [users]
    iterate:
      over: "queue.user_ids"
      into: "state.user_id"
      concurrency: 10

    request:
      url: "{state.base_url}/users/{state.user_id}/posts"
    response:
      records:
        jmespath: "data[]"
        primary_key: [id]
```

## Iterate Configuration

```yaml
iterate:
  # Expression or queue to iterate over
  over: "queue.user_ids"

  # State variable to store current item
  into: "state.current_id"

  # Queue iteration only: when to consume the queue
  #   deferred (default) — wait for producer to finish, then read from start
  #   immediate — tail the queue live as the producer appends (fail-fast)
  consume: deferred

  # Number of concurrent iterations (default: 10)
  concurrency: 5

  # Optional condition to start iteration
  if: "state.process_details == true"
```

## Queue Consumption: `deferred` vs `immediate`

`iterate.consume` controls **when** a consumer reads its queue:

- **`deferred`** (default): wait for the producer to fully finish, then read from the start. Use when the consumer needs the complete set (dedupe/aggregate), or for ordinary fan-out.
- **`immediate`**: tail the queue live — start processing as soon as the producer appends. Runs the producer and consumers concurrently (forces threads). Enables **fail-fast**: the producer and the consumers tailing its queue form a group, and if any member fails, the rest of that group is terminated. A failed consumer stops its producer; a failed producer stops its consumers. Unrelated streams keep running.

```yaml
endpoints:
  search:
    queue_only: true
    response:
      processors:
        - expression: "record.id"
          output: "queue.item_ids"

  details:
    iterate:
      over: "queue.item_ids"
      into: "state.item_id"
      consume: immediate   # tail live, fail-fast
    request:
      url: "{state.base_url}/items/{state.item_id}"
```

`immediate` only affects queue iteration (no effect on arrays, `range()`, etc.).

## Queue Lifecycle

1. **Declare**: Register in `queues` array
2. **Write**: Send values via `output: "queue.name"`
3. **Read**: Consume via `iterate.over: "queue.name"`
4. **Auto-cleanup**: Queues are temporary JSONL files

**Important**: Producers run before consumers (auto-ordered). Cannot write to a queue after reading starts.

## Direct Queue-to-Records

Process queue items without HTTP requests:

```yaml
queues:
  - user_ids
  - valid_user_ids

endpoints:
  # Producer
  fetch_users:
    request:
      url: "{state.base_url}/users"
    response:
      processors:
        - expression: "record.id"
          output: "queue.user_ids"

  # Transform without API call
  filter_users:
    iterate:
      over: "queue.user_ids"
      into: "response.records"  # No HTTP request!
    response:
      processors:
        - expression: "upper(record)"
          output: "record.user_id_upper"
        - expression: "record"
          if: '!is_null(record) && record != ""'
          output: "queue.valid_user_ids"

  # Consumer with API call
  fetch_user_details:
    iterate:
      over: "queue.valid_user_ids"
      into: "state.user_id"
    request:
      url: "{state.base_url}/users/{state.user_id}"
```

## Date Range Iteration

Generate date range without a queue:

```yaml
endpoints:
  daily_reports:
    iterate:
      over: >
        range(
          date_trunc(date_add(now(), -7, "day"), "day"),
          date_trunc(now(), "day"),
          "1d"
        )
      into: "state.current_day"
      concurrency: 7

    request:
      url: "{state.base_url}/reports"
      parameters:
        date: '{date_format(state.current_day, "%Y-%m-%d")}'
```

## Chunk Processing

Process items in batches:

```yaml
endpoints:
  # Producer: fetch all variant IDs
  list_variants:
    response:
      processors:
        - expression: "record.id"
          output: "queue.variant_ids"

  # Consumer: process in batches of 50
  batch_variant_details:
    iterate:
      over: "chunk(queue.variant_ids, 50)"
      into: "state.variant_batch"  # Array of 50 IDs
      concurrency: 5

    request:
      url: "{state.base_url}/variants/batch"
      parameters:
        ids: '{join(state.variant_batch, ",")}'
```

## Conditional Queue Writing

Filter what goes into the queue:

```yaml
processors:
  # Only queue US customers
  - expression: "record.id"
    if: 'record.country == "US"'
    output: "queue.us_customer_ids"

  # Only queue active users
  - expression: "record.id"
    if: 'record.status == "active" && record.verified == true'
    output: "queue.verified_active_ids"
```

## Multiple Queues

Chain multiple endpoints:

```yaml
queues:
  - customer_ids
  - order_ids
  - line_item_ids

endpoints:
  customers:
    response:
      processors:
        - expression: "record.id"
          output: "queue.customer_ids"

  customer_orders:
    iterate:
      over: "queue.customer_ids"
      into: "state.customer_id"
    request:
      url: "{state.base_url}/customers/{state.customer_id}/orders"
    response:
      processors:
        - expression: "record.id"
          output: "queue.order_ids"

  order_line_items:
    iterate:
      over: "queue.order_ids"
      into: "state.order_id"
    request:
      url: "{state.base_url}/orders/{state.order_id}/line_items"
```

## Static Iteration

Iterate over a static list:

```yaml
endpoints:
  fetch_regions:
    iterate:
      over: '["us-east", "us-west", "eu-central"]'
      into: "state.region"

    request:
      url: "{state.base_url}/regions/{state.region}/data"
```

## Tips

- Use descriptive queue names: `user_ids`, `order_ids`
- Keep queue items small (IDs, strings)
- Let Sling determine execution order
- Use `if` conditions to filter before queuing

## Related Topics

- [PROCESSORS.md](PROCESSORS.md) - Writing to queues
- [PAGINATION.md](PAGINATION.md) - Pagination within iterations
- [FUNCTIONS.md](FUNCTIONS.md) - `range()`, `chunk()`, `join()` functions
