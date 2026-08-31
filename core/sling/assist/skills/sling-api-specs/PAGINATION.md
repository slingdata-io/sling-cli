# Pagination

Pagination controls how Sling navigates through multiple pages of results.

**Required.** Every paginated endpoint must set `stop_condition`. Without it the run times out after walking pages forever. Treat this as a hard checklist item before you declare the spec done. Also initialize numeric state used in `next_state` (`state.skip: 0`, `state.limit: 50`) so the first page does not add nil.

## Structure

```yaml
pagination:
  # Update state for next page request
  next_state:
    cursor: "{response.json.next_cursor}"

  # When to stop paginating
  stop_condition: 'is_null(response.json.next_cursor)'
```

## Cursor-Based (Stripe, Shopify)

Use `starting_after`, `cursor`, or `page_token`. Stop when no more results:

```yaml
endpoints:
  items:
    state:
      starting_after: '{coalesce(sync.last_id, null)}'
    sync: [last_id]

    request:
      url: "{state.base_url}/items"
      parameters:
        limit: 100
        starting_after: '{state.starting_after}'

    pagination:
      next_state:
        starting_after: "{response.records[-1].id}"
      stop_condition: 'length(response.records) == 0'

    response:
      records:
        jmespath: "data[]"
      processors:
        - expression: "record.id"
          output: "state.last_id"
          aggregation: "last"
```

## Page Number

Increment `page` parameter. Stop when reaching total pages:

```yaml
endpoints:
  items:
    state:
      page: 1

    request:
      url: "{state.base_url}/items"
      parameters:
        page: "{state.page}"
        per_page: 100

    pagination:
      next_state:
        page: "{state.page + 1}"
      stop_condition: 'response.json.page >= response.json.total_pages'
```

## Offset-Based

Increment `offset` by `limit`. Stop when less than limit returned:

```yaml
endpoints:
  items:
    state:
      offset: 0
      limit: 100

    request:
      url: "{state.base_url}/items"
      parameters:
        offset: "{state.offset}"
        limit: "{state.limit}"

    pagination:
      next_state:
        offset: "{state.offset + state.limit}"
      stop_condition: 'length(response.records) < state.limit'
```

## Link Header (GitHub)

Parse `Link` header for `rel="next"` URL:

```yaml
endpoints:
  items:
    request:
      url: "{state.base_url}/items"

    pagination:
      next_state:
        url: >
          {
            if(contains(response.headers.link, "rel=\"next\""),
               trim(split_part(split(response.headers.link, ",")[0], ";", 0), "<>"),
               null
            )
          }
      stop_condition: '!contains(response.headers.link, "rel=\"next\"")'
```

## Response Cursor

When API returns cursor in response body:

```yaml
endpoints:
  items:
    request:
      url: "{state.base_url}/items"
      parameters:
        cursor: "{state.cursor}"

    pagination:
      next_state:
        cursor: "{response.json.next_cursor}"
      stop_condition: 'is_null(response.json.next_cursor) || response.json.next_cursor == ""'
```

## Has-More Flag

When API returns `has_more` boolean:

```yaml
pagination:
  next_state:
    starting_after: "{response.records[-1].id}"
  stop_condition: 'response.json.has_more == false'
```

## Multiple Stop Conditions

Combine conditions with logical operators:

```yaml
pagination:
  stop_condition: >
    length(response.records) == 0 ||
    is_null(response.json.next_cursor) ||
    state.total_fetched >= 10000
```

## Pagination with Iteration

When using `iterate`, pagination runs per iteration:

```yaml
endpoints:
  user_orders:
    iterate:
      over: "queue.user_ids"
      into: "state.user_id"
      concurrency: 5

    state:
      page: 1

    request:
      url: "{state.base_url}/users/{state.user_id}/orders"
      parameters:
        page: "{state.page}"

    pagination:
      next_state:
        page: "{state.page + 1}"
      stop_condition: 'length(response.records) == 0'
```

Each user ID gets its own pagination cycle.

## Next State Dependencies

If `next_state` depends on `response.*`, Sling waits for the current request. Otherwise, next request is prepared in parallel:

```yaml
# Sequential - depends on response
pagination:
  next_state:
    cursor: "{response.json.next_cursor}"

# Can parallelize - depends only on state
pagination:
  next_state:
    page: "{state.page + 1}"
```

## Default Stop Condition

Set a default stop condition for all endpoints:

```yaml
defaults:
  pagination:
    stop_condition: "response.json.has_more == false"
```

## Related Topics

- [VARIABLES.md](VARIABLES.md) - State variables in pagination
- [RESPONSE.md](RESPONSE.md) - Accessing response data
- [QUEUES.md](QUEUES.md) - Iteration with queues
