/**
mode: incremental
update_key: ts
unique_key: [id, k]
hooks:
  start:
    - type: log
      message: "name={model.name} schema={model.schema} database=[{model.database}] full_name={model.full_name} mode={model.mode} update_key={model.update_key} unique_key={model.unique_key}"
    # KILL goes through Exec on ClickHouse, else the driver returns "bad connection"
    - type: query
      connection: '{target.name}'
      query: |
        KILL MUTATION WHERE database = '{model.schema}' AND table = '{model.name}'
          AND command LIKE '%_sling_build_tmp_{model.name}_%'
**/
SELECT toUInt64(number) AS id, 'a' AS k, now() AS ts FROM numbers(3)
