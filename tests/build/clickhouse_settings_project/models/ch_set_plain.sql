SELECT toUInt64(number + 1) AS id,
       toDate('2024-01-01') + number AS event_day,
       toDateTime('2024-01-01 00:00:00') + number * 86400 AS ts
FROM numbers({% if extra %}3{% else %}2{% endif %})
