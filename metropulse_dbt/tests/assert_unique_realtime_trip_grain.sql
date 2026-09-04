SELECT
    COUNT(*) AS duplicate_count,
    trip_id,
    start_date
FROM {{ ref('int_realtime_trips') }}
GROUP BY trip_id, start_date
HAVING COUNT(*) > 1