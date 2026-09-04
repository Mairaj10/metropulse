SELECT
    COUNT(*) AS duplicate_count,
    realtime_trip_id,
    realtime_trip_start_date
FROM {{ ref('int_realtime_trip_matches') }}
GROUP BY
    realtime_trip_id,
    realtime_trip_start_date
HAVING COUNT(*) > 1