SELECT
    realtime_trip_id,
    stop_id,
    ingested_at_utc,
    COUNT(*) AS row_count

FROM {{ ref('int_stop_prediction_comparisons') }}

GROUP BY
    realtime_trip_id,
    stop_id,
    ingested_at_utc

HAVING COUNT(*) > 1