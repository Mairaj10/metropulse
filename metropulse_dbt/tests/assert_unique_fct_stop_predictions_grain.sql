SELECT 
COUNT(*) AS duplicate_count,
realtime_trip_id,
stop_id,
ingested_at_utc
FROM {{ ref("fct_stop_predictions")}}
GROUP BY realtime_trip_id, stop_id, ingested_at_utc
HAVING COUNT(*) > 1