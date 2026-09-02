SELECT
COUNT(*),
trip_id,
stop_id,
ingested_at_utc
FROM {{ ref("stg_gtfs_rt_stop_updates")}}
GROUP BY trip_id, stop_id, ingested_at_utc
HAVING COUNT(*) > 1