{{ config(materialized='table') }}

SELECT
realtime_trip_id,
static_trip_id,
realtime_trip_start_date,
stop_id,
route_id,
ingested_at_utc,
predicted_arrival_time,
scheduled_arrival_utc,
DATEDIFF('second', scheduled_arrival_utc, predicted_arrival_time) as predicted_delay_seconds
FROM {{ ref('int_stop_prediction_comparisons') }}