{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['realtime_trip_id', 'stop_id', 'ingested_at_utc']
) }}

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


{% if is_incremental() %}

WHERE ingested_at_utc > (
    SELECT DATEADD(
        'hour',
        -1,
        MAX(ingested_at_utc)
    )
    FROM {{ this }}
)
{% endif %}