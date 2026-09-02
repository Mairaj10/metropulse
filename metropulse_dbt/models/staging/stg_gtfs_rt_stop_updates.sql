SELECT
ingested_at_utc,
convert_timezone(
    'UTC',
    to_timestamp_tz(feed_generated_at_epoch, 0)
) AS feed_generated_at,
entity_id,
trip_id,
route_id,
try_to_date(start_date, 'YYYYMMDD') AS start_date,
start_time,
convert_timezone(
    'UTC',
    to_timestamp_tz(trip_update_timestamp_epoch, 0)
) AS trip_update_timestamp,
stop_id,
stop_sequence,
convert_timezone(
    'UTC',
    to_timestamp_tz(arrival_time_epoch, 0)
) AS arrival_time,
convert_timezone(
    'UTC',
    to_timestamp_tz(departure_time_epoch, 0)
) AS departure_time

FROM {{ source('raw', 'gtfs_rt_stop_updates') }}