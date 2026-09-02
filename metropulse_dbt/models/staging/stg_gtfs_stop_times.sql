SELECT
    trip_id,
    stop_id,
    arrival_time,
    departure_time,
    try_cast(stop_sequence as integer) as stop_sequence,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc

FROM {{ source('raw', 'gtfs_stop_times') }}