SELECT 
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    try_cast(direction_id as integer) as direction_id,
    shape_id,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc
FROM {{ source('raw', 'gtfs_trips') }}