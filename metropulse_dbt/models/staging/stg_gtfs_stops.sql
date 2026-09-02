SELECT
    stop_id,
    stop_name,
    try_cast(stop_lat as number(9,6)) as stop_lat,
    try_cast(stop_lon as number(9,6)) as stop_lon,
    try_cast(location_type as integer) as location_type,
    parent_station,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc

FROM {{ source('raw', 'gtfs_stops') }}