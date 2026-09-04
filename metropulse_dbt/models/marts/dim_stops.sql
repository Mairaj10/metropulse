SELECT
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station

FROM {{ ref('stg_gtfs_stops') }}