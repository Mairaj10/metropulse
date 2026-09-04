SELECT
    trip_id,
    trip_headsign,
    route_id,
    service_id,
    shape_id,
    direction_id
FROM {{ ref('stg_gtfs_trips') }}