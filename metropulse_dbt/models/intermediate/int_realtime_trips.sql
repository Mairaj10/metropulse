SELECT DISTINCT
    trip_id,
    start_date,
    route_id
FROM {{ ref('stg_gtfs_rt_stop_updates') }}