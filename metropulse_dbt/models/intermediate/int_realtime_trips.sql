SELECT DISTINCT
    trip_id,
    start_date,
    start_time,
    route_id
FROM {{ ref('stg_gtfs_rt_stop_updates') }}