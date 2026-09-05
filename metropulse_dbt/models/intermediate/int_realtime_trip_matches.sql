WITH active_static_trips AS (

    SELECT
        active_services.service_date,
        static_trips.trip_id,
        static_trips.route_id,
        static_trips.service_id

    FROM {{ ref('stg_gtfs_trips') }} AS static_trips

    INNER JOIN {{ ref('int_active_services_by_date') }} AS active_services
        ON static_trips.service_id = active_services.service_id
)

SELECT
    realtime_trips.trip_id AS realtime_trip_id,
    active_static_trips.trip_id AS static_trip_id,
    realtime_trips.start_date AS realtime_trip_start_date,
    realtime_trips.route_id,
    active_static_trips.service_id,

    CASE
        WHEN active_static_trips.trip_id IS NOT NULL THEN 'matched'
        ELSE 'unmatched'
    END AS match_status

FROM {{ ref('int_realtime_trips') }} AS realtime_trips

LEFT JOIN active_static_trips
    ON realtime_trips.start_date = active_static_trips.service_date
    AND realtime_trips.route_id = active_static_trips.route_id
    AND RIGHT(
        active_static_trips.trip_id,
        LENGTH(realtime_trips.trip_id)
    ) = realtime_trips.trip_id

