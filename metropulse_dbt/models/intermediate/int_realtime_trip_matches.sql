SELECT realtime_trips.trip_id AS realtime_trip_id, 
static_trips.trip_id AS static_trip_id,
 realtime_trips.start_date AS realtime_trip_start_date,
 realtime_trips.start_time AS realtime_trip_start_time,
 realtime_trips.route_id,
  active_services.service_id,
  CASE WHEN static_trips.trip_id IS NOT NULL THEN 'matched' ELSE 'unmatched' END AS match_status
   FROM {{ ref('int_realtime_trips') }} as realtime_trips 
   LEFT JOIN {{ ref('int_active_services_by_date') }} as active_services
    ON realtime_trips.start_date = active_services.service_date 
    LEFT JOIN {{ ref('stg_gtfs_trips') }} as static_trips
     ON realtime_trips.route_id = static_trips.route_id 
     AND active_services.service_id = static_trips.service_id 
     AND RIGHT( static_trips.trip_id, LENGTH(realtime_trips.trip_id) ) = realtime_trips.trip_id

