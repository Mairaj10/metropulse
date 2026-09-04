with base as (SELECT
    trip_matches.realtime_trip_id,
    trip_matches.static_trip_id,
    trip_matches.realtime_trip_start_date,
    realtime_updates.stop_id,
    trip_matches.route_id,
    realtime_updates.ingested_at_utc,
    realtime_updates.arrival_time AS predicted_arrival_time,
    try_to_number(split_part(stop_times.arrival_time, ':', 1)) AS scheduled_arrival_hour,
    try_to_number(split_part(stop_times.arrival_time, ':', 2)) AS scheduled_arrival_minute,
    try_to_number(split_part(stop_times.arrival_time, ':', 3)) AS scheduled_arrival_second

FROM {{ ref('int_realtime_trip_matches') }} AS trip_matches

INNER JOIN {{ ref('stg_gtfs_rt_stop_updates') }} AS realtime_updates
    ON trip_matches.realtime_trip_id = realtime_updates.trip_id
    AND trip_matches.realtime_trip_start_date = realtime_updates.start_date

INNER JOIN {{ ref('stg_gtfs_stop_times') }} AS stop_times
    ON trip_matches.static_trip_id = stop_times.trip_id
    AND realtime_updates.stop_id = stop_times.stop_id

WHERE trip_matches.match_status = 'matched')

SELECT
    realtime_trip_id,
    static_trip_id,
    realtime_trip_start_date,
    stop_id,
    route_id,
    ingested_at_utc,
    predicted_arrival_time,

    CONVERT_TIMEZONE(
        'UTC',
        TIMESTAMP_TZ_FROM_PARTS(
            YEAR(realtime_trip_start_date),
            MONTH(realtime_trip_start_date),
            DAY(realtime_trip_start_date),
            scheduled_arrival_hour,
            scheduled_arrival_minute,
            scheduled_arrival_second,
            0,
            'America/New_York'
        )
    ) AS scheduled_arrival_utc

FROM base



