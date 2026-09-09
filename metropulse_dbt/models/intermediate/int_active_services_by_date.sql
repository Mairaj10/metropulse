WITH realtime_dates AS (

    SELECT DISTINCT
        start_date AS service_date,
        dayname(start_date) AS weekday_name

    FROM {{ ref('stg_gtfs_rt_stop_updates') }}

    WHERE start_date IS NOT NULL

),

normal_active_services AS (

    SELECT
        realtime_dates.service_date,
        realtime_dates.weekday_name,
        calendar.service_id

    FROM realtime_dates

    JOIN {{ ref('stg_gtfs_calendar') }} AS calendar
        ON realtime_dates.service_date
           BETWEEN calendar.start_date AND calendar.end_date

    WHERE
        CASE
            WHEN realtime_dates.weekday_name = 'Mon' THEN calendar.monday
            WHEN realtime_dates.weekday_name = 'Tue' THEN calendar.tuesday
            WHEN realtime_dates.weekday_name = 'Wed' THEN calendar.wednesday
            WHEN realtime_dates.weekday_name = 'Thu' THEN calendar.thursday
            WHEN realtime_dates.weekday_name = 'Fri' THEN calendar.friday
            WHEN realtime_dates.weekday_name = 'Sat' THEN calendar.saturday
            WHEN realtime_dates.weekday_name = 'Sun' THEN calendar.sunday
        END = 1

),

added_services AS (

    SELECT
        realtime_dates.service_date,
        realtime_dates.weekday_name,
        calendar_dates.service_id

    FROM realtime_dates

    JOIN {{ ref('stg_gtfs_calendar_dates') }} AS calendar_dates
        ON realtime_dates.service_date = calendar_dates.date

    WHERE calendar_dates.exception_type = 1

),

active_services AS (

    SELECT * FROM normal_active_services

    UNION

    SELECT * FROM added_services

)

SELECT
    active_services.service_date,
    active_services.weekday_name,
    active_services.service_id

FROM active_services

WHERE NOT EXISTS (

    SELECT 1

    FROM {{ ref('stg_gtfs_calendar_dates') }} AS removed

    WHERE removed.date = active_services.service_date
      AND removed.service_id = active_services.service_id
      AND removed.exception_type = 2

)
