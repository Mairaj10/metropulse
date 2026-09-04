WITH realtime_dates AS (

    SELECT DISTINCT
        start_date AS service_date
    FROM {{ ref('stg_gtfs_rt_stop_updates') }}
    WHERE start_date IS NOT NULL

),

calendar AS (

    SELECT
        service_id,
        start_date,
        end_date,
        monday,
        tuesday,
        wednesday,
        thursday,
        friday,
        saturday,
        sunday
    FROM {{ ref('stg_gtfs_calendar') }}

),

dated_service as (SELECT
    realtime_dates.service_date,
    dayname(realtime_dates.service_date) AS weekday_name,
    calendar.service_id,
    calendar.start_date,
    calendar.end_date,
    calendar.monday,
    calendar.tuesday,
    calendar.wednesday,
    calendar.thursday,
    calendar.friday,
    calendar.saturday,
    calendar.sunday

FROM realtime_dates

JOIN calendar
    ON realtime_dates.service_date
       BETWEEN calendar.start_date AND calendar.end_date),

normal_service AS (SELECT service_date,
       weekday_name,
       service_id,
       CASE WHEN weekday_name = 'Mon' THEN monday
            WHEN weekday_name = 'Tue' THEN tuesday
            WHEN weekday_name = 'Wed' THEN wednesday
            WHEN weekday_name = 'Thu' THEN thursday
            WHEN weekday_name = 'Fri' THEN friday
            WHEN weekday_name = 'Sat' THEN saturday
            WHEN weekday_name = 'Sun' THEN sunday
       END AS runs_on_day
FROM dated_service),

final_service AS (SELECT
    normal_service.service_date,
    normal_service.weekday_name,
    normal_service.service_id,
    normal_service.runs_on_day,
    calendar_dates.exception_type,
    CASE
        WHEN calendar_dates.exception_type = 1 THEN 1
        WHEN calendar_dates.exception_type = 2 THEN 0
        ELSE normal_service.runs_on_day
    END AS is_active_service
FROM normal_service
LEFT JOIN {{ ref("stg_gtfs_calendar_dates") }} AS calendar_dates
    ON normal_service.service_id = calendar_dates.service_id
    AND normal_service.service_date = calendar_dates.date)

SELECT *
FROM final_service
WHERE is_active_service = 1
