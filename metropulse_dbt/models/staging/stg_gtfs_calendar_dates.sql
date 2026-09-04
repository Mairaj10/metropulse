SELECT
    service_id,
    try_to_date(date, 'YYYYMMDD') AS date,
    try_cast(exception_type as integer) AS exception_type,
   CASE
    WHEN try_cast(exception_type AS integer) = 1 THEN 'add_service'
    WHEN try_cast(exception_type AS integer) = 2 THEN 'remove_service'
END AS exception_action,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc

FROM {{ source('raw', 'gtfs_calendar_dates') }}