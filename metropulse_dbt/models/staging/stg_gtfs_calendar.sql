SELECT 

    service_id,
    try_cast(monday as integer) as monday,
    try_cast(tuesday as integer) as tuesday,
    try_cast(wednesday as integer) as wednesday,
    try_cast(thursday as integer) as thursday,
    try_cast(friday as integer) as friday,
    try_cast(saturday as integer) as saturday,
    try_cast(sunday as integer) as sunday,
    try_to_date(start_date, 'YYYYMMDD') AS start_date,
    try_to_date(end_date, 'YYYYMMDD') AS end_date,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc

FROM {{ source('raw', 'gtfs_calendar') }}