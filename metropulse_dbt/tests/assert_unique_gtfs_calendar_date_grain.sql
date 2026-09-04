SELECT 
count(*),
service_id,
date
FROM {{ ref("stg_gtfs_calendar_dates") }}
GROUP BY service_id, date
HAVING count(*) > 1