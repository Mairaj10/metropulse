SELECT 
    COUNT(*) AS duplicate_count,
    trip_id,
    stop_sequence
FROM {{ ref('stg_gtfs_stop_times') }}
group by trip_id, stop_sequence
HAVING count(*) > 1