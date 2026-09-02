 SELECT
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    try_cast(route_type as integer) as route_type,
    route_color,
    route_text_color, 
    try_cast(route_sort_order as integer) as route_sort_order,
    try_cast(loaded_at_utc as timestamp_tz) as loaded_at_utc
 
 FROM {{ source('raw', 'gtfs_routes') }}