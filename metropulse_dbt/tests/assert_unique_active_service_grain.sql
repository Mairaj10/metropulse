SELECT COUNT(*) AS duplicate_count,
         service_date,
         service_id
FROM {{ ref('int_active_services_by_date') }}
GROUP BY service_date, service_id
HAVING COUNT(*) > 1