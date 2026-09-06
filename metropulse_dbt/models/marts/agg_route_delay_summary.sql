WITH base AS (

    SELECT
        f.route_id,

        CONVERT_TIMEZONE(
            'America/New_York',
            f.predicted_arrival_time
        ) AS predicted_arrival_ny,

        f.predicted_delay_seconds

    FROM {{ ref('fct_stop_predictions') }} AS f

    INNER JOIN {{ ref('dashboard_route_scope') }} AS s
        ON f.route_id = s.route_id

),

prepared AS (

    SELECT
        route_id,
        TO_DATE(predicted_arrival_ny) AS predicted_arrival_date_ny,
        EXTRACT(HOUR FROM predicted_arrival_ny) AS predicted_arrival_hour_ny,
        predicted_delay_seconds

    FROM base

)

SELECT
    route_id,
    predicted_arrival_date_ny,
    predicted_arrival_hour_ny,
    {{ predicted_delay_metrics('predicted_delay_seconds') }}

FROM prepared

GROUP BY
    route_id,
    predicted_arrival_date_ny,
    predicted_arrival_hour_ny