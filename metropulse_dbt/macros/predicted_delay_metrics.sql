{% macro predicted_delay_metrics(delay_column) %}

{% set tolerance = var('delay_tolerance_seconds') %}

    COUNT({{ delay_column }}) AS prediction_count,

    ROUND(
        AVG({{ delay_column }}),
        2
    ) AS avg_predicted_delay_seconds,

    ROUND(
        MEDIAN({{ delay_column }}),
        2
    ) AS median_predicted_delay_seconds,

    ROUND(
        100.0 * COUNT_IF({{ delay_column }} > {{ tolerance }})
        / NULLIF(COUNT({{ delay_column }}), 0),
        2
    ) AS pct_predicted_late,

    ROUND(
        100.0 * COUNT_IF({{ delay_column }} < -{{ tolerance }})
        / NULLIF(COUNT({{ delay_column }}), 0),
        2
    ) AS pct_predicted_early,

    ROUND(
        100.0 * COUNT_IF({{ delay_column }} BETWEEN -{{ tolerance }} AND {{ tolerance }})
        / NULLIF(COUNT({{ delay_column }}), 0),
        2
    ) AS pct_roughly_on_time

{% endmacro %}