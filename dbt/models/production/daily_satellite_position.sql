-- Set configuration settings
{{ config(
    materialized='view'
) }}

WITH daily_satellite_stats AS (
    SELECT
        DATE_TRUNC('day', timestamp) AS day,
        id AS satellite_id,
        AVG(velocity_mph) AS average_velocity_mph,
        AVG(altitude) AS average_altitude_miles
    FROM {{ref('stg_satellite_position_ft')}} AS sat_position
    GROUP BY id, DATE_TRUNC('day', timestamp)
)

SELECT
    *
FROM daily_satellite_stats
