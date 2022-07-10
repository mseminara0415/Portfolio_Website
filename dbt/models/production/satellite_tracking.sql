-- Set configuration settings
{{ config(
    materialized='view'
) }}

WITH satellites_to_track AS (
    SELECT
        DISTINCT id
    FROM {{ref('stg_satellite_position_ft')}} AS sat_position
    WHERE sat_position.still_in_orbit

    UNION

    SELECT
        DISTINCT id
    FROM {{ref('stg_satellite_tle_ft')}} AS satellite_tle
),

-- Get most recent record for satellite position
newest_satellite_position AS (
    SELECT
        DISTINCT ON (satellites_to_track.id)
        satellites_to_track.id,
        sat_position.timestamp AS position_timestamp,
        sat_position.latitude,
        sat_position.longitude,
        sat_position.altitude
    FROM satellites_to_track
    LEFT JOIN {{ref('stg_satellite_position_ft')}} AS sat_position
        ON satellites_to_track.id = sat_position.id
    ORDER BY satellites_to_track.id, sat_position.timestamp DESC
),

-- Get most recent record for satellite tle
newest_satellite_tle AS (
    SELECT
        DISTINCT ON (satellites_to_track.id)
        satellites_to_track.id,
        sat_tle.timestamp,
        sat_tle.timestamp AS tle_timestamp,
        sat_tle.line_1,
        sat_tle.line_2
    FROM satellites_to_track
    LEFT JOIN {{ref('stg_satellite_tle_ft')}} AS sat_tle
        ON satellites_to_track.id = sat_tle.id
    ORDER BY satellites_to_track.id, sat_tle.timestamp DESC
),

-- Build final query set
final AS (
    SELECT
        satellites_to_track.id,
        newest_satellite_position.position_timestamp,
        newest_satellite_position.latitude,
        newest_satellite_position.longitude,
        newest_satellite_position.altitude,
        newest_satellite_tle.tle_timestamp,
        newest_satellite_tle.line_1,
        newest_satellite_tle.line_2
    FROM satellites_to_track
    LEFT JOIN newest_satellite_position
        ON satellites_to_track.id = newest_satellite_position.id
    LEFT JOIN newest_satellite_tle
        ON satellites_to_track.id = newest_satellite_tle.id
)

SELECT
    *
FROM final
