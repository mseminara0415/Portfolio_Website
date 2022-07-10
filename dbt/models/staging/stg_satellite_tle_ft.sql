-- Set configuration settings
{{ config(
    materialized='view'
) }}

-- Stage satellite tle data from our raw schema
WITH staged_satellite_tle_ft AS (
    SELECT
        id,
        LOWER(name) AS name,
        TO_TIMESTAMP(requested_timestamp) AS timestamp,
        source,
        line1 AS line_1,
        line2 AS line_2
    FROM raw.satellite_tle_ft
)

SELECT
    *
FROM staged_satellite_tle_ft