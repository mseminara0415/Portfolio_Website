-- Set configuration settings
{{ config(
    materialized='view'
) }}

WITH satellite_details AS (
    SELECT
        satellite_id,
        name,
        description,
        operator,
        cospar_id,
        manufacturer,
        mission_duration,
        mission_type,
        spacecraft_type,
        satellite_bus,
        launch_mass_lbs,
        launch_date,
        AGE(launch_date) AS time_since_launch,
        launch_site,
        reference_system,
        rocket,
        satellite_website,
        wikipedia_link
    FROM {{ref('stg_satellite_detail_dm')}} AS sat_details
)

SELECT
    *
FROM satellite_details