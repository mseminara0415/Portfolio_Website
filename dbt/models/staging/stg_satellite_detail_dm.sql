-- Set configuration settings
{{ config(
    materialized='view'
) }}

WITH staged_satellite_detail_dm AS (
    SELECT
        satellite_id,
        LOWER(name) AS name,
        description,
        SPLIT_PART(operator, '[', 1) AS operator,
        cospar_id,
        manufacturer,
        mission_duration,
        mission_type,
        spacecraft_type,
        SPLIT_PART(satellite_bus,'[', 1) AS satellite_bus,
        launch_mass_lbs,
        launch_date,
        SPLIT_PART(launch_site,'[', 1) AS launch_site,
        SPLIT_PART(reference_system,'[', 1) AS reference_system,
        SPLIT_PART(rocket,'[',1) AS rocket,
        website AS satellite_website,
        wikipedia_link
    FROM raw.satellite_detail_dm
)

SELECT
    *
FROM staged_satellite_detail_dm