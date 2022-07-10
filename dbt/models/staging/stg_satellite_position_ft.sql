-- Set configuration settings
{{ config(
    materialized='view'
) }}

-- Set needed variables to calculate orbital velocity
{% set earth_gravitational_constant = 0.00000000006674 %}
{% set earth_mass_kg = 5972000000000000000000000 %}
{% set earth_radius_meters = 6371000 %}
{% set meters_per_second_to_mph_conversion_rate = 2.237 %}

-- Stage satellite position data from our raw schema
WITH staged_satellite_position_ft AS (
    SELECT
        LOWER(name) AS name,
        id,
        latitude,
        longitude,
        altitude,
        CASE
            WHEN velocity IS NULL AND units = 'miles'
                THEN {{ altitude_to_orbital_velocity_mph('altitude', earth_gravitational_constant, earth_mass_kg, earth_radius_meters, meters_per_second_to_mph_conversion_rate) }}
            WHEN velocity IS NULL AND units != 'miles'
                THEN NULL
            ELSE velocity
        END AS velocity_mph,
        CASE
            WHEN visibility = 'daylight'
                THEN 'visible'
            ELSE visibility
        END AS visibility,
        TO_TIMESTAMP(timestamp) AS timestamp,
        daynum,
        solar_lat,
        solar_lon,
        units,
        source,
        NOT altitude = 0 AS still_in_orbit
    FROM raw.satellite_position_ft
)


SELECT
    *
FROM staged_satellite_position_ft