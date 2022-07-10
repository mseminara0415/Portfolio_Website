-- Oribital speed calculation: √((G × M) / (R + h)), units must be in metric (meter, kilogram, second). Gives answer in m/s.
-- gravitational constant of earth (G) 6.674 * 10^-11m^3 (.00000000006674)
-- earths mass (M) 5.972 x 10^24 kg
-- earths radius (R) 6,371,000 meters
-- height of satellite (h)

{% macro altitude_to_orbital_velocity_mph(altitude_column, earth_gravitational_constant, earth_mass_kg, earth_radius_meters, meters_per_second_to_mph_conversion_rate) %}
    SQRT(({{ earth_gravitational_constant }} * {{ earth_mass_kg }}) / ({{ earth_radius_meters }} + ({{ altitude_column }} * 1609))) * {{ meters_per_second_to_mph_conversion_rate }}
{% endmacro %}

