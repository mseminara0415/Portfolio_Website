-- Create satellite position fact table within the raw schema
CREATE TABLE IF NOT EXISTS raw.satellite_position_ft (
    name varchar(255) NOT NULL,
    norad_id  int NOT NULL,
    latitude numeric NOT NULL,
    longitude numeric NOT NULL,
    altitude numeric,
    velocity numeric NOT NULL,
    visibility varchar(255),
    footprint numeric,
    position_timestamp bigint NOT NULL,
    daynum numeric,
    solar_lat numeric,
    solar_lon numeric,
    units varchar(255)
)