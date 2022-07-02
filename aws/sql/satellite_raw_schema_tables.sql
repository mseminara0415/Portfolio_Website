-- Create satellite position fact table within the raw schema
CREATE TABLE IF NOT EXISTS raw.satellite_position_ft (
    name varchar(255) NOT NULL,
    id int NOT NULL,
    latitude numeric NOT NULL,
    longitude numeric NOT NULL,
    altitude numeric NOT NULL,
    velocity numeric NOT NULL,
    visibility varchar(255),
    footprint numeric,
    position_timestamp bigint NOT NULL,
    daynum numeric,
    solar_lat numeric,
    solar_lon numeric,
    units varchar(255),
    source text NOT NULL
);

-- Create satellite tle fact table within the raw schema
CREATE TABLE IF NOT EXISTS raw.satellite_tle_ft (
    id int NOT NULL,
    name varchar(255) NOT NULL,
    header varchar(255),
    tle_line_1 varchar(255),
    tle_line_2 varchar(255),
    requested_timestamp bigint,
    tle_timestamp bigint,
    source text NOT NULL
);

-- Create satellite wikipedia dimension table within the raw schema
CREATE TABLE IF NOT EXISTS raw.satellite_detail_dm (
    name varchar(255) NOT NULL,
    description text,
    application varchar,
    operator varchar,
    cospar_id varchar,
    call_sign varchar,
    manufacturer varchar,
    mission_duration varchar,
    mission_type varchar,
    spacecraft_type varchar,
    satellite_id int NOT NULL,
    satellite_bus varchar,
    launch_mass_lbs int,
    launch_date date,
    launch_site varchar,
    reference_system varchar,
    rocket varchar,
    website varchar,
    wikipedia_link varchar,
);