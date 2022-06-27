-- Create satellite position fact table within the raw schema
CREATE TABLE IF NOT EXISTS raw.satellite_tle_ft (
    id int NOT NULL,
    name varchar(255) NOT NULL,
    header varchar(255),
    tle_line_1 varchar(255),
    tle_line_2 varchar(255),
    requested_timestamp bigint,
    tle_timestamp bigint,
    source text NOT NULL
)