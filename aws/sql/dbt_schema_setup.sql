-- This file goes over the steps needed to setup our
-- dbt_cloud user with access to all the schemas and tables needed
-- when running dbt.
--
-- Extremely helpful article on setting up dbt permissions within Postgre database
-- https://mattmazur.com/2018/06/12/wrangling-dbt-database-permissions/

-- Step 1: Create desired schema
CREATE SCHEMA schema_name_here;

-- Step 2: Give dbt user full access the 'schema_name_here;
GRANT ALL ON SCHEMA schema_name_here TO dbt_user;

-- Step 3: Make the dbt user owner of the new schema
ALTER SCHEMA schema_name_here OWNER TO dbt_user;

-- Step 4: Give dbt user read access to the source tables
GRANT USAGE ON SCHEMA schema_name_here TO dbt_user;
GRANT SELECT ON ALL TABLES IN SCHEMA schema_name_here TO dbt_user;


