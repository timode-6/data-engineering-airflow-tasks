-- Setup for the DB clone tool. Run it as ACCOUNTADMIN with Run all;
-- running it again is safe.
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 1. Roles
--    DEVELOPMENT owns dev databases, READ_ONLY_ROLE only reads them,
--    CLONE_TOOL is the role the Streamlit app runs as.
CREATE ROLE IF NOT EXISTS DEVELOPMENT;
CREATE ROLE IF NOT EXISTS READ_ONLY_ROLE;
CREATE ROLE IF NOT EXISTS CLONE_TOOL;

-- custom roles go under SYSADMIN, so admins keep control of what they own
GRANT ROLE DEVELOPMENT TO ROLE SYSADMIN;
GRANT ROLE READ_ONLY_ROLE TO ROLE SYSADMIN;
GRANT ROLE CLONE_TOOL TO ROLE SYSADMIN;

-- granted to you as well, so you can switch to them and check the result.
-- The name is quoted so it matches exactly, even with lowercase letters,
-- dots or an @ in it.
SET me = '"' || CURRENT_USER() || '"';
GRANT ROLE DEVELOPMENT TO USER IDENTIFIER($me);
GRANT ROLE READ_ONLY_ROLE TO USER IDENTIFIER($me);
GRANT ROLE CLONE_TOOL TO USER IDENTIFIER($me);


-- 2. What the app's role needs, and why
-- a clone is a new database
GRANT CREATE DATABASE ON ACCOUNT TO ROLE CLONE_TOOL;
-- everything DEVELOPMENT can do: read DEV to clone it, and drop an earlier
-- clone that already belongs to DEVELOPMENT when it's recreated
GRANT ROLE DEVELOPMENT TO ROLE CLONE_TOOL;
-- grants on future schemas, tables and views need MANAGE GRANTS:
-- owning the database isn't enough for those
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE CLONE_TOOL;
-- the app's queries run on a warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE CLONE_TOOL;
-- developers and readers need one too, to query their clones
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DEVELOPMENT;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE READ_ONLY_ROLE;


-- 3. Where the deployed app will live
CREATE DATABASE IF NOT EXISTS TOOLS;
CREATE SCHEMA IF NOT EXISTS TOOLS.APPS;
GRANT USAGE ON DATABASE TOOLS TO ROLE CLONE_TOOL;
GRANT USAGE ON SCHEMA TOOLS.APPS TO ROLE CLONE_TOOL;
GRANT CREATE STREAMLIT ON SCHEMA TOOLS.APPS TO ROLE CLONE_TOOL;


-- 4. DEV, the database to clone from: a schema with a table and a view
CREATE DATABASE IF NOT EXISTS DEV;
CREATE SCHEMA IF NOT EXISTS DEV.SALES;
USE SCHEMA DEV.SALES;

CREATE OR REPLACE TABLE ORDERS AS
SELECT
    column1 AS order_id,
    column2::DATE AS order_date,
    column3 AS customer,
    column4::NUMBER(10, 2) AS amount
FROM VALUES
    (1, '2025-11-01', 'Alice', 120.50),
    (2, '2025-11-01', 'Bob', 80.00),
    (3, '2025-11-02', 'Alice', 42.10),
    (4, '2025-11-03', 'Carol', 310.00);

-- ORDERS without a database name: in a clone, the view reads the clone's
-- table, not DEV's
CREATE OR REPLACE VIEW DAILY_REVENUE AS
SELECT order_date, SUM(amount) AS revenue
FROM ORDERS
GROUP BY order_date;

-- all of DEV belongs to DEVELOPMENT, like a real dev database would
GRANT OWNERSHIP ON DATABASE DEV TO ROLE DEVELOPMENT COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE DEV TO ROLE DEVELOPMENT COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE DEV TO ROLE DEVELOPMENT COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE DEV TO ROLE DEVELOPMENT COPY CURRENT GRANTS;


-- 5. Workspaces runs Streamlit apps on a compute pool, and the deployed app
--    runs as CLONE_TOOL, so that role has to be able to use the pool
GRANT USAGE ON COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU TO ROLE CLONE_TOOL;
-- creating a Streamlit app in a workspace needs a default warehouse on your user
ALTER USER IDENTIFIER($me) SET DEFAULT_WAREHOUSE = COMPUTE_WH;
