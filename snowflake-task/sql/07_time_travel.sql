-- Time travel: two DDL and two DML statements, run against the CDC test of
-- 26 October 2026 (README, "Results")
--
-- The two moments come from the audit log, by Airflow run id. For your own
-- runs, take the ids from `airflow dags list-runs airline_dwh_load`: the first
-- is the incremental load, the second the run right after it. Time travel
-- keeps one day of history by default, so this works for a day after the runs

USE ROLE AIRLINE_ETL;
USE WAREHOUSE AIRLINE_WH;
USE DATABASE AIRLINE_DWH;

SET before_changes = (SELECT MIN(STARTED_AT) FROM AIRLINE_DWH.AUDIT.LOAD_LOG
                      WHERE RUN_ID = 'manual__2026-10-26T08:21:22.522277+00:00');  -- change load starts
SET after_changes  = (SELECT MIN(STARTED_AT) FROM AIRLINE_DWH.AUDIT.LOAD_LOG
                      WHERE RUN_ID = 'manual__2026-10-26T08:21:25.259899+00:00');  -- its re-run starts
SELECT $before_changes AS before_changes, $after_changes AS after_changes;

-- DML 1: what the change load did to flight statuses, read at two points in time
SELECT b.FLIGHT_STATUS AS status_before, a.FLIGHT_STATUS AS status_after, COUNT(*) AS bookings
FROM CORE.FACT_FLIGHT AT(TIMESTAMP => $before_changes::TIMESTAMP_LTZ) b
JOIN CORE.FACT_FLIGHT AT(TIMESTAMP => $after_changes::TIMESTAMP_LTZ) a
  ON a.PASSENGER_ID = b.PASSENGER_ID
WHERE a.FLIGHT_STATUS <> b.FLIGHT_STATUS
GROUP BY status_before, status_after
ORDER BY status_before, status_after;

-- DDL 1: a zero-copy clone of the fact table as the initial load left it
CREATE OR REPLACE TABLE CORE.FACT_FLIGHT_BEFORE_CHANGES
  CLONE CORE.FACT_FLIGHT AT(TIMESTAMP => $before_changes::TIMESTAMP_LTZ);
SELECT COUNT(*) AS flights, COUNT_IF(FLIGHT_STATUS = 'Cancelled') AS cancelled
FROM CORE.FACT_FLIGHT_BEFORE_CHANGES;

-- DML 2: undo a mistaken DELETE. Select all four statements and run them together.
DELETE FROM MART.FLIGHTS_DAILY WHERE DEPARTURE_DATE >= '2022-12-01';  -- December is gone
SET oops = LAST_QUERY_ID();
INSERT INTO MART.FLIGHTS_DAILY
SELECT * FROM MART.FLIGHTS_DAILY BEFORE(STATEMENT => $oops)
WHERE DEPARTURE_DATE >= '2022-12-01';
SELECT COUNT(*) AS december_rows FROM MART.FLIGHTS_DAILY WHERE DEPARTURE_DATE >= '2022-12-01';

-- DDL 2: drop the clone by mistake, then bring it back
DROP TABLE CORE.FACT_FLIGHT_BEFORE_CHANGES;
UNDROP TABLE CORE.FACT_FLIGHT_BEFORE_CHANGES;
SELECT COUNT(*) AS flights FROM CORE.FACT_FLIGHT_BEFORE_CHANGES;
