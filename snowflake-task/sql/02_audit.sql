-- The audit log every load procedure writes to, and the helper that the CORE
-- and MART procedures call after each MERGE

USE ROLE AIRLINE_ETL;
USE WAREHOUSE AIRLINE_WH;
USE DATABASE AIRLINE_DWH;

-- One row per table per procedure call. LOG_ID is ORDER, so it follows
-- insertion order; the default, NOORDER, does not
CREATE TABLE IF NOT EXISTS AUDIT.LOAD_LOG (
  LOG_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER PRIMARY KEY,
  RUN_ID          VARCHAR NOT NULL,
  PROCEDURE_NAME  VARCHAR NOT NULL,
  TARGET_TABLE    VARCHAR NOT NULL,
  ROWS_INSERTED   NUMBER DEFAULT 0 NOT NULL,
  ROWS_UPDATED    NUMBER DEFAULT 0 NOT NULL,
  STATUS          VARCHAR NOT NULL,
  ERROR_MESSAGE   VARCHAR,
  STARTED_AT      TIMESTAMP_LTZ NOT NULL,
  FINISHED_AT     TIMESTAMP_LTZ NOT NULL
);

-- Logs one MERGE: inserted = rows after - rows before; the other rows the
-- MERGE touched were updates. That holds while only one load writes at a time,
-- which the DAG guarantees with max_active_runs=1
CREATE OR REPLACE PROCEDURE AUDIT.LOG_MERGE(
  RUN_ID VARCHAR, PROCEDURE_NAME VARCHAR, TARGET_TABLE VARCHAR,
  ROWS_BEFORE NUMBER, ROWS_AFFECTED NUMBER, STARTED_AT TIMESTAMP_LTZ)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  full_name   VARCHAR DEFAULT 'AIRLINE_DWH.' || TARGET_TABLE;
  rows_after  NUMBER;
  inserted    NUMBER;
  updated     NUMBER;
BEGIN
  SELECT COUNT(*) INTO :rows_after FROM IDENTIFIER(:full_name);
  inserted := rows_after - ROWS_BEFORE;
  updated := ROWS_AFFECTED - inserted;
  INSERT INTO AIRLINE_DWH.AUDIT.LOAD_LOG
    (RUN_ID, PROCEDURE_NAME, TARGET_TABLE, ROWS_INSERTED, ROWS_UPDATED,
     STATUS, STARTED_AT, FINISHED_AT)
  VALUES
    (:RUN_ID, :PROCEDURE_NAME, :TARGET_TABLE, :inserted, :updated,
     'SUCCESS', :STARTED_AT, CURRENT_TIMESTAMP());
  RETURN TARGET_TABLE || ': ' || inserted || ' inserted, ' || updated || ' updated';
END;
$$;
