-- The 20 separate CALL test_2(...) statements as one loop in a
-- Snowflake Scripting procedure. Run it with a warehouse and a database
-- selected (or USE WAREHOUSE / USE DATABASE above this line).

-- 1. Only for trying it out
CREATE SCHEMA IF NOT EXISTS test_2_demo;
USE SCHEMA test_2_demo;

CREATE OR REPLACE TABLE test_2_calls (
    call_no       INTEGER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
    run_date      DATE,
    loaded_at     TIMESTAMP_NTZ,
    previous_date DATE
);

CREATE OR REPLACE PROCEDURE test_2(run_date DATE, loaded_at TIMESTAMP_NTZ, previous_date DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO test_2_calls (run_date, loaded_at, previous_date)
    VALUES (:run_date, :loaded_at, :previous_date);
    RETURN 'ok';
END;
$$;


-- 2. The loop
CREATE OR REPLACE PROCEDURE run_test_2()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Each call's last argument is the run date of the call before it, so the
    -- list only holds run dates and load timestamps, and LAG supplies the
    -- previous date (2025-01-28 before the first run)
    runs CURSOR FOR
        SELECT
            run_date,
            loaded_at,
            LAG(run_date, 1, '2025-01-28'::DATE) OVER (ORDER BY run_date) AS previous_date
        FROM (
            SELECT column1::DATE AS run_date, column2::TIMESTAMP_NTZ AS loaded_at
            FROM VALUES
                ('2025-02-06', '2025-02-12 09:38:25.999982000'),
                ('2025-02-14', '2025-02-14 16:17:14.095384000'),
                ('2025-02-20', '2025-02-21 08:41:53.643244000'),
                ('2025-02-25', '2025-03-11 15:52:28.575590000'),
                ('2025-03-06', '2025-03-13 15:35:21.729785000'),
                ('2025-03-13', '2025-03-13 16:32:27.178218000'),
                ('2025-03-20', '2025-03-26 08:35:19.585812000'),
                ('2025-03-27', '2025-03-28 07:23:03.611707000'),
                ('2025-04-07', '2025-04-08 18:57:03.804270000'),
                ('2025-04-10', '2025-04-15 11:19:51.275211000'),
                ('2025-04-14', '2025-04-15 14:34:32.097939000'),
                ('2025-04-24', '2025-04-24 14:41:48.705573000'),
                ('2025-05-02', '2025-05-08 11:05:44.640510000'),
                ('2025-05-15', '2025-05-21 10:00:08.361011000'),
                ('2025-05-22', '2025-05-28 08:07:06.096731000'),
                ('2025-05-29', '2025-05-30 10:01:45.906511000'),
                ('2025-06-05', '2025-06-09 09:22:04.668390000'),
                ('2025-06-19', '2025-07-03 08:27:40.115104000'),
                ('2025-06-26', '2025-07-03 09:15:38.292950000'),
                ('2025-07-03', '2025-07-07 10:53:30.915895000')
        ) AS run_list
        ORDER BY run_date;
    run_date DATE;
    loaded_at TIMESTAMP_NTZ;
    previous_date DATE;
    calls INTEGER DEFAULT 0;
BEGIN
    FOR run IN runs DO
        run_date := run.run_date;
        loaded_at := run.loaded_at;
        previous_date := run.previous_date;
        CALL test_2(:run_date, :loaded_at, :previous_date);
        calls := calls + 1;
    END FOR;
    -- No COMMIT needed
    RETURN 'test_2 called ' || calls || ' times';
END;
$$;


-- 3. Run it
CALL run_test_2();

SELECT * FROM test_2_calls ORDER BY call_no;
