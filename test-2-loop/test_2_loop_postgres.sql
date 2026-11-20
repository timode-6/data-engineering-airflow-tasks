-- The 20 separate CALL test_2(...) statements as one loop in a procedure
-- PostgreSQL 11 or newer

-- 1. Only for trying it out: a stand-in test_2 that writes down what it gets
CREATE SCHEMA IF NOT EXISTS test_2_demo;
SET search_path TO test_2_demo;

DROP TABLE IF EXISTS test_2_calls;
CREATE TABLE test_2_calls (
    call_no       bigint GENERATED ALWAYS AS IDENTITY,
    run_date      date,
    loaded_at     timestamp,
    previous_date date
);

CREATE OR REPLACE PROCEDURE test_2(run_date date, loaded_at timestamp, previous_date date)
LANGUAGE sql
AS $$
    INSERT INTO test_2_calls (run_date, loaded_at, previous_date)
    VALUES (run_date, loaded_at, previous_date);
$$;


-- 2. The loop
CREATE OR REPLACE PROCEDURE run_test_2()
LANGUAGE plpgsql
AS $$
DECLARE
    run record;
BEGIN
    FOR run IN
        SELECT
            run_date,
            loaded_at,
            lag(run_date, 1, date '2025-01-28') OVER (ORDER BY run_date) AS previous_date
        FROM (
            VALUES
                (date '2025-02-06', timestamp '2025-02-12 09:38:25.999982000'),
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
        ) AS runs (run_date, loaded_at)
        ORDER BY run_date
    LOOP
        CALL test_2(run.run_date, run.loaded_at, run.previous_date);
        -- Commit after every run, as the separate CALLs did
        COMMIT;
    END LOOP;
END;
$$;


-- 3. Run it
CALL run_test_2();

SELECT * FROM test_2_calls ORDER BY call_no;
