-- Checks a clone made by the app: DEVELOPMENT owns it and can work in it,
-- READ_ONLY_ROLE can read it and nothing else. Run all; the last result is
-- one table with every check. The clone here is DEV_TIMOFEI: if yours has
-- another name, replace it everywhere
--
-- Each role is checked on its own, with secondary roles off. Snowsight turns
-- on all your roles as secondary ones by default, and then READ_ONLY_ROLE
-- would get DEVELOPMENT's privileges too. Afterwards, USE SECONDARY ROLES ALL
-- brings them back
--
-- To look at the grants themselves, run these on their own:
--   SHOW GRANTS ON TABLE DEV_TIMOFEI.SALES.ORDERS;
--   SHOW GRANTS TO ROLE READ_ONLY_ROLE;

-- 1. DEVELOPMENT adds a schema and a table after the clone was made,
--    to see that the future grants cover them
USE ROLE DEVELOPMENT;
USE SECONDARY ROLES NONE;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE SCHEMA DEV_TIMOFEI.SCRATCH;
CREATE OR REPLACE TABLE DEV_TIMOFEI.SCRATCH.NOTES (id INTEGER, note VARCHAR);
INSERT INTO DEV_TIMOFEI.SCRATCH.NOTES VALUES (1, 'made after the clone');

-- 2. READ_ONLY_ROLE tries to read and to change things
USE ROLE READ_ONLY_ROLE;
USE SECONDARY ROLES NONE;
USE WAREHOUSE COMPUTE_WH;

EXECUTE IMMEDIATE $$
DECLARE
    n INTEGER;
    read_table VARCHAR;
    read_view VARCHAR;
    read_new_table VARCHAR;
    read_source VARCHAR;
    insert_row VARCHAR DEFAULT 'allowed';
    update_rows VARCHAR DEFAULT 'allowed';
    delete_rows VARCHAR DEFAULT 'allowed';
    create_table VARCHAR DEFAULT 'allowed';
    drop_table VARCHAR DEFAULT 'allowed';
    grant_select VARCHAR DEFAULT 'allowed';
    checks RESULTSET;
BEGIN
    BEGIN
        SELECT COUNT(*) INTO :n FROM DEV_TIMOFEI.SALES.ORDERS;
        read_table := 'allowed, rows: ' || n;
    EXCEPTION
        WHEN OTHER THEN read_table := 'refused: ' || SQLERRM;
    END;

    BEGIN
        SELECT COUNT(*) INTO :n FROM DEV_TIMOFEI.SALES.DAILY_REVENUE;
        read_view := 'allowed, rows: ' || n;
    EXCEPTION
        WHEN OTHER THEN read_view := 'refused: ' || SQLERRM;
    END;

    BEGIN
        SELECT COUNT(*) INTO :n FROM DEV_TIMOFEI.SCRATCH.NOTES;
        read_new_table := 'allowed, rows: ' || n;
    EXCEPTION
        WHEN OTHER THEN read_new_table := 'refused: ' || SQLERRM;
    END;

    BEGIN
        INSERT INTO DEV_TIMOFEI.SALES.ORDERS VALUES (5, '2025-11-04', 'Dave', 10);
    EXCEPTION
        WHEN OTHER THEN insert_row := 'refused: ' || SQLERRM;
    END;

    BEGIN
        UPDATE DEV_TIMOFEI.SALES.ORDERS SET amount = 0;
    EXCEPTION
        WHEN OTHER THEN update_rows := 'refused: ' || SQLERRM;
    END;

    BEGIN
        DELETE FROM DEV_TIMOFEI.SALES.ORDERS;
    EXCEPTION
        WHEN OTHER THEN delete_rows := 'refused: ' || SQLERRM;
    END;

    BEGIN
        CREATE TABLE DEV_TIMOFEI.SALES.RO_TEST (id INTEGER);
    EXCEPTION
        WHEN OTHER THEN create_table := 'refused: ' || SQLERRM;
    END;

    BEGIN
        GRANT SELECT ON TABLE DEV_TIMOFEI.SALES.ORDERS TO ROLE PUBLIC;
    EXCEPTION
        WHEN OTHER THEN grant_select := 'refused: ' || SQLERRM;
    END;


    BEGIN
        DROP TABLE DEV_TIMOFEI.SALES.ORDERS;
    EXCEPTION
        WHEN OTHER THEN drop_table := 'refused: ' || SQLERRM;
    END;

    BEGIN
        SELECT COUNT(*) INTO :n FROM DEV.SALES.ORDERS;
        read_source := 'allowed, rows: ' || n;
    EXCEPTION
        WHEN OTHER THEN read_source := 'refused: ' || SQLERRM;
    END;

    checks := (
        SELECT step, action, expected, actual,
               IFF(STARTSWITH(actual, expected), '✅', '❌') AS ok
        FROM (
            SELECT 1 AS step, 'read ORDERS' AS action, 'allowed' AS expected, :read_table AS actual
            UNION ALL SELECT 2, 'read the DAILY_REVENUE view', 'allowed', :read_view
            UNION ALL SELECT 3, 'read a table made after the clone', 'allowed', :read_new_table
            UNION ALL SELECT 4, 'insert into ORDERS', 'refused', :insert_row
            UNION ALL SELECT 5, 'update ORDERS', 'refused', :update_rows
            UNION ALL SELECT 6, 'delete from ORDERS', 'refused', :delete_rows
            UNION ALL SELECT 7, 'create a table', 'refused', :create_table
            UNION ALL SELECT 8, 'grant SELECT to another role', 'refused', :grant_select
            UNION ALL SELECT 9, 'drop ORDERS', 'refused', :drop_table
            UNION ALL SELECT 10, 'read DEV, the source', 'refused', :read_source
        )
        ORDER BY step
    );
    RETURN TABLE(checks);
END;
$$;
