-- Compares the view with the notebook's report, output/bom_explosion.csv.
-- Both only_in_sql and only_in_pandas should be 0.

BEGIN;

CREATE TEMP TABLE pandas_report (LIKE bom_explosion) ON COMMIT DROP;

\copy pandas_report FROM 'output/bom_explosion.csv' WITH (FORMAT csv, HEADER MATCH)

SELECT
    (SELECT count(*) FROM bom_explosion) AS sql_rows,
    (SELECT count(*) FROM pandas_report) AS pandas_rows,
    (SELECT count(*) FROM (TABLE bom_explosion EXCEPT ALL TABLE pandas_report) AS d)
        AS only_in_sql,
    (SELECT count(*) FROM (TABLE pandas_report EXCEPT ALL TABLE bom_explosion) AS d)
        AS only_in_pandas;

COMMIT;
