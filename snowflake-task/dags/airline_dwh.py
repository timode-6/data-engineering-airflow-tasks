"""Airline DWH loads into Snowflake, orchestrated by Airflow.

One DAG runs every load:
    reset_dwh -> upload_to_stage -> load_raw -> load_core -> load_marts
"""

from __future__ import annotations

from pathlib import Path

import pendulum

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import DAG, Param, TriggerRule, chain, task
from airflow.sdk.exceptions import AirflowSkipException

SNOWFLAKE_CONN_ID = "snowflake_default"
STAGE = "@AIRLINE_DWH.RAW.AIRLINE_STAGE"
INCOMING_DIR = Path("/opt/airflow/data/incoming")
CORE_TABLES = ["FACT_FLIGHT", "DIM_PASSENGER", "DIM_AIRPORT", "DIM_DATE", "DIM_COUNTRY", "DIM_CONTINENT"]
MART_TABLES = ["FLIGHTS_DAILY", "AIRPORT_PERFORMANCE", "PASSENGER_PROFILE"]

# What a full reload starts from: an empty stage, RAW, CORE and MART.
RESET_STATEMENTS = [
    f"REMOVE {STAGE}",
    "TRUNCATE TABLE AIRLINE_DWH.RAW.AIRLINE_BOOKINGS",
    *[f"TRUNCATE TABLE AIRLINE_DWH.CORE.{table}" for table in CORE_TABLES],
    *[f"TRUNCATE TABLE AIRLINE_DWH.MART.{table}" for table in MART_TABLES],
    "CREATE OR REPLACE STREAM AIRLINE_DWH.RAW.AIRLINE_BOOKINGS_STREAM "
    "ON TABLE AIRLINE_DWH.RAW.AIRLINE_BOOKINGS APPEND_ONLY = TRUE",
    "CREATE OR REPLACE STREAM AIRLINE_DWH.CORE.FLIGHT_DETAILS_STREAM "
    "ON VIEW AIRLINE_DWH.CORE.V_FLIGHT_DETAILS",
]


@task
def reset_dwh(params=None) -> None:
    """Empty the DWH before a full reload"""
    if not params["full_reload"]:
        raise AirflowSkipException("Incremental load: nothing to reset")
    SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).run(RESET_STATEMENTS, autocommit=True)


@task(trigger_rule=TriggerRule.NONE_FAILED)
def upload_to_stage(params=None) -> list[dict]:
    """PUT the files matching file_name from data/incoming into the internal stage"""
    pattern = params["file_name"]
    if Path(pattern).name != pattern:
        raise ValueError(f"file_name must be a name or pattern inside data/incoming/, got {pattern!r}")
    paths = sorted(INCOMING_DIR.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"nothing in {INCOMING_DIR} matches {pattern!r}")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    uploaded = []
    for path in paths:
        rows = hook.run(
            f"PUT 'file://{path}' {STAGE} AUTO_COMPRESS=TRUE OVERWRITE=FALSE",
            handler=fetch_all_handler,
            return_dictionaries=True,
        )
        for row in rows:
            print(f"{row['source']} -> {row['target']}: {row['status']}")
            uploaded.append({"file": row["target"], "status": row["status"]})
    return uploaded


def call_procedure(task_id: str, procedure: str) -> SQLExecuteQueryOperator:
    """Call a Snowflake procedure, passing the Airflow run id for the audit log"""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"CALL {procedure}(%(run_id)s)",
        parameters={"run_id": "{{ run_id }}"},
        autocommit=True,
        show_return_value_in_logs=True,
    )


with DAG(
    dag_id="airline_dwh_load",
    description="Load files from data/incoming through RAW, CORE and MART",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Every load uses the same stage, streams and tables, so two loads must never interleave
    max_active_runs=1,
    # Runs only when triggered (schedule=None), so it can start unpaused
    is_paused_upon_creation=False,
    params={
        "file_name": Param(
            "Airline_Dataset.csv",
            type="string",
            description="File name or glob pattern in data/incoming/",
        ),
        "full_reload": Param(
            False,
            type="boolean",
            description="Empty the DWH first, as for an initial load",
        ),
    },
    tags=["snowflake", "airline"],
):
    chain(
        reset_dwh(),
        upload_to_stage(),
        call_procedure("load_raw", "AIRLINE_DWH.RAW.LOAD_RAW"),
        call_procedure("load_core", "AIRLINE_DWH.CORE.LOAD_CORE"),
        call_procedure("load_marts", "AIRLINE_DWH.MART.LOAD_MARTS"),
    )
