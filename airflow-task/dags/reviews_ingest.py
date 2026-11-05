from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, TaskGroup, task

from config import (
    FINAL_FILE,
    REVIEWS_PROCESSED,
    SOURCE_FILE,
    STAGE_NULLS,
    STAGE_SORTED,
)

TRANSFORM_GROUP_ID = "transform_reviews"
FIRST_TRANSFORM_TASK = f"{TRANSFORM_GROUP_ID}.replace_nulls"
EMPTY_FILE_TASK = "log_empty_file"


with DAG(
    dag_id="reviews_ingest",
    description="Sense the reviews CSV, branch on whether it has data, transform it",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["reviews", "etl"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=str(SOURCE_FILE),
        fs_conn_id="fs_default",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    @task.branch
    def check_file_has_data() -> str:
        path = SOURCE_FILE

        if path.stat().st_size == 0:
            print(f"{path} is zero bytes")
            return EMPTY_FILE_TASK

        with path.open("r", encoding="utf-8", errors="replace") as handle:
            header = handle.readline()
            first_row = handle.readline()

        if not header.strip() or not first_row.strip():
            print(f"{path} has a header but no data rows")
            return EMPTY_FILE_TASK

        print(f"{path} has data: {path.stat().st_size} bytes")
        return FIRST_TRANSFORM_TASK

    # The spec asks specifically for a bash script on the empty branch.
    log_empty_file = BashOperator(
        task_id=EMPTY_FILE_TASK,
        bash_command=(
            'echo "[$(date -Is)] EMPTY INPUT: {{ params.source }} contains no data '
            'rows; skipping transformations." >&2; exit 0'
        ),
        params={"source": str(SOURCE_FILE)},
    )

    with TaskGroup(
        group_id=TRANSFORM_GROUP_ID,
        tooltip="Replace nulls, sort by created_date, strip symbols from content",
    ) as transform_reviews:

        @task
        def replace_nulls() -> dict[str, int]:
            from transformations import replace_nulls as run

            STAGE_NULLS.parent.mkdir(parents=True, exist_ok=True)
            return run(str(SOURCE_FILE), str(STAGE_NULLS))

        @task
        def sort_by_created_date() -> dict[str, int]:
            from transformations import sort_by_created_date as run

            return run(str(STAGE_NULLS), str(STAGE_SORTED))

        @task(outlets=[REVIEWS_PROCESSED])
        def clean_content() -> dict[str, int]:
            from transformations import clean_content as run

            FINAL_FILE.parent.mkdir(parents=True, exist_ok=True)
            return run(str(STAGE_SORTED), str(FINAL_FILE))

        replace_nulls() >> sort_by_created_date() >> clean_content()

    wait_for_file >> check_file_has_data() >> [log_empty_file, transform_reviews]
