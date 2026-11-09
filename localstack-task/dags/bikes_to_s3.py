"""Upload each month's trips to S3, with Spark's departures and returns per station.

    data/incoming/trips_2019-06.csv
      1. Spark:  -> data/metrics/station_counts_2019-06.csv
      2. upload: -> s3://helsinki-city-bikes/raw/year=2019/month=06/trips_2019-06.csv
      3. upload: -> s3://helsinki-city-bikes/metrics/year=2019/month=06/station_counts_2019-06.csv
      4. move both files to data/processed/

Runs every 10 minutes, with a mapped task group per file. Every step can run
again: uploads replace the same keys and Spark overwrites its file. A group
that fails leaves its file in data/incoming/ for the next run.
"""

from __future__ import annotations

import shlex
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, get_current_context, task, task_group

from bikes.files import archive, file_name, month_of, waiting_files
from bikes.resources import BUCKET, FileKind, S3File

DATA = Path("/opt/airflow/data")  # ./data, mounted by docker compose
INCOMING = DATA / "incoming"
METRICS = DATA / "metrics"
PROCESSED = DATA / "processed"
SPARK_JOB = Path("/opt/airflow/jobs/station_counts.py")  # ./jobs, mounted
AWS_CONN_ID = "aws_default"  # LocalStack, see docker-compose.override.yaml


def month_label(path: str) -> str:
    """The file's month, which also becomes the task's label in the UI."""
    month = month_of(path)
    if month is None:
        raise ValueError(f"{Path(path).name} is not a monthly trips file")
    get_current_context()["month"] = month
    return month


def counts_path(month: str) -> Path:
    return METRICS / file_name(month, FileKind.METRICS)


def upload(local: Path, target: S3File) -> str:
    """Put local at the target's key, replacing what is there; returns the key."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    S3Hook(aws_conn_id=AWS_CONN_ID).load_file(
        filename=local, key=target.key, bucket_name=BUCKET, replace=True
    )
    return target.key


with DAG(
    dag_id="bikes_to_s3",
    description="Monthly trip files and Spark's station counts, from data/incoming to S3",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,  # runs after a fresh `docker compose up`
    max_active_runs=1,  # two runs at once would race over the same files
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["bikes", "s3", "spark"],
    doc_md=__doc__,
):

    @task(retries=0)
    def check_bucket() -> None:
        """Stop the run early if the bucket is missing, as after a LocalStack restart."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        if not S3Hook(aws_conn_id=AWS_CONN_ID).check_for_bucket(BUCKET):
            raise RuntimeError(
                f"Bucket {BUCKET} not found. LocalStack forgets everything when it "
                "restarts: run `python -m bikes.infra up` on your machine."
            )

    @task
    def find_files() -> list[str]:
        """The trips files waiting in data/incoming, oldest month first."""
        return [str(path) for path in waiting_files(INCOMING)]

    # Two at a time: each runs its own JVM, about a gigabyte.
    @task.bash(map_index_template="{{ month }}", max_active_tis_per_dagrun=2)
    def count_stations(path: str) -> str:
        """Departures and returns per station, with spark-submit on this worker."""
        output = counts_path(month_label(path))
        args = [str(SPARK_JOB), path, str(output)]
        return "spark-submit --master 'local[2]' --driver-memory 1g " + shlex.join(args)

    @task(map_index_template="{{ month }}", max_active_tis_per_dagrun=4)
    def upload_trips(path: str) -> str:
        return upload(Path(path), S3File(FileKind.RAW, month_label(path)))

    @task(map_index_template="{{ month }}", max_active_tis_per_dagrun=4)
    def upload_counts(path: str) -> str:
        month = month_label(path)
        return upload(counts_path(month), S3File(FileKind.METRICS, month))

    @task(map_index_template="{{ month }}")
    def archive_files(path: str) -> None:
        """Move both files to data/processed, so the next run leaves them alone."""
        month = month_label(path)
        archive(Path(path), PROCESSED)
        archive(counts_path(month), PROCESSED)

    @task_group
    def month_files(path: str) -> None:
        """Everything one monthly file goes through."""
        count_stations(path) >> upload_trips(path) >> upload_counts(path) >> archive_files(path)

    check_bucket() >> month_files.expand(path=find_files())
