# Helsinki city bikes on LocalStack

The [Helsinki city bikes](https://www.kaggle.com/geometrein/helsinki-city-bikes)
trips of 2016–2020 go through an AWS pipeline emulated by LocalStack and end
up in two Tableau Public dashboards.

```
data/source/archive.zip
  │  python -m bikes.split: one CSV per month
  ▼
data/incoming/trips_2019-06.csv
  │  Airflow DAG bikes_to_s3, every 10 minutes:
  │  Spark counts departures and returns per station, both files go to S3
  ▼
S3     raw/year=2019/month=06/trips_2019-06.csv
       metrics/year=2019/month=06/station_counts_2019-06.csv
  │  ObjectCreated events
  ▼
SQS    bike-files  →  Lambda bike-loader (pandas), once both files of a month are in S3
  ▼
DynamoDB   raw trips, daily, monthly and station metrics
  │  python -m bikes.export
  ▼
CSV  →  Tableau Public
```

## Dashboards

- [By day](https://public.tableau.com/views/Helsinkicitybikes/Byday): trips per day; average distance, duration, speed and temperature per day.
- [By month](https://public.tableau.com/views/Helsinkicitybikes/Bymonth): the same per month.

## Setup

You need Docker with Compose, Python 3.12+ and the Kaggle archive.

```
cp .env.example .env    # on Linux, set AIRFLOW_UID to the output of `id -u`
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml'
docker build -t bikes-airflow:3.3.2 .
docker compose up -d    # Airflow and LocalStack

python -m venv .venv    # then activate it
pip install -r requirements-dev.txt
```

Put the archive downloaded from Kaggle into `data/source/archive.zip`.

## Run

```
python -m bikes.infra up        # S3 bucket, SQS queues, DynamoDB tables
python -m bikes.infra deploy    # the Lambda and its SQS trigger
python -m bikes.split data/source/archive.zip data/incoming
```

The DAG picks up the monthly files within 10 minutes (http://localhost:8080,
`airflow` / `airflow`). Then:

```
python -m bikes.infra status    # files in S3, messages in the queues, items in the tables
python -m bikes.infra loads     # one line per month: trips, items, seconds
python -m bikes.infra logs      # the Lambda's log
python -m bikes.export          # data/export/*.csv for Tableau
```

A full run loads 12,157,292 trips in 34 months: 224,427 raw trip items,
1,009 days, 34 months and 7,786 station-months. The biggest month, June 2019
with 715,131 trips, loads in 53 seconds.

LocalStack's free edition starts empty after every restart. Run `up` and
`deploy` again, then put the trips files back:
`mv data/processed/trips_*.csv data/incoming/`. If DAG runs fail after a
reboot without starting any task, run `docker compose restart airflow-apiserver`.

## Tests

```
pytest                                   # the Spark test needs Java 17 or 21 in JAVA_HOME
ruff check . && ruff format --check .
```

## Layout

```
dags/bikes_to_s3.py       the DAG
jobs/station_counts.py    spark-submit entry point
src/bikes/                monthly split, Spark job, AWS setup (infra), CSV export
src/bikes/loader/         the Lambda
tests/                    pytest, with in-memory S3, SQS and DynamoDB
docs/                     loader.md, dynamodb.md, tableau.md, dataset_profile.txt
```
