# Pagila - analytics in PySpark

The seven questions from the SQL task, answered with the PySpark DataFrame API only - no
SQL. Tables are read from PostgreSQL over JDBC. Everything is in pagila_spark.py


## Requirements

| | |
| --- | --- |
| Java | 8  |
| Python | 3.9 |
| PySpark | 3.1.3 |

## Database

Use Pagila v3.1.0, not master: current Pagila requires PostgreSQL 18+ and won't load
on 16

The dumps aren't committed. Put them in pagila/ before starting the container -
otherwise Docker mounts an empty directory in their place and initialisation fails with
an error that doesn't mention the missing files

```yml
mkdir -p pagila
curl -L -o pagila/pagila-schema.sql https://raw.githubusercontent.com/devrimgunduz/pagila/pagila-v3.1.0/pagila-schema.sql
curl -L -o pagila/pagila-data.sql   https://raw.githubusercontent.com/devrimgunduz/pagila/pagila-v3.1.0/pagila-data.sql

docker compose up -d
docker compose logs -f
```

The container loads both files on first start and stops on the first error. It listens
on **5433**, so it won't collide with another PostgreSQL on the default port

## Run

```bash
python pagila_spark.py
```
