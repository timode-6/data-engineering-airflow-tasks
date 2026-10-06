# Pagila - analytical queries

Seven analytical queries against the [Pagila](https://github.com/devrimgunduz/pagila)
sample database, a PostgreSQL port of MySQL's Sakila: a DVD rental store with films,
actors, categories, inventory, rentals, payments and customers.

## Requirements

PostgreSQL 13 or newer.
Tested on PostgreSQL 16 with Pagila v3.1.0.


## Loading the data

```bash
curl -OL https://github.com/devrimgunduz/pagila/archive/refs/tags/pagila-v3.1.0.tar.gz
tar xzf pagila-v3.1.0.tar.gz
cd pagila-pagila-v3.1.0

createdb pagila
psql -d pagila -v ON_ERROR_STOP=1 -f pagila-schema.sql
psql -d pagila -v ON_ERROR_STOP=1 -f pagila-data.sql
```


The dump assigns ownership to a <u>postgres</u> role. If your server doesn't have one , create it first:

```sql
CREATE ROLE postgres SUPERUSER LOGIN PASSWORD 'postgres';
```


## Running the queries

```bash
psql -d pagila -f queries.sql
```


## Notes on the output

Query 6 returns 597 cities, of which only 15 have an inactive customer, each with exactly
one. With 599 customers spread over 597 cities, almost every city holds a single customer,
so the split carries little signal