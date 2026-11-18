# BoM explosion

For every FIN material, its whole production chain by plant and year, built from
the monthly BoM table (material → component). It's done twice, in a pandas
notebook and in SQL (PostgreSQL), and the two results are compared.

## Notebook

Python 3.11 or newer.

```
python -m venv .venv
source .venv/bin/activate.fish     # bash: source .venv/bin/activate
pip install -r requirements.txt
jupyter lab bom_explosion.ipynb
```

Run all cells. The report goes to `output/bom_explosion.csv`.

## SQL

Postgres runs in Docker. The project folder is mounted into the container, so
psql there reads the CSV files directly.

```
docker compose up -d --wait
docker compose exec -w /work postgres psql -U postgres -v ON_ERROR_STOP=1 \
    -f sql/01_load.sql -f sql/02_bom_explosion.sql -f sql/03_compare.sql
```

The last script compares the view with the notebook's report, so run the
notebook first. `only_in_sql` and `only_in_pandas` should both be 0.

To see the result:

```
docker compose exec postgres psql -U postgres -c "SELECT * FROM bom_explosion WHERE fin_material_id = '10000'"
```

Or connect any SQL client to `localhost:5432`, user and password `postgres`. If
5432 is taken, change the first number under `ports` in `docker-compose.yml`.
`docker compose down -v` stops Postgres and deletes its data; the scripts
create everything again.

| Script | What it does |
| --- | --- |
| `sql/01_load.sql` | loads data/bom.csv into the `bom` table, through an all-text table because the export has thousands separators in quantities |
| `sql/02_bom_explosion.sql` | the `bom_explosion` view: annual pairs, then `WITH RECURSIVE` down from each FIN |
| `sql/03_compare.sql` | the view against output/bom_explosion.csv |

The scripts need Postgres 15 or newer (`HEADER MATCH`, the `CYCLE` clause). The
container runs 18.

The .xlsx version of the data has to be saved as CSV for the SQL part, with
codes kept as text:

```
python -c "import pandas as pd; pd.read_excel('data/bom.xlsx', dtype=str).to_csv('data/bom.csv', index=False)"
```

## Files

| File | What it is |
| --- | --- |
| `bom_explosion.ipynb` | what's in the file, the explosion split into functions, checks of the result |
| `sql/` | the same in SQL, see above |
| `docker-compose.yml` | Postgres for the SQL part |
| `data/bom.csv` | task_2_data_ex as given (CSV export). The notebook reads the .xlsx too: point `DATA` in the first cell at it |
| `output/bom_explosion.csv` | the notebook's report: 10 FIN materials, 10 rows each |

## Decisions

- Annual figures are sums of the 12 months. A material's produced quantity is
  repeated on each of its component rows, so it's counted once per month.
- The FIN → PROD pair isn't a row, as in the task's example: the FIN has its own
  columns, and the PROD material it consists of is the first `prod_material_id`.
- A chain is followed within one plant and year.
- Quantities are the BoM's own figures. Nothing is recalculated along the chain.
- The file only has 2024, so there are no 2025 rows. Every year in the data gets
  its own explosion.
- If the data had a cycle (a material among its own components), the notebook
  would stop with an error and the view would stop at the repeated material.
