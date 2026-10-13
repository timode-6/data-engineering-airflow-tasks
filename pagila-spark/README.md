# Pagila analytics in PySpark

The same seven questions as the Pagila SQL task (PYT-9572), answered with the
PySpark DataFrame API only - no `spark.sql`, no `expr`, no `selectExpr`. Tables are
read from PostgreSQL over JDBC

1. Films per category, descending
2. Ten most-rented actors
3. Highest-earning category
4. Titles of films absent from inventory
5. Three actors most frequent in "Children"
6. Active and inactive customers per city
7. Category with the most rental hours, for cities starting with "a" and for cities
   containing "-"

## Requirements

| | |
|---|---|
| PySpark | 3.1.3 (`requirements.txt`) |
| Java | 8 or 11 — Spark 3.1.3 does not run on a modern JDK |
| PostgreSQL | 16, with Pagila v3.1.0 loaded |
| JDBC driver | `org.postgresql:postgresql:42.7.3`, fetched from Maven Central on first run |

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version          # must report 1.8 or 11
```

## Setup

```bash
cp .env.example .env
$EDITOR .env                  # set PAGILA_PASSWORD
docker compose up -d          # Pagila on PostgreSQL 16, published on 5433
```

Then export the variables into your shell. **This step is required.** Docker
Compose reads `.env` by itself, but a Python process does not - the script reads
`os.environ` and nothing else, so without this it exits with
`PAGILA_PASSWORD is not set`

```bash
set -a; source .env; set +a
python pagila_spark.py
```

fish:

```fish
for l in (grep -v '^#' .env | string match -er .)
    set -gx (string split -m1 = $l)
end
python pagila_spark.py
```

Or set the one variable directly, without `.env`:

```bash
export PAGILA_PASSWORD='...'
python pagila_spark.py
```

### Configuration

| Variable | Default | |
|---|---|---|
| `PAGILA_JDBC_URL` | `jdbc:postgresql://127.0.0.1:5433/pagila` | 5433, not 5432, to avoid colliding with a local PostgreSQL |
| `PAGILA_USER` | `postgres` | |
| `PAGILA_PASSWORD` | none | No fallback on purpose: a default here would be a credential committed to the repo. `docker-compose.yml` uses `${PAGILA_PASSWORD:?}` for the same reason. |

`.env` is gitignored; `.env.example` is the committed template

## Results

Full output of a run is in [`docs/output.txt`](docs/output.txt), so the results can
be read without starting PostgreSQL. All seven were compared row for row against
the SQL task's output on the same database and agree

## Assumptions

These are readings of the task, not facts about the data, and a different reading
would give a different answer

**Query 2 returns exactly ten rows, ties at the boundary dropped.** The task asks
for "the 10 actors", a fixed count, where queries 3, 5 and 7 ask for "the most",
which a tie shares. So this one uses `limit(10)` where those use `rank()`. If an
eleventh actor ties for tenth place they are not returned

**Query 5 uses `rank()`, not `dense_rank()`.** `rank() <= 3` returns the actors
whose rank is third or better - SQL's `FETCH FIRST 3 ROWS WITH TIES`.
`dense_rank() <= 3` answers a different question: every actor holding one of the
top three *distinct* film counts. With counts 5, 5, 4, 4, 3 that is five actors
against `rank()`'s four. "Top three actors" means the former

**Query 6 reports country alongside city.** City names are not unique in Pagila -
`city` holds a London in the United Kingdom (312) and a London in Canada (313).
In this data the ambiguity is latent rather than visible: only the UK one has
customers, so the join drops the other and query 6 emits a single London row. No
city name has more than one `city_id` among cities that have customers. Country is
in the output anyway, because the schema permits the collision and a query that
can return rows you cannot tell apart is worth fixing before the data makes it
visible, not after

**Query 7 matches "starts with a" case-insensitively.** `lower(city).startswith("a")`
matches Abha and abha alike. Pagila capitalises city names, so a literal lowercase
`a` would match nothing and the group would be empty - which makes this the only
reading that answers the question, but it is still a reading

**Query 7 excludes unreturned rentals.** Rows with `return_date IS NULL` have no
measurable duration and are dropped. The alternatives - counting them up to now, or
up to the latest date in the data - both invent an end time the data does not
contain, and both make the answer depend on when the query is run

**Query 7 computes durations from epoch seconds.** That equals wall-clock time as
long as the JVM time zone has no DST transition inside a rental, which holds for
Europe/Minsk. Elsewhere, run with `-Duser.timezone=UTC`

## Implementation notes


**Column projection and caching.** `read_table` selects only the columns the
queries use, which is pushed into the `SELECT` the driver sends, so unused columns
never cross the wire. Each table is cached because most are read by several
queries; without it every query re-queries PostgreSQL

**Query 1 counts the joined column, not `*`.** After a left join a category with no
films still produces one row, and `count("*")` would report it as 1. Counting
`film_id` counts non-null values, so it reports 0

**Query 4 uses a `left_anti` join** - the DataFrame equivalent of `NOT EXISTS`, and
null-safe in the same way. A `NOT IN` translation would carry the null trap the SQL
task was pointing at

**Queries 3 and 5 use a window with no `partitionBy`**, so Spark warns that all
data moves to a single partition. Correct here - both rankings are global, over
aggregates of 16 and 200 rows

**Query 6 uses `count(when(cond, True))`** - the DataFrame form of
`COUNT(*) FILTER (WHERE cond)`. `when()` yields null where the condition fails and
`count()` ignores nulls, so both totals come from one pass rather than two
aggregations joined back together

**Query 7 stacks two separately-filtered groups with `unionByName`.** A city can
belong to both groups - al-Ayn and Augusta-Richmond County both start with "a" and
contain "-". Labelling each row with a single `when()` would silently drop those
cities from one side

**Output is bounded.** `show_all` prints the true row count, then at most 50 rows;
query 6 alone returns 597. The DataFrame is cached before counting so the plan is
computed once rather than once per action

## Regenerating the output

```bash
mkdir -p docs
python pagila_spark.py | tee docs/output.txt
```
