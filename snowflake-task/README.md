# Airline DWH: Snowflake, loaded by Airflow

A small data warehouse for the airline bookings dataset, built in three layers.
Stored procedures and Snowflake streams move the data from one layer to the
next, and one Airflow DAG decides what runs and in which order. Every load is
recorded in an audit table. On top of that there are time travel queries and a
secure view with a row access policy

```
data/incoming/*.csv
  | PUT                                          (Airflow: upload_to_stage)
  v
@RAW.AIRLINE_STAGE
  | COPY INTO                                    (RAW.LOAD_RAW)
  v
RAW.AIRLINE_BOOKINGS ---- append-only stream
  | CORE.V_BOOKINGS_CHANGES: trimmed, typed, newest row per passenger
  | MERGE                                        (CORE.LOAD_CORE)
  v
CORE: DIM_CONTINENT, DIM_COUNTRY, DIM_DATE, DIM_PASSENGER, DIM_AIRPORT, FACT_FLIGHT
  | stream on CORE.V_FLIGHT_DETAILS, the fact joined with its dimensions
  | MERGE                                        (MART.LOAD_MARTS)
  v
MART: FLIGHTS_DAILY, AIRPORT_PERFORMANCE, PASSENGER_PROFILE

AUDIT.LOAD_LOG          rows inserted and updated, per table, per run
MART.V_FLIGHTS_SECURE   secure view on the fact, filtered by SECURITY.CONTINENT_POLICY
```

## The task, item by item

| Requirement | Where |
|---|---|
| Stage 1, raw data | `RAW.AIRLINE_BOOKINGS`, one VARCHAR per source column ([`sql/03_raw.sql`](sql/03_raw.sql)) |
| Stage 2, cleaned data in tables with constraints | the `CORE` star schema: five dimensions and a fact ([`sql/04_core.sql`](sql/04_core.sql)) |
| Stage 3, a data mart for dashboards | three `MART` tables ([`sql/05_mart.sql`](sql/05_mart.sql)) |
| A procedure, run by Airflow, loading Stage 1 from a stage | `RAW.LOAD_RAW`: internal stage, file format, `COPY INTO` |
| Pipeline 1 → 2 → 3 on procedures and streams, orchestrated by Airflow | `LOAD_RAW` → `LOAD_CORE` → `LOAD_MARTS`, two streams, DAG `airline_dwh_load` |
| Initial load | a run with `full_reload: true` |
| Incremental load based on CDC | the streams carry only rows that changed; the MERGEs insert new rows and update changed ones |
| Logging inserted and updated rows to an audit table | `AUDIT.LOAD_LOG` ([`sql/02_audit.sql`](sql/02_audit.sql)) |
| 2 DDL and 2 DML queries with time travel | [`sql/07_time_travel.sql`](sql/07_time_travel.sql) |
| Secure view on a fact table with a row-level security policy | `MART.V_FLIGHTS_SECURE`, `SECURITY.CONTINENT_POLICY` ([`sql/06_security.sql`](sql/06_security.sql)) |
| Snowpipe (optional) | not done |
| BI report (optional) | not done |

CORE and MART hold nine tables. RAW, the audit log and the security mapping
table come on top of those

## Layout

| Path | What it is |
|---|---|
| `sql/01_setup.sql` | Warehouse, role, database, schemas, Airflow's service user (ACCOUNTADMIN) |
| `sql/02_audit.sql` | `AUDIT.LOAD_LOG` and `AUDIT.LOG_MERGE` |
| `sql/03_raw.sql` | File format, stage, raw table, its stream, `RAW.LOAD_RAW` |
| `sql/04_core.sql` | Dimensions, fact, `CORE.V_BOOKINGS_CHANGES`, `CORE.LOAD_CORE` |
| `sql/05_mart.sql` | `CORE.V_FLIGHT_DETAILS` and its stream, the marts, `MART.LOAD_MARTS` |
| `sql/06_security.sql` | Analyst roles, mapping table, row access policy, secure view, tests |
| `sql/07_time_travel.sql` | The time travel queries |
| `sql/08_checks.sql` | Audit summary, state check, fingerprint comparison |
| `dags/airline_dwh.py` | The DAG `airline_dwh_load` |
| `scripts/make_cdc_files.py` | Splits the dataset into an initial file and a change file |
| `docker-compose.override.yaml` | Snowflake connection, data folder, key mount |
| `.env.example` | Every variable the stack needs |

`docker-compose.yaml` is not committed. It is the official file, downloaded
unmodified during setup. Everything this project adds is in the override

## Versions

| | |
|---|---|
| Airflow | 3.3.2, official image (Python 3.13) |
| Snowflake provider | 6.14.0, already in the official image |
| Snowflake | Enterprise trial on AWS us-east-2 (Ohio) |

Enterprise edition matters: row access policies need it. The trial account is
in us-east-2 rather than Frankfurt; nothing in the project depends on the region

## Data

`Airline_Dataset.csv`: 98,619 bookings in 18 columns. Not committed; put it in
`data/incoming/`.

- Every passenger books once (`Passenger ID` is unique), so the fact's grain is
  one flight per passenger.
- Departures run from 2022-01-01 to 2022-12-30: 364 days, with no 31 December.
- There are 235 countries, 6 continents and 9,128 airports. The source has no
  airport id, so an airport is its name, country and code together. 85 airports
  have codes that are not IATA codes (`0`, `-`, `SB2`, `O62`, `TR7`, `BR-`);
  `IATA_CODE` is NULL for them, and the code itself is kept in `AIRPORT_CODE`.
- Flight statuses split almost evenly: Cancelled 32,942, On Time 32,846,
  Delayed 32,831.
- `Ticket Type` is `Business` and `Passenger Status` is `On Time` on every row.
  Both are kept, though they carry no information.
- Seven nationality fields have stray spaces, which the CORE view trims.
- The country list already uses newer names such as Czechia and Eswatini but
  still says Turkey. The CDC test below renames it to Türkiye.

## How the loads work

### The DAG

```
airline_dwh_load   schedule=None, max_active_runs=1
params: file_name (default Airline_Dataset.csv), full_reload (default false)

  reset_dwh         full reloads only: empty the stage, RAW, CORE and MART,
                    then recreate both streams so they start empty
  upload_to_stage   PUT data/incoming/<file_name> into @RAW.AIRLINE_STAGE;
                    file_name may be a pattern, e.g. airline_0*.csv
  load_raw          CALL RAW.LOAD_RAW(run_id)
  load_core         CALL CORE.LOAD_CORE(run_id)
  load_marts        CALL MART.LOAD_MARTS(run_id)
```

- **One DAG for both kinds of load.** An initial load is a run with
  `full_reload: true`. In an incremental run, `reset_dwh` is skipped.
  `upload_to_stage` has the trigger rule `none_failed`, so it runs after a
  skipped reset but not after a failed one.
- **Loads never overlap.** Every load uses the same stage, streams and tables,
  so two loads must never interleave. With `max_active_runs=1`, a second trigger
  waits in the queue until the running load ends. The project first had two
  DAGs; see [What the tests caught](#what-the-tests-caught).
- **The audit key.** Airflow passes its `run_id` to every procedure, and that
  id ties together the audit log rows of one run.

### RAW

`upload_to_stage` PUTs the file into an internal stage, compressed. `LOAD_RAW`
runs `COPY INTO` with the file format and adds `METADATA$FILENAME` and
`METADATA$FILE_ROW_NUMBER` as `SOURCE_FILE` and `SOURCE_ROW`. `LOADED_AT`
defaults to the load time

Nothing is converted on the way in. An empty field stays an empty string, and a
row with the wrong number of columns stops the load (`ON_ERROR = ABORT_STATEMENT`)

`COPY` skips files it has already loaded, because Snowflake keeps 64 days of
load history. So delivering the same file twice adds nothing. A full reload
truncates RAW, which clears that history as well

An append-only stream on RAW holds the rows that `LOAD_CORE` has not processed
yet

### CORE

`CORE.V_BOOKINGS_CHANGES` reads that stream:
- it trims text, uppercases the codes, and parses age and date with
  `TRY_TO_NUMBER` and `TRY_TO_DATE`
- it keeps the newest row per passenger. Newest means loaded later, then from
  the file whose name sorts later, then the later line

`LOAD_CORE` MERGEs six tables in that order: continents, countries, dates,
passengers, airports, and last the fact, which needs the airport ids

- **Only real changes count.** A MERGE updates a row only when a value actually
  differs (`IS DISTINCT FROM`), so a row sent again unchanged counts as nothing
- **Dimensions take the latest row per key**, with the same ordering as
  passengers. That is how Turkey becomes Türkiye even when one batch holds both
  names
- **All-or-nothing.** The six MERGEs run in one transaction, so the stream moves
  on only if all of them succeed. If one fails, the procedure rolls back, logs
  `FAILED` with the error and re-raises, which fails the Airflow task. The rows
  stay in the stream for the next run

Snowflake enforces only `NOT NULL`. The primary, unique and foreign keys are
declared for readers and tools, and the MERGE logic keeps them true

### MART

| Table | One row per | Columns |
|---|---|---|
| `FLIGHTS_DAILY` | date and continent | flights, on time, delayed, cancelled, on-time % |
| `AIRPORT_PERFORMANCE` | airport | name, IATA code, country, continent, flights, delayed, cancelled, % |
| `PASSENGER_PROFILE` | nationality, gender and age group | passengers, on time, delayed, cancelled, cancelled % |

**The stream sits on a view.** A mart has to change when a dimension changes,
even if no flight does. So the stream for this layer is on
`CORE.V_FLIGHT_DETAILS`, the fact joined with all five dimensions, which holds
every column a mart groups by or shows. A change in any of those tables reaches
the stream with the old and the new values. A renamed country, for example,
puts all of that country's flights into the stream

**Only affected groups are recomputed.** `LOAD_MARTS` takes the groups that
appear in the stream, using both old and new values, recomputes them from the
current view and MERGEs the result:
- a flight that moves to another group is also subtracted from its old one
- names are read from the dimensions, so renames reach the marts too
- a group that empties out keeps a row of zeros rather than being deleted

### Audit log

`AUDIT.LOAD_LOG` gets one row per table per procedure call: the run id, the
procedure, the table, rows inserted, rows updated, the status, any error, and
the start and end times
- `LOAD_RAW` logs the rows it added
- For each MERGE, `AUDIT.LOG_MERGE` computes inserted rows as the count after
  minus the count before. The rest of the rows the MERGE touched were updates

## Setup

### Snowflake

1. **Create a key pair for Airflow's service user.** The private key stays on
   your machine: the compose override mounts it read-only into the Airflow
   containers, and it is never in the repo

   ```bash
   mkdir -p ~/.snowflake
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/airflow_svc_key.p8 -nocrypt
   openssl rsa -in ~/.snowflake/airflow_svc_key.p8 -pubout -out ~/.snowflake/airflow_svc_key.pub
   chmod 600 ~/.snowflake/airflow_svc_key.p8
   grep -v -- '-----' ~/.snowflake/airflow_svc_key.pub | tr -d '\n'   # the public key body
   ```

2. **Run the setup script.** Paste the public key body into
   [`sql/01_setup.sql`](sql/01_setup.sql) and run that file in Snowsight as
   ACCOUNTADMIN. In a Snowsight file, Ctrl+A and then Ctrl+Enter run everything

3. **Run the rest, in order:** `02`, `03`, `04` and `05`. Then run parts 1 and 2
   of `06_security.sql`; part 3 is its test. Each file switches to the role it
   needs

4. **Check the key.** In Snowsight, `DESC USER AIRFLOW_SVC` shows
   `RSA_PUBLIC_KEY_FP`. It must match `SHA256:` followed by the output of this:

   ```bash
   openssl rsa -pubin -in ~/.snowflake/airflow_svc_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
   ```

`AIRFLOW_SVC` is a `TYPE = SERVICE` user, so it has no password and no MFA and
logs in with the key only

### Airflow

The commands work in both bash and fish

```bash
# The official compose file, unmodified
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config ./data/incoming

# Configuration: fill in the four values below
cp .env.example .env
id -u
docker run --rm apache/airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
$EDITOR .env

docker compose up -d
```

The four values in `.env`:
- `AIRFLOW_UID`: the number `id -u` printed
- `FERNET_KEY`: the key the `docker run` line printed
- `SNOWFLAKE_ACCOUNT`: your account identifier, `ORGNAME-ACCOUNTNAME`
- `SNOWFLAKE_PRIVATE_KEY_PATH`: the absolute path to `airflow_svc_key.p8`

The image is the official one: it already includes the Snowflake provider

The override defines the connection `snowflake_default` as an environment
variable. It contains no secret, only the path where the key is mounted.
Airflow does not list connections defined this way under Admin → Connections

## Running

```bash
# The two CDC test files, from data/incoming/Airline_Dataset.csv
python3 scripts/make_cdc_files.py

# The DAG starts unpaused
docker compose exec airflow-scheduler airflow dags list
```

Every load is the same command with a different `--conf`:

```bash
docker compose exec airflow-scheduler airflow dags trigger airline_dwh_load --conf '<conf>'
```

| Load | `--conf` |
|---|---|
| Full load of the whole dataset | `{"full_reload": true}` |
| Initial load of the CDC test | `{"file_name": "airline_001_initial.csv", "full_reload": true}` |
| Incremental load | `{"file_name": "airline_002_changes.csv"}` |
| Rebuild from every file in one batch | `{"file_name": "airline_0*.csv", "full_reload": true}` |

The trigger form in the UI shows the same two parameters. Queued runs start one
after another, in the order they were triggered

To see a run's audit rows:

```sql
SELECT * FROM AIRLINE_DWH.AUDIT.LOAD_LOG ORDER BY LOG_ID;
```

## The CDC test

`scripts/make_cdc_files.py` simulates the source system as of 1 December 2022

- **`airline_001_initial.csv`:** the 90,695 bookings that depart before
  1 December.
- **`airline_002_changes.csv`:** what the source sends next, 10,519 lines:
  - **7,924 new bookings:** every December departure.
  - **2,474 status updates:** every 25th earlier booking that is not cancelled
    moves one step, from On Time to Delayed or from Delayed to Cancelled.
  - **121 second updates:** some of those change again, from Delayed to
    Cancelled, further down the same file, so the later line has to win.
  - **A rename:** Turkey became Türkiye in June 2022, so the 80 lines for
    Turkish airports carry the new name.

The file names carry a sequence number, so they sort in delivery order. That
matters when one `COPY` loads several files at once: their rows share
`LOADED_AT`, so the file name decides which line is newer

## Results

All runs were on 26 October 2026, triggered back to back, times are UTC

| Run | Load | Triggered | Started | Ended |
|---|---|---|---|---|
| 1 | Full reload, `airline_001_initial.csv` | 08:21:19 | 08:21:20 | 08:22:27 |
| 2 | Incremental, `airline_002_changes.csv` | 08:21:22 | 08:22:28 | 08:23:19 |
| 3 | The same file again | 08:21:25 | 08:23:20 | 08:24:13 |
| 4 | Full reload, `airline_0*.csv`, both files in one batch | 08:27:28 | 08:27:28 | 08:28:33 |

Runs 2 and 3 were queued within six seconds of run 1. Each started only when the
previous one had ended

### Audit log

Rows inserted / updated, from `AUDIT.LOAD_LOG` (the first query in
[`sql/08_checks.sql`](sql/08_checks.sql)). Every row has status `SUCCESS`

| Table | Run 1 | Run 2 | Run 3 | Run 4 |
|---|---|---|---|---|
| RAW.AIRLINE_BOOKINGS | 90695 / 0 | 10519 / 0 | 0 / 0 | 101214 / 0 |
| CORE.DIM_CONTINENT | 6 / 0 | 0 / 0 | 0 / 0 | 6 / 0 |
| CORE.DIM_COUNTRY | 235 / 0 | 0 / 1 | 0 / 0 | 235 / 0 |
| CORE.DIM_DATE | 334 / 0 | 30 / 0 | 0 / 0 | 364 / 0 |
| CORE.DIM_PASSENGER | 90695 / 0 | 7924 / 0 | 0 / 0 | 98619 / 0 |
| CORE.DIM_AIRPORT | 9128 / 0 | 0 / 0 | 0 / 0 | 9128 / 0 |
| CORE.FACT_FLIGHT | 90695 / 0 | 7924 / 2474 | 0 / 0 | 98619 / 0 |
| MART.FLIGHTS_DAILY | 2004 / 0 | 180 / 1329 | 0 / 0 | 2184 / 0 |
| MART.AIRPORT_PERFORMANCE | 9128 / 0 | 0 / 6207 | 0 / 0 | 9128 / 0 |
| MART.PASSENGER_PROFILE | 2005 / 0 | 31 / 1231 | 0 / 0 | 2036 / 0 |

What run 2 shows:
- **RAW 10,519:** RAW receives every line, so it keeps the full history. CORE
  and MART keep the current state.
- **DIM_COUNTRY, 1 update:** the rename.
- **DIM_DATE, 30 new rows:** 1 to 30 December.
- **DIM_PASSENGER, 0 updates:** the 2,474 updated bookings carry unchanged
  passenger data.
- **DIM_AIRPORT, 0 / 0:** every airport already had a booking before December.
- **FACT_FLIGHT, 2,474 updates, not 2,595:** the 121 bookings that arrived twice
  are updated once, with the later line.
- **FLIGHTS_DAILY:** 180 new rows (30 days × 6 continents). The 1,329 updates
  are earlier days whose status counts changed.
- **AIRPORT_PERFORMANCE, 6,207 updates:** these include all 65 Turkish
  airports. 21 of them have no new or changed flight and are updated for the
  rename alone.
- **Run 3, all zeros:** `COPY` skips a file it has already loaded.

### State after the loads

The second query in `08_checks.sql` gives the same result after run 3 and after
run 4:

| Check | Value |
|---|---|
| Mart rows that differ from a recomputation from scratch | 0 |
| Flights | 98,619 |
| Bookings with two lines in the change file | 121 |
| ... of them Cancelled, as the later line says | 121 |
| Name of `TR` in `DIM_COUNTRY` | Türkiye |
| Airports named Türkiye / Turkey in `AIRPORT_PERFORMANCE` | 65 / 0 |
| Fingerprint of all business data in CORE | -3109019029595386374 |

The third query computes the fingerprint at the end of run 3 through time
travel and the fingerprint after run 4, and compares them: `SAME_DATA` is
`TRUE`. Loading the files one by one and replaying them as one batch produce
the same warehouse. The fingerprint is a `HASH_AGG` over every business column
in CORE, without surrogate keys, lineage or timestamps

### Time travel

The two moments come from the audit log: when the incremental load started, and
when the run after it started (08:22:40 and 08:23:31 UTC). The rebuild truncated
every table afterwards, but time travel still reads both versions

| Query | Result |
|---|---|
| DML 1: status changes between the two moments, a self-join of `FACT_FLIGHT` at both | Delayed → Cancelled 1,258; On Time → Cancelled 121; On Time → Delayed 1,095. That is 2,474 in total, the updates in the audit log |
| DDL 1: `CREATE TABLE ... CLONE FACT_FLIGHT AT(...)` | 90,695 flights, 30,240 cancelled: the fact as the initial load left it |
| DML 2: `DELETE` December from `FLIGHTS_DAILY`, then `INSERT ... SELECT ... BEFORE(STATEMENT => ...)` | 180 rows back, with their original values |
| DDL 2: `DROP TABLE` on the clone, then `UNDROP TABLE` | 90,695 flights again |

### Secure view and row access policy

`SECURITY.CONTINENT_ACCESS` maps roles to continents:
- `AIRLINE_ANALYST_EU` sees Europe;
- `AIRLINE_ANALYST_AMERICAS` sees North and South America;
- `AIRLINE_ETL` has `*`, which means every continent.

The policy lets a row through when the current role is mapped to the row's
continent, or to `*`. It is attached to the secure view, not to `FACT_FLIGHT`,
so the pipeline itself is never filtered. The view leaves out passenger and
pilot names

| Query (part 3 of `06_security.sql`) | Result |
|---|---|
| `AIRLINE_ANALYST_EU` reads the view | EU 12,335 |
| `AIRLINE_ANALYST_AMERICAS` reads the view | NAM 32,033, SAM 10,718 |
| An analyst reads `CORE.FACT_FLIGHT` directly | error: does not exist or not authorized |
| `AIRLINE_ETL` reads the view | AF 11,030, AS 18,637, EU 12,335, NAM 32,033, OC 13,866, SAM 10,718 (98,619) |
| `POLICY_REFERENCES` for the view | `CONTINENT_POLICY`, `ROW_ACCESS_POLICY`, on `CONTINENT_CODE`, `ACTIVE` |

A role that is not in the mapping table sees no rows, and that includes
ACCOUNTADMIN

## What the tests caught

All three of these are fixed in the code above

- **Overlapping loads.** At first, the initial and incremental loads were two
  DAGs. An incremental run started while a full reload was still running:
  - The full reload's MERGE batch picked up both files
  - `LOG_MERGE` logged updates such as `6 / -6`, because it counted rows before
    the other run's inserts were committed
  - Worse, an overlapping reset could delete a file another run had just staged

  One DAG with `max_active_runs=1` makes this impossible
- **`ANY_VALUE` in the dimension MERGEs.** In that combined batch, Turkey and
  Türkiye appeared together, and `ANY_VALUE` kept the old name. The dimensions
  now take the latest row per key. Run 4 repeats that situation on purpose and
  gets Türkiye and the same fingerprint as the file-by-file load
- **A mart that missed renames.** The view behind the mart stream first left out
  the country table, so a renamed country would not have reached
  `AIRPORT_PERFORMANCE`. In this test, that would have left 21 of the 65 Turkish
  airports under the old name. The view now carries every column a mart shows,
  and `FLIGHTS_DAILY` reads continent names from `DIM_CONTINENT`

## Notes and limitations

- **No deletes.** The feed has no delete marker, so a booking that disappears
  from the source stays in CORE
- **Order comes from loading, not from the source.** "Newest" means loaded
  later, then the later file name, then the later line. A real CDC feed carries
  a change timestamp or sequence number, which would replace this ordering.
  Until then, file names must sort in delivery order, as the generator's
  numbered names do
- **The audit counts assume one load at a time.** `LOG_MERGE` derives inserts
  from row counts. That holds while only one load runs, which the DAG
  guarantees. Two procedures called by hand at the same time would still leave
  correct data, but wrong counts
- **Time travel history lasts one day,** the default retention period.
  `07_time_travel.sql` and the last query of `08_checks.sql` use the run ids of
  the test above; for other runs, take the ids from
  `airflow dags list-runs airline_dwh_load`
- **Timestamps show in Los Angeles time,** the account's default time zone.
  They are `TIMESTAMP_LTZ`, so they display in the session's time zone
- **Re-running the scripts is safe for data.** Tables are created only if they
  are missing; views, procedures and the file format are replaced. Re-running
  `05_mart.sql` recreates the mart stream empty, so run it between loads
