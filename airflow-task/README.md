# Reviews pipeline — Airflow to MongoDB

Two DAGs linked by an asset. The first waits for a reviews CSV to appear, checks
it has data, and runs three transformations over it. Writing the final file marks
an asset as updated, which is what schedules the second DAG — it loads the result
into MongoDB. Three aggregations are then run in MongoDB itself.

```
reviews_ingest  (schedule=None, started manually)

  wait_for_file                FileSensor, mode="reschedule"
        |
  check_file_has_data          @task.branch
        |
        +--------------------> log_empty_file          BashOperator
        |
        +--------------------> transform_reviews       TaskGroup
                                 replace_nulls
                                   -> sort_by_created_date
                                     -> clean_content   outlets=[asset]
                                                              |
                                        file:///opt/airflow/data/processed/
                                              reviews_processed.csv
                                                              |
reviews_to_mongo  (schedule=[asset])  <------------------------+

  load_to_mongodb -> verify_load
```

## Layout

| Path | What it is |
|---|---|
| `dags/reviews_ingest.py` | Sensor, branch, TaskGroup of three transformations |
| `dags/reviews_to_mongo.py` | Asset-triggered load into MongoDB |
| `include/config.py` | Paths and the shared `Asset` definition |
| `include/transformations.py` | The three transformations, as plain functions |
| `mongo/aggregations.js` | The three required queries, with expected results |
| `tests/test_transformations.py` | Tests for the transformations, no Airflow needed |
| `docs/` | Evidence from real runs: task export, query output, screenshots |
| `Dockerfile` | Official image plus the MongoDB provider and pandas |
| `docker-compose.override.yaml` | MongoDB, the data mount, `PYTHONPATH`, two settings |
| `.env.example` | Every variable the stack needs |

`docker-compose.yaml` is **not** committed. It is downloaded unmodified from the
Airflow docs during setup, so there is no risk of a local edit silently diverging
from what upstream publishes. Everything this project adds lives in the override.

## Versions

| | |
|---|---|
| Airflow | 3.3.2 |
| MongoDB | 7.0 |
| `apache-airflow-providers-mongo` | 5.5.0 |
| pandas | 2.2.3 |
| Executor | CeleryExecutor (the compose file's default) |

Airflow 3 matters for the imports. `DAG`, `TaskGroup`, `Asset` and `task` come
from `airflow.sdk`; `FileSensor` and `BashOperator` come from
`apache-airflow-providers-standard`, not core. On Airflow 2.x the same DAGs would
need `airflow.sensors.filesystem`, `airflow.operators.bash`,
`airflow.decorators` and `Dataset` in place of `Asset`.

## Data

`tiktok_google_play_reviews.csv` — 307,057 Google Play reviews of TikTok,
2022-01-17 to 2022-04-05. Not committed (80 MB); put your copy in
`data/incoming/`.

Columns: `reviewId, userName, userImage, content, score, thumbsUpCount,
reviewCreatedVersion, at, replyContent, repliedAt`.

**The file has no `created_date` column.** The task specification names one, and
the nearest thing is `at`, the review timestamp. The first transformation renames
`at` to `created_date` and every later stage, including the MongoDB documents,
uses the specification's name. `repliedAt` is a different thing — the date of the
developer's reply — and is left alone.

## Setup

The commands work in both bash and fish.

```bash
# 1. The official compose file, unmodified
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml'

# 2. Directories Airflow expects, plus the data folders
mkdir -p ./dags ./logs ./plugins ./config ./data/{incoming,staging,processed}

# 3. The image: official plus the MongoDB provider and pandas
docker build -t reviews-airflow:3.3.2 .

# 4. Configuration: fill in the three values described below
cp .env.example .env
id -u
docker run --rm reviews-airflow:3.3.2 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
$EDITOR .env           # or open .env in any editor

# 5. Check the override merged correctly — see below
docker compose config | less

# 6. Start
docker compose up -d
```

**Step 1 is required.** `docker-compose.override.yaml` only adds to the upstream
file; it is not a compose file on its own. Without `docker-compose.yaml` next to
it, `docker compose` stops with `no configuration file provided`. Renamed to
`docker-compose.yaml`, it stops because `airflow-worker` has neither an image nor
a build context.

**Step 4 fills in three values in `.env`:**

- `AIRFLOW_UID` — on Linux, the number `id -u` printed. With the default 50000,
  files the containers create in `logs/` and `data/` belong to another user and
  need `sudo` to delete.
- `MONGO_PASSWORD` — any value. It becomes the MongoDB root password when the
  `mongo-data` volume is first created, and only then (see Teardown).
- `FERNET_KEY` — the key the `docker run` line printed, all 44 characters
  including the trailing `=`. Airflow encrypts connection passwords with it. A
  malformed value fails with `Incorrect padding` as soon as a connection is saved.
  Do not swap it for a new key once connections exist: their passwords would no
  longer decrypt. Rotating means `FERNET_KEY=<new>,<old>` and
  `airflow rotate-fernet-key`.

### Step 5 is not optional

`docker compose config` prints the final merged configuration. It is the one
command that catches all three ways an override file goes wrong:

- **A misspelled service name** creates a *new* service with only the override's
  keys and no image, instead of extending the real one. Airflow 3 renamed
  `airflow-webserver` to `airflow-apiserver`, so confirm the keys in your
  downloaded file match the ones in the override:
  ```bash
  docker compose -f docker-compose.yaml config --services
  ```
  Every service named in `docker-compose.override.yaml` must appear in that list.
- **Volumes not merging.** Each Airflow service should end up with
  `/opt/airflow/dags`, `/logs`, `/config`, `/plugins` from upstream *and*
  `/opt/airflow/include` and `/opt/airflow/data` from the override.
- **A missing variable.** `MONGO_PASSWORD` is interpolated as
  `${MONGO_PASSWORD:?...}`, so an unset value fails here with that message
  rather than starting an unprotected database.

### Two settings in the override

Besides MongoDB, the mounts and `PYTHONPATH`, the override sets two things the
first runs showed were needed:

- **`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`.** The upstream file loads over a
  hundred example DAGs, which bury the two this project adds.
- **`AIRFLOW_CONN_FS_DEFAULT`.** `FileSensor` uses the `fs_default` connection
  unless told otherwise, and Airflow 3 no longer creates default connections — the
  very first run failed with ``The conn_id `fs_default` isn't defined``. The
  connection holds no secret, so an environment variable is enough. Connections
  defined that way are not listed in the UI.

### The MongoDB connection

The DAG reads it from the `mongo_default` connection. Create it after the stack
is up:

```bash
docker compose exec airflow-scheduler \
  airflow connections add mongo_default \
    --conn-type mongo \
    --conn-host mongo \
    --conn-port 27017 \
    --conn-login root \
    --conn-password '<MONGO_PASSWORD from .env>' \
    --conn-schema admin
```

Or in the UI: **Admin -> Connections -> +**, connection type Mongo, the same
values. `--conn-schema` is the `authSource`, which for the root user created by
the Mongo image is `admin`.

Two things worth being clear about:

**Host is `mongo`, not `localhost`.** That is the compose service name, resolved
on the project network. Inside a container `localhost` is the container itself,
so `localhost:27017` reaches nothing. Compass on your own machine *does* use
`localhost:27017`, through the port the override publishes — the two are
different addresses for the same database.

**The env-var alternative is invisible in the UI.** Setting
`AIRFLOW_CONN_MONGO_DEFAULT='mongo://root:...@mongo:27017/?authSource=admin'`
works and is more reproducible, but connections defined by environment variable
do not appear in Admin -> Connections. Since the task asks for the connection to
be configured in Airflow, the CLI or UI route is the one that is visibly there.

## Running it

```bash
cp /path/to/tiktok_google_play_reviews.csv data/incoming/
```

Open http://localhost:8080 (`airflow` / `airflow`) and trigger `reviews_ingest`;
the Trigger dialog also unpauses it. The same from the command line:

```bash
docker compose exec airflow-scheduler airflow dags unpause reviews_ingest
docker compose exec airflow-scheduler airflow dags trigger reviews_ingest
docker compose exec airflow-scheduler airflow dags list-runs reviews_to_mongo
```

The sensor finds the file on its first poke, the branch takes the transform path,
and when `clean_content` succeeds Airflow records an asset event and starts
`reviews_to_mongo` by itself — do not trigger the second DAG by hand. The whole
chain takes about half a minute. The **Assets** page shows the link between the
two DAGs.

**`reviews_to_mongo` has to be unpaused before the event arrives.** The compose
file pauses new DAGs, and Airflow queues an asset-triggered run only for a DAG
that is unpaused at the moment of the event. Unpausing it later does not replay
the event. That is why this project's first two successful runs loaded nothing:
both fired their events while `reviews_to_mongo` was still paused, and the audit
log shows it was unpaused half an hour after the second. The DAG now sets
`is_paused_upon_creation=False`, which covers a fresh setup. The flag is ignored
for a DAG that is already registered, so on an existing setup check its toggle,
or run `airflow dags unpause reviews_to_mongo`.

### The empty-file branch

```bash
mv data/incoming/tiktok_google_play_reviews.csv /tmp/reviews_full.csv
head -1 /tmp/reviews_full.csv > data/incoming/tiktok_google_play_reviews.csv
# trigger reviews_ingest, then put the real file back:
mv /tmp/reviews_full.csv data/incoming/tiktok_google_play_reviews.csv
```

That leaves a header with no rows. `check_file_has_data` treats it as empty —
zero bytes and header-only both count, since a header-only export has nothing to
process — and routes to `log_empty_file`, whose bash script logs the fact. The
TaskGroup is skipped, the asset is not updated, and `reviews_to_mongo` correctly
does not run.

## Results

Everything below comes from real runs. Times are UTC, as in Airflow's run ids;
the screenshots show the UI's local time, UTC+3. The raw evidence is in two files:

- [`docs/task_runs.txt`](docs/task_runs.txt) — every task instance with its
  state, duration and return value, exported from the Airflow metadata database
- [`docs/mongo_results.txt`](docs/mongo_results.txt) — the output of
  `mongo/aggregations.js`, run in MongoDB

The commands that produce them are at the end of this section.

### Run history

| Started (2026) | DAG | What happened |
|---|---|---|
| 10-08 10:27 | `reviews_ingest` | Failed at the sensor: `fs_default` not defined. Now set in the override. |
| 10-08 10:36 | `reviews_ingest` | The sensor rescheduled itself every 30 s for 23 minutes until the file was copied in, then the transform path ran. Asset event at 10:59, but `reviews_to_mongo` was paused, so nothing loaded. |
| 10-19 11:13 | `reviews_ingest` | Transform path. Asset event at 11:13, `reviews_to_mongo` still paused. |
| 10-19 12:09 | `reviews_ingest` | Empty-file path, header-only input. No asset event, by design. |
| 10-24 17:19 | `reviews_ingest` | Transform path. Asset event at 17:19:50. |
| 10-24 17:19 | `reviews_to_mongo` | Asset-triggered, started a second after the event. 307,057 documents loaded. |

### The full run

`reviews_ingest` started at 17:19:29 on 2026-10-24, and `reviews_to_mongo`
finished at 17:20:02 — 33 seconds for the whole chain.

| Task | Time | Result |
|---|---|---|
| `wait_for_file` | 3.5 s | file found on the first poke |
| `check_file_has_data` | 0.1 s | chose `transform_reviews.replace_nulls` |
| `log_empty_file` | — | skipped |
| `transform_reviews.replace_nulls` | 5.7 s | 307,057 rows · 703,092 empty fields filled · 18 null words replaced |
| `transform_reviews.sort_by_created_date` | 4.2 s | 307,057 rows · 0 unparseable dates |
| `transform_reviews.clean_content` | 4.5 s | 307,057 rows · 41,278 contents changed · 7,320 left empty |
| `load_to_mongodb` | 6.6 s | 307,057 documents inserted |
| `verify_load` | 2.6 s | 307,057 documents, same as the file · 53,238 short · 79 days |

The counts are each task's return value. The two earlier successful runs
returned the same numbers.

703,092 breaks down as 306,938 `replyContent` + 306,938 `repliedAt` +
89,216 `reviewCreatedVersion` — almost no review gets a developer reply. The 18
null words are all in `content`: `None` ×12, `N/A` ×4, `Null` ×2.

### The empty-file run

On 2026-10-19 at 12:09, with a header-only file in place, `check_file_has_data`
chose `log_empty_file`, whose bash script logged:

```
[2026-10-19T12:09:57+00:00] EMPTY INPUT: /opt/airflow/data/incoming/tiktok_google_play_reviews.csv contains no data rows; skipping transformations.
```

The three transforms were skipped, so no asset event was recorded and
`reviews_to_mongo` did not run. The run itself counts as a success: a run
succeeds when every final task ends in success or skipped, and an empty export
is a handled case, not an error.

`docs/task_runs.txt` shows the skips happening in two ways. `replace_nulls` has
no duration, because the branch skipped it directly. `sort_by_created_date` and
`clean_content` show 0.0, because the scheduler skipped them afterwards: their
default trigger rule, `all_success`, cannot be met once the task before them is
skipped.

### The three aggregations

Run in MongoDB by `mongo/aggregations.js`, with the output in
`docs/mongo_results.txt`. `verify_load` runs the same pipelines and prints the
results to its task log. All three match the values computed independently in
pandas before the load.

**1. Top-5 most frequent comments**

| content | count |
|---|---|
| Good | 12,164 |
| Nice | 10,050 |
| `-` | 7,338 |
| Nice app | 4,950 |
| Good app | 3,678 |

`-` placing third is a real consequence of the specification's own rules, not a
bug. 7,320 reviews were nothing but emoji, so stage 3 leaves them empty, and
stage 1's rule says an empty value becomes `-`; the other 18 are the null words
above. The counts for real comments also rose against the raw file — `Good` was
11,452 before — because `Good 😊` and `Good` become the same string once the
emoji is gone.

The specification does not say what to do with content that transformation 3
empties. Treating it as null is consistent with stage 1; the alternative is to
leave it as `""`, which keeps `-` out of the top five at the cost of having two
spellings of "nothing" in the data. `aggregations.js` also runs the query
without the placeholder: Good, Nice, Nice app and Good app, then Great (2,288)
in fifth place.

**2. Content shorter than five characters** — 53,238 documents.

`$strLenCP`, not `$strLenBytes`: CP counts characters, which is what the task
asks for. Bytes would measure a four-character Cyrillic review as 8 and miss it.
The predicate cannot use an index, because the length is computed per document at
query time; indexing it would mean storing the length as its own field at load.

The raw file has 48,454 such reviews. Stage 3 adds 4,784 to that: 2,039 reviews
made of five or more emoji, which are now `-`, and 2,745 where stripping the
emoji left a short word (`Good 💯` becomes `Good`). The other 5,281 reviews that
stage 3 emptied were already shorter than five characters.

**3. Average rating per day** — 79 days, 2022-01-17 to 2022-04-05. The mean over
all 307,057 reviews is 4.306.

Each group key is a BSON Date such as `ISODate('2022-01-17T00:00:00.000Z')`,
which is the timestamp type the task asks for. `$dateTrunc` produces it, and that
only works because the loader converts `created_date` to a real `datetime` before
inserting; on a string column `$dateTrunc` errors. Grouping with `$dateToString`
instead would return text and sort lexicographically.

Daily averages stay between 4.20 and 4.38, with one clear dip: 2022-03-10, at
4.10 over 4,214 reviews. The first day is partial — the data starts at 10:41 — so
it has only 2,279 reviews.

Days are UTC, matching source timestamps that carry no offset. Pass
`timezone: "Europe/Minsk"` to bucket by local day — it moves the boundaries and
changes the averages.

### Screenshots

A full run: `log_empty_file` skipped, `transform_reviews` succeeded. The sensor's
log on the right shows it rescheduling every 30 seconds until the file arrived.

![Full run](docs/img/full_run.png)

The empty-file run: `log_empty_file` succeeded and the three transforms were
skipped. The grid on the left shows every run up to that point.

![Empty-file run](docs/img/empty_run.png)

The asset, with the task that produces it and the DAG scheduled on it.

![Assets](docs/img/assets.png)

`mongo_default` under Admin -> Connections. `fs_default` is not listed, because
it comes from an environment variable.

![Connections](docs/img/connections.png)

### Reproducing the evidence

```bash
docker compose exec -T postgres psql -U airflow -d airflow -c "SELECT ti.dag_id, ti.run_id, ti.task_id, ti.state, round(ti.duration::numeric, 1) AS seconds, x.value FROM task_instance ti LEFT JOIN xcom x ON x.dag_id = ti.dag_id AND x.run_id = ti.run_id AND x.task_id = ti.task_id AND x.map_index = ti.map_index AND x.key = 'return_value' ORDER BY ti.start_date" > docs/task_runs.txt

docker compose cp mongo/aggregations.js mongo:/tmp/aggregations.js
docker compose exec -T mongo sh -c 'mongosh --quiet -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin /tmp/aggregations.js' > docs/mongo_results.txt
```

The first reads Airflow's metadata database, the Postgres from the compose file.
The second runs the script with the mongosh inside the mongo container, which
already has the credentials in its environment, so no password is typed.

## Tests

```bash
pip install "pandas==2.2.3" pytest
python -m pytest tests/ -v
```
