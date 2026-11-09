# DynamoDB tables

Five tables, billed on demand, with string keys, defined in
`src/bikes/resources.py`. Key names avoid DynamoDB's
[reserved words](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html)
such as `DATE`, `MONTH` and `NAME`.

| Table | Partition key | Sort key | One item is |
|---|---|---|---|
| `bike_trips` | `trip_date` (`2019-06-06`) | `station_part` (`Kamppi (M)#0`) | the raw trips that left one station on one day |
| `bike_daily_metrics` | `trip_month` (`2019-06`) | `trip_date` | one day's trip count and averages |
| `bike_monthly_metrics` | `trip_year` (`2019`) | `trip_month` | the same for one month |
| `bike_station_metrics` | `trip_month` | `station_name` | departures from and returns to one station in one month |
| `bike_loads` | `trip_month` | | which files the month was loaded from |

So the days of a month, the months of a year and the stations of a month are
each one Query; one station's trips on a day are a Query on `trip_date` with
`station_part` beginning with `Kamppi (M)#`.

## Items

**`bike_trips`**

| Attribute | Type | |
|---|---|---|
| `station_name` | S | the departure station |
| `part` | N | a station's day is cut into parts of at most 1,000 trips |
| `trip_count` | N | trips in this item |
| `trips_csv_gz` | B | gzip of a CSV file: the header and the trips' lines, as in the source |

**`bike_daily_metrics`**, and **`bike_monthly_metrics`** with `days` added

| Attribute | Type | |
|---|---|---|
| `trips` | N | every trip that departed that day |
| `valid_trips` | N | the trips the averages use (the rule below) |
| `avg_distance_m` | N | mean distance |
| `avg_duration_s` | N | mean of `duration (sec.)`, not return minus departure |
| `avg_speed_kmh` | N | mean of each trip's distance / duration |
| `avg_temperature_c` | N | mean of the hourly readings, each hour counted once |
| `temperature_hours` | N | how many hourly readings that mean is over |

A valid trip is at least 10 m and 10 s long, at most 5 hours (the
[longest an HSL ride may last](https://hsl.fi/en/citybikes/helsinki/faq)) and
at most 30 km/h. That leaves out about 5% of trips, mostly with zero distance.
The file's `avg_speed (km/h)` column is not used: it holds km/min
(`docs/dataset_profile.txt`). Monthly averages come from the month's trips,
not from the daily averages, which would weigh a quiet day like a busy one.

**`bike_station_metrics`**: `departures` and `returns` (N), from the Spark
job's file.

**`bike_loads`** ([loader.md](loader.md) says how it is used)

| Attribute | Type | |
|---|---|---|
| `load_state` | S | `loading`, `loaded` or `failed` |
| `raw_etag`, `metrics_etag` | S | the S3 ETags of the two files loaded |
| `claim_id`, `claimed_until` | S | the call that claimed the month, and until when |
| `started_at`, `finished_at` | S | ISO 8601, UTC |
| `trips`, `valid_trips`, `days`, `stations`, `raw_items`, `removed_items` | N | what the load wrote and removed |
| `load_seconds`, `read_seconds`, `build_seconds`, `write_seconds` | N | the load's time: total, S3, pandas, DynamoDB |
| `load_error` | S | why it failed, at most 1,000 characters |

## Why the raw trips are grouped

One item per trip does not fit LocalStack. Measured with `python -m
bikes.probe` on 4.14.0: on disk, DynamoDB writes 76 items a second, about 44
hours for all trips; in memory (`DYNAMODB_IN_MEMORY=1`), with 100,000 trips
of June 2019:

| Layout | Memory per trip | All 12.16M trips | Writes |
|---|---|---|---|
| One item per trip | 1,956 bytes | 22.1 GiB | 1,917 trips/s |
| One item per station and day | 380 bytes | 4.3 GiB | 37,262 trips/s |
| The same, gzip-compressed | 84 bytes | 0.9 GiB | 148,258 trips/s |

Compressing large values into a Binary attribute is also what
[AWS suggests](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-use-s3-too.html),
and it is cheaper: DynamoDB bills every write per started 1 KB. The cost is
that a single trip has no key of its own; the dashboards only need the
metrics tables.
