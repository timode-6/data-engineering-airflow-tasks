# Findings — query plans and Spark UI metrics

`spark_basics.py` reads two CSVs and a partitioned parquet dataset, filters and joins,
unions the result, and writes a CSV. It was run on a local standalone cluster — one
master and two workers with different resources (2 cores / 3 GB and 3 cores / 4 GB) —
rather than in local mode.

![Spark master UI](3_Spark_Basics/img/master_ui.png)

Plans were captured with both `queryExecution().toString()` and
`explainString(..., 'EXTENDED')`; the two outputs are identical. Metrics come from the
history server's SQL tab.

Everything below was checked against the source data, not just read off the plan.

---

## 1. Parsed → Analyzed: a cast nobody wrote

The analyzer resolves types and inserts exactly one node:

```
Project [..., cast(net_worth#18 as string) AS net_worth#68, ...]
```

Branch 1 extracts `net_worth` to `int` so it can filter `> 60`. Branch 2 reads the parquet
with `people_schema`, where `net_worth` is a `string`. `union()` matches columns by
position and needs compatible types, so Spark casts the integer back to a string. The
extraction work is undone for the output. See §6 for why this matters.

## 2. Analyzed → Optimized: rule-based optimizations

| Rule | Analyzed | Optimized |
| --- | --- | --- |
| Predicate pushdown through join | `Filter isnotnull(age)` above the Join | merged into the filter below it, before the join |
| Pushdown through project | `Filter (net_worth#18 > 60)` above the Project computing `net_worth#18` | on the raw relation, expression inlined as `cast(regexp_extract(net_worth#2, ...)) > 60` |
| Combine filters | two Filter nodes | one Filter joined with `AND` |
| Collapse project | two stacked Projects (regex, then CASE) | one Project |
| Column pruning | country relation read whole | `Project [country#12]` inserted |

The redundant parquet `Project`, which selected every column in the same order, is removed.

Four **inferred filters** appear that are not in the code:

- `isnotnull(net_worth#2)` — `> 60` can never be true for null
- `isnotnull(CASE WHEN nationality ...)` and `isnotnull(country#12)` — an inner join never
  matches null keys, so they are discarded before the join
- `isnotnull(nationality#61)` — implied by `= Russia`

The filter on `age` is the clearest case: it is written at the very end of the script,
after the join, and ends up evaluated during the CSV scan.

## 3. Optimized → Physical: strategy choices

**`BroadcastHashJoin [...] BuildRight`.** The country table is under the default 10 MB
`autoBroadcastJoinThreshold`, so it is sent whole to every executor instead of both sides
being shuffled.

**Whole-stage codegen `*(1)`, `*(2)`, `*(3)`.** Operators sharing a number are fused into
one generated function. Stage 2 fuses scan, filter, project, join and project.

**Pushed versus data filters on the people CSV:**

```
DataFilters:   [isnotnull(net_worth), (cast(regexp_extract(...)) > 60), isnotnull(age), ...]
PushedFilters: [IsNotNull(net_worth), IsNotNull(age)]
```

Only the two simple null checks reach the CSV reader. The regex comparison and the CASE
null check are computed expressions a file reader cannot evaluate, so they stay in the
`Filter` node above the scan. Pushed filters are hints; Spark still evaluates them in the
Filter for correctness.

**Projection pushdown on the country CSV:** `ReadSchema: struct<country:string>` — one of
three columns read.

**The parquet scan:**

```
PartitionFilters: [isnotnull(nationality#61), (nationality#61 = Russia)]
PushedFilters:    []
ReadSchema:       struct<rank:int,name:string,net_worth:string,bday:string,age:int>
```

- **Partition pruning.** `PushedFilters` is empty because the only filter is on the
  partition column and is handled entirely by skipping directories.
- **`nationality` is absent from `ReadSchema`.** The file's embedded schema stores
  `rank, name, net_worth, bday, age` and no `nationality`. The value exists only in the
  directory name `nationality=Russia/` and is reconstructed from the path. That is what
  makes pruning possible: the value is known without opening the file.
- **`Batched: true` → `ColumnarToRow`.** Parquet uses the vectorized reader, then converts
  to rows so it can be unioned with the row-based CSV branch, which is `Batched: false`.

## 4. Measured in the SQL tab

![SQL tab DAG](3_Spark_Basics/img/sql_dag.png)

| Node | Rows | |
| --- | --- | --- |
| people CSV on disk | 100 | |
| Scan csv | **95** | pushed `IsNotNull(age)` dropped 5 rows inside the reader |
| Filter | 21 | the non-pushable regex and CASE conditions |
| BroadcastHashJoin | 21 | every remaining person matched a country |
| Scan parquet | 9 | `number of partitions read: 1` — one of 25 |
| Union → write | 30 | 21 + 9 |

The source CSV has exactly 5 null `age` values and no null `net_worth`, which is why the
scan emits 95 rather than 100: the pushed predicates execute during parsing.

**The broadcast is ~700× its data.** The country CSV is 1 383 B; the broadcast reports
`data size: 1028.0 KiB`. `HashedRelation` preallocates memory pages regardless of
content, so even a tiny table costs about a megabyte per executor.

**Fixed costs dominate.** `time to collect: 1.7 s` for 77 rows is the most expensive step
in the job. WholeStageCodegen (3), the parquet branch, took 588 ms to produce 9 rows — the
slowest of the three stages. None of that is data processing: it is task scheduling,
executor startup, reader initialization and JIT warm-up. At this size the plan
optimizations are real but save microseconds against seconds of setup.

**Two output files**, one per union branch: `Union` concatenates partitions without a
shuffle, so each side writes its own part file.

## 5. Measured in the Stages tab

![Stages](3_Spark_Basics/img/stages.png)

| Stage | Description | Tasks | Input | Output |
| --- | --- | --- | --- | --- |
| 0 | broadcast exchange | 1/1 | 1 383.0 B | — |
| 1 | `csv at NativeMethodAccessorImpl.java:0` | 2/2 | 9.9 KiB | 1 484.0 B |

**Shuffle Read and Shuffle Write are empty for both stages.** The whole job runs without a
shuffle: the broadcast join avoids one, the union concatenates partitions rather than
redistributing them, and nothing groups or sorts across partitions. That is the same fact
the physical plan states — `BroadcastExchange` is the only exchange — seen from the
execution side.

Two stages and three tasks total for a job that reads 307 rows across three sources. The
stage boundary exists only because the broadcast must be built and collected before the
probe side can use it.

## 6. Two bugs the plan exposes

**Mixed formats in one column.** The parquet stores `net_worth` as the original text —
`"$70 Billion"`, `"$17 Billion"` in the Russia partition. Branch 1 emits the extracted
number cast back to string, `"240"`. So `1.csv` holds both `"240"` and `"$70 Billion"`
in the same column, and anything that parses it downstream breaks on half the rows. The
cast inserted in §1 is what reveals this.

**Inconsistent filtering between branches.** The `> 60` filter and the nationality
mapping apply only to branch 1. The Russia branch skips both, which is why a
`"$17 Billion"` entry appears in output that is otherwise strictly above 60.

Neither is a Spark problem — both come from `union()` joining two differently processed
datasets. `unionByName` would at least guard against column-order mistakes, but the real
fix is to apply the same transformations to both branches before combining them.

## 7. Data quality in the input

The first line of `wiki_number_of_billionaires.csv` is quoted as a single field, so one
row has `country = 'World,"2,668",0.35'` and nulls in the other columns. It passes the
inferred `isnotnull(country)` filter and can never match a nationality, so it doesn't
affect the result — but the pipeline carries it silently.

## 8. Notes on the course material

**Partition pruning does not require a Hive metastore.** The module README says it only
applies to tables registered in a metastore and not to plain DataFrames. This run shows
static partition pruning on a plain DataFrame reading a directory-partitioned source.
The README appears to conflate it with *dynamic* partition pruning, the join-driven
variant added in Spark 3.0, which is more restricted.

**Pruning works from directory names, not metadata files.** The explanatory document
describes Spark consulting the extra files it writes to decide which partitions to skip.
The `_SUCCESS` file is an empty commit marker. Spark lists the directories, parses
`key=value` from each path, and skips non-matching ones without opening any file in
them — consistent with `nationality` being absent from the file schema.
