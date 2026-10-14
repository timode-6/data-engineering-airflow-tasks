# Apache Spark course

Work for the [spark_demo_course](https://github.com/andrewD46/spark_demo_course):
the PySpark basics notebook, and a local standalone cluster with query-plan analysis

| Path | |
| --- | --- |
| `1_PySpark_Basics/PySpark_Basics.ipynb` | the notebook tasks, with outputs |
| `3_Spark_Basics/spark_basics.py` | the job run on the cluster |
| `3_Spark_Basics/plans.txt`, `plans1.txt` | captured query plans (identical - see the script) |
| [`3_Spark_Basics/FINDINGS.md`](3_Spark_Basics/FINDINGS.md) | analysis of the plans and the Spark UI metrics |

---

## Environment

The course README targets Windows. These are the Linux equivalents, and every version
constraint below was hit in practice rather than copied from documentation

| | Version | Why |
| --- | --- | --- |
| Java | **8** | Spark 3.1.3 fails on Java 17+ with `NoSuchMethodException: java.nio.DirectByteBuffer.<init>(long,int)` |
| Python | **3.9** | PySpark 3.1.3 supports 3.6–3.9 only |
| PySpark | **3.1.3** | matches the course; `spark_basics.py` calls an internal JVM API whose signature moves between versions |

```bash
uv venv --python 3.9 .venv
source .venv/bin/activate
uv pip install -r requirements.txt

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk/jre
java -version
```

```bash
export SPARK_HOME=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
```

---

## Module 1 - notebook

Put `ds_salaries.csv` ([Kaggle](https://www.kaggle.com/datasets/ruchi798/data-science-job-salaries))
next to the notebook, then `jupyter notebook`.

The notebook is run in two sessions, with a kernel restart between them, so the
predefined-schema read is measured from a cold session rather than one that has
already read the file. The execution counts show the restart as a single reset.

### inferSchema against an explicit schema

The difference that matters is whether a Spark job is created:

| Read | Spark job | Why |
| --- | --- | --- |
| `inferSchema=true` | **yes** | schema inference scans the whole file, so a lazy `read` becomes an action |
| explicit `StructType` | **no** | `read` stays a transformation; nothing is read until the first action |

Wall times from the saved outputs: inferSchema **2.8 s**, re-read of the same file
**167 ms**, predefined schema **1.17 s**.

The predefined-schema read is not doing 1.17 s of work. It is the first Spark call
in the restarted kernel, so it pays JVM and py4j start-up. Measured with a throwaway
`spark.range(1).count()` absorbing start-up first, the same two reads come out at
**660 ms** and **171 ms** — about a second of each figure above is the session waking
up, not reading.

So the wall clock is a weak proxy here: on a 607-row file, start-up is larger than
the thing being measured. The job count in the Spark UI is the direct evidence, which
is why the table above is about jobs rather than seconds.

---

## Module 3 - standalone cluster

### Configuration

`conf/` is not committed — it holds absolute paths, which differ per machine. Create
it once, from inside `3_Spark_Basics`:

```bash
cd 3_Spark_Basics
mkdir -p conf for_history
cat > conf/spark-defaults.conf <<EOF
spark.eventLog.enabled true
spark.eventLog.dir file://$PWD/for_history
spark.history.fs.logDirectory file://$PWD/for_history
EOF
```

Keeping it in the project rather than inside `site-packages` means a reinstall of
PySpark does not wipe it.

Then set the environment in each terminal, with the venv active:

```bash
source env.sh        # bash / zsh
```

```fish
source env.fish      # fish
```

Both set `JAVA_HOME`, `SPARK_HOME`, `SPARK_CONF_DIR` and the PySpark interpreters.
`JAVA_HOME` defaults to the Arch OpenJDK 8 path — export your own before sourcing if
Java 8 lives elsewhere.

The cluster also needs these, which the scripts do not set because they are specific
to running the standalone master locally:

```bash
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_HOST=127.0.0.1
```

`SPARK_MASTER_HOST` pins the master to a predictable address. Without it the master URL
is built from the hostname, which on many Linux setups resolves to `127.0.1.1`

### Cluster

One terminal per process, each with the environment above. Use `spark-class` rather than
`sbin/start-worker.sh` cuz the script tracks workers by PID file, so a second invocation on
the same host refuses to start, and it can't create two workers with different
resources

```bash
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master --host 127.0.0.1
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7077 --cores 2 --memory 3g
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7077 --cores 3 --memory 4g
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.history.HistoryServer
```

| UI | Address |
| --- | --- |
| Master | `localhost:8080` - expect 2 workers, 5 cores, 7 GB |
| Application | `localhost:4040` |
| History server | `localhost:18080` |

### Run

```bash
$SPARK_HOME/bin/spark-submit --master spark://127.0.0.1:7077 spark_basics.py
```

`plans.txt` and `plans1.txt` were produced by running `spark_basics.py` from this
directory on the standalone cluster. The absolute paths in the `Location` fields are
from the machine that ran it.

The analysis of those plans — the analyzer-inserted cast, the rule-based
optimizations, predicate pushdown, partition pruning and the row counts through the
SQL tab — is in [`FINDINGS.md`](3_Spark_Basics/FINDINGS.md).
