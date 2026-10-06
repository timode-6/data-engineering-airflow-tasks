# Apache Spark course

Work for the [spark_demo_course](https://github.com/andrewD46/spark_demo_course):
the PySpark basics notebook, and a local standalone cluster with query-plan analysis

| Path | |
| --- | --- |
| `1_PySpark_Basics/PySpark_Basics.ipynb` | the 33 notebook tasks, with outputs |
| `3_Spark_Basics/spark_basics.py` | the job run on the cluster |
| `3_Spark_Basics/plans.txt`, `plans1.txt` | captured query plans (identical - see the script) |



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



| Read | Wall time | Job in Spark UI |
| --- | --- | --- |
| `inferSchema=true` | 4.24 s | yes - schema inference scans the whole file, so a lazy `read` becomes an action |
| explicit `StructType` | milliseconds | no - `read` stays a genuine transformation |

---

## Module 3 - standalone cluster

### Configuration

Keep the config in the project rather than inside `site-packages`, which a reinstall wipes:

```bash
cd 3_Spark_Basics
mkdir -p conf for_history
cat > conf/spark-defaults.conf <<EOF
spark.eventLog.enabled true
spark.eventLog.dir file://$PWD/for_history
spark.history.fs.logDirectory file://$PWD/for_history
EOF

export SPARK_CONF_DIR=$PWD/conf
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_HOST=127.0.0.1
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
```

`SPARK_MASTER_HOST` pins the master to a predictable address. Without it the master URL
is built from the hostname, which on many Linux setups resolves to `127.0.1.1`

`SPARK_CONF_DIR` is not committed — it holds absolute paths. Create it once:

```bash
cd 3_Spark_Basics
mkdir -p conf for_history
printf 'spark.eventLog.enabled true\nspark.eventLog.dir file://%s/for_history\nspark.history.fs.logDirectory file://%s/for_history\n' "$PWD" "$PWD" > conf/spark-defaults.conf
```

Then, with the PySpark venv active:

```bash
source env.fish      # sets JAVA_HOME, SPARK_HOME, SPARK_CONF_DIR and the PySpark interpreters
```

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

The `Location` fields in `plans.txt` contain the paths from the machine that produced
them. That's expected: the plan records where the data actually was
