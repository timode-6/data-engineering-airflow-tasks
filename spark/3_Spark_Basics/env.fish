# spark/3_Spark_Basics/env.fish 
set -l here (dirname (status filename))

if not python -c "import pyspark" 2>/dev/null
    echo "Activate the PySpark venv first — see README.md"
    exit 1
end

set -x JAVA_HOME /usr/lib/jvm/java-8-openjdk/jre
set -x SPARK_HOME (python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
set -x SPARK_CONF_DIR (realpath $here)/conf
set -x SPARK_LOCAL_IP 127.0.0.1
set -x SPARK_MASTER_HOST 127.0.0.1
set -x PYSPARK_PYTHON (which python)
set -x PYSPARK_DRIVER_PYTHON (which python)
