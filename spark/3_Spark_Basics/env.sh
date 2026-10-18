# Resolve this file's directory, so SPARK_CONF_DIR does not depend on where you
# were standing when you sourced it.
_here="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# Guard: without the venv active, SPARK_HOME below resolves to nothing and Spark
# fails later with an error that says nothing about the real cause.
if ! python -c "import pyspark" >/dev/null 2>&1; then
    echo "Activate the PySpark venv first — see README.md" >&2
    return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk/jre}"

# Taken from the installed package rather than hardcoded: the pip wheel ships
# Spark inside site-packages, and the path changes with the Python version.
export SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"

# The wheel has no conf/ of its own, so point Spark at the project-local one.
export SPARK_CONF_DIR="$_here/conf"

# Pin both ends to the venv interpreter. Without this the driver and the workers
# can end up on different Pythons, which fails with a version-mismatch error.
export PYSPARK_PYTHON="$(command -v python)"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

unset _here
