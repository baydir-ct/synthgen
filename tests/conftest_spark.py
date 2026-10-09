"""Local Spark session for tests and scripts. Imports nothing from synthgen."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys


def ensure_local_spark_env() -> None:
    """Workers use this interpreter; Spark 3.5/4.0 need Java 17 or 21.
    On macOS, pick a supported JDK automatically if JAVA_HOME is unset."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if os.environ.get("JAVA_HOME") or not shutil.which("/usr/libexec/java_home"):
        return
    for version in ("21", "17"):
        found = subprocess.run(["/usr/libexec/java_home", "-v", version], capture_output=True, text=True)
        if found.returncode == 0:
            os.environ["JAVA_HOME"] = found.stdout.strip()
            return


def local_spark(warehouse: str):
    ensure_local_spark_env()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("synthgen-local")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session
