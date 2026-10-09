"""Smoke-test the local analytics stack before any data is touched.

Success looks like two log lines:
- Spark version with a counted range of exactly 1_000_000 rows
- DuckDB version from ``SELECT version()``

The file on disk was empty when this project was picked up, so this script
is the Phase 1 environment check. It does not read ``data/raw``.
"""

from __future__ import annotations

import logging
import os
import sys

import duckdb
from pyspark.sql import SparkSession

# 1_000_000 is large enough to prove the JVM can allocate and count, and
# small enough that a 2g driver finishes in seconds on a 24 GB laptop.
SMOKE_ROWS = 1_000_000
# Leave most of the 24 GB machine free. Later pipeline jobs raise this to 12g.
DRIVER_MEMORY = "2g"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("check_env")


def build_spark() -> SparkSession:
    """Local Spark session used only for the row-count smoke test."""
    # local[*] uses every core. This check is CPU-light; later jobs may pin
    # fewer cores so the OS and DuckDB still have headroom.
    return (
        SparkSession.builder.appName("env-check")
        .master("local[*]")
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def check_spark() -> None:
    """Create a 1_000_000-row range and count it through the JVM."""
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        logger.error("JAVA_HOME is unset. Export it to a JDK 17 home before running.")
        sys.exit(1)
    logger.info("JAVA_HOME=%s", java_home)

    spark = build_spark()
    try:
        counted = spark.range(SMOKE_ROWS).count()
        # The checkpoint looks for this exact shape: version, then "rows: 1000000".
        logger.info("Spark version %s rows: %s", spark.version, counted)
        if counted != SMOKE_ROWS:
            logger.error("Expected %s rows, counted %s", SMOKE_ROWS, counted)
            sys.exit(1)
    finally:
        spark.stop()


def check_duckdb() -> None:
    """Confirm the in-process DuckDB build that later reads gold Parquet."""
    # In-memory connection: no file is created under data/.
    connection = duckdb.connect(database=":memory:")
    try:
        version = connection.execute("SELECT version()").fetchone()[0]
    finally:
        connection.close()
    logger.info("DuckDB version %s", version)


def main() -> None:
    """Run the Spark row-count check and the DuckDB version check."""
    logger.info("Python %s", sys.version.split()[0])
    check_spark()
    check_duckdb()
    logger.info("Environment check passed")


if __name__ == "__main__":
    main()
