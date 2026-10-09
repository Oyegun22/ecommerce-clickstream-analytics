"""Land the October 2019 REES46 CSV as partitioned Parquet (bronze).

Bronze is a typed copy of the source, not a cleaned table. Every row is
kept. Empty CSV fields stay null. Prices are not filtered. The only added
field is ``event_date``, and it exists so later reads can skip days.

Decisions worth defending in an interview:

- Explicit schema and ``FAILFAST``. A value that does not match the schema
  stops the load. Silent nulls would hide a bad line inside 42 million rows.
- ``category_id`` is a long. Published values are about 2e18, and signed
  int64 holds up to about 9.22e18. A string would waste space and slow joins.
- ``event_time`` is parsed as UTC and the session timezone is UTC. The
  laptop timezone would move late-evening UTC events onto the next local day
  and split one source day across two partitions.
- The partition column is not a business attribute. Spark stores it in the
  directory name (``event_date=2019-10-01``) and puts it back on read.
- Compression is zstd. Snappy writes faster. zstd is smaller on repeated
  category and brand strings, and both Spark 3.5 and DuckDB read it.
- The driver heap defaults to 12g, the project cap on a 24 GB machine.
  ``local[6]`` out of 12 cores leaves CPU for the OS and for garbage collection.
- There is no shuffle before the write. The CSV is time-ordered, so each
  ~128 MB input split already sits in one day or on a midnight boundary.
  Forcing one file per day would shuffle the whole month on a 12 GB heap.

Re-runs overwrite ``data/bronze/events``. ``data/raw`` is only read.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "2019-Oct.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "events"
RAW_ROOT = PROJECT_ROOT / "data" / "raw"

# Project cap. The OS and the Python process need the rest of the 24 GB.
MAX_DRIVER_BYTES = 12 * 1024**3
DEFAULT_DRIVER_MEMORY = "12g"
# 12 hardware cores. Six leaves headroom so a CSV parse does not pin the machine.
DEFAULT_MASTER = "local[6]"
# October 2019 UTC. A date outside this month means the parser or timezone is wrong.
MONTH_START = "2019-10-01"
MONTH_END = "2019-10-31"
TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss 'UTC'"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("ingest_bronze")


def memory_to_bytes(value: str) -> int:
    """Parse Spark memory strings such as ``12g`` or ``512m``."""
    text = value.strip().lower()
    if text.endswith("g"):
        return int(float(text[:-1]) * 1024**3)
    if text.endswith("m"):
        return int(float(text[:-1]) * 1024**2)
    raise ValueError(f"Use a Spark size like 12g or 512m, got {value!r}")


def source_schema() -> StructType:
    """Column types for the nine REES46 fields. All are nullable.

    Empty CSV fields become null. That is the raw value. Silver decides
    whether a null brand or a null session is filled or dropped.
    Price stays a double here so a weird scale cannot fail the landing.
    Money math belongs in silver and gold, where a decimal cast is explicit.
    """
    return StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )


def assert_safe_output(output: Path) -> None:
    """Refuse to write into the raw drop. Raw files are immutable."""
    resolved = output.resolve()
    raw = RAW_ROOT.resolve()
    if resolved == raw or raw in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, raw)
        sys.exit(1)


def build_spark(driver_memory: str, master: str) -> SparkSession:
    """Start a local Spark session with a heap that was fixed before JVM launch.

    ``SparkSession.builder.config("spark.driver.memory", ...)`` is not enough
    on its own. The driver JVM is already starting, and ``-Xmx`` has to be
    passed to ``spark-submit`` through ``PYSPARK_SUBMIT_ARGS``. The heap is
    checked after launch. A silent 1g default would OOM halfway through the CSV.
    """
    requested = memory_to_bytes(driver_memory)
    if requested > MAX_DRIVER_BYTES:
        logger.error("Driver memory %s exceeds the 12g cap", driver_memory)
        sys.exit(1)
    if not os.environ.get("JAVA_HOME"):
        logger.error("JAVA_HOME is unset. Point it at JDK 17 before running.")
        sys.exit(1)

    # Derby's log otherwise lands in the repo root on local-mode startup.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {driver_memory} "
        "--driver-java-options -Dderby.system.home=/tmp/rees46-derby "
        "pyspark-shell"
    )
    spark = (
        SparkSession.builder.appName("bronze-ingest")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.compression.codec", "zstd")
        # TIMESTAMP_MICROS is what DuckDB expects. Spark's older INT96 default
        # is an unnecessary compatibility trap for the gold-layer queries.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / "spark-warehouse"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    max_heap = spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
    logger.info("JAVA_HOME=%s", os.environ.get("JAVA_HOME"))
    logger.info("Spark version %s master %s", spark.version, master)
    logger.info("JVM maxMemory=%s bytes (requested %s)", max_heap, driver_memory)
    if max_heap < int(requested * 0.6):
        logger.error(
            "JVM heap is smaller than requested. Refusing to read the CSV. "
            "maxMemory=%s requested=%s",
            max_heap,
            requested,
        )
        spark.stop()
        sys.exit(1)
    return spark


def read_events(spark: SparkSession, source: Path) -> DataFrame:
    """Read the CSV with the fixed schema. Do not infer types.

    Inference would scan a large prefix of a 5.6 GB file and can turn the
    19-digit ``category_id`` into a double, which corrupts the id.
    """
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(source_schema())
        .csv(str(source))
    )


def with_event_date(events: DataFrame) -> DataFrame:
    """Parse the UTC timestamp and derive the partition date. Drop no rows."""
    parsed = events.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_time"), TIMESTAMP_PATTERN),
    )
    return parsed.withColumn("event_date", F.to_date(F.col("event_time")))


def write_events(events: DataFrame, output: Path) -> None:
    """Overwrite bronze Parquet partitioned by ``event_date``.

    No repartition. See the module docstring for why the time-ordered CSV
    already produces an acceptable file layout.
    """
    (
        events.write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")
        .parquet(str(output))
    )


def directory_bytes(path: Path) -> int:
    """Bytes on disk for every file under ``path``, including ``_SUCCESS``."""
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def validate(spark: SparkSession, output: Path, csv_bytes: int) -> None:
    """Read the Parquet back and log the figures Phase 2 has to report.

    One grouped scan produces the row count, the event_type mix, and the
    partition count. A second full scan is not worth it at this size.
    """
    stored = spark.read.parquet(str(output))
    grouped = (
        stored.groupBy("event_date", "event_type")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.when(F.col("event_time").isNull(), 1).otherwise(0)).alias(
                "null_event_time"
            ),
        )
        .collect()
    )
    if not grouped:
        logger.error("Bronze output has no rows: %s", output)
        sys.exit(1)

    total_rows = 0
    null_event_time = 0
    by_type: dict[str, int] = {}
    by_date: dict[str, int] = {}
    for row in grouped:
        total_rows += row["rows"]
        null_event_time += row["null_event_time"]
        event_type = row["event_type"] if row["event_type"] is not None else "NULL"
        event_date = str(row["event_date"]) if row["event_date"] is not None else "NULL"
        by_type[event_type] = by_type.get(event_type, 0) + row["rows"]
        by_date[event_date] = by_date.get(event_date, 0) + row["rows"]

    parquet_bytes = directory_bytes(output)
    parquet_files = sum(1 for item in output.rglob("*.parquet") if item.is_file())
    ratio = (csv_bytes / parquet_bytes) if parquet_bytes else 0.0

    logger.info("BRONZE_ROWS rows=%s", total_rows)
    logger.info("BRONZE_NULL_EVENT_TIME rows=%s", null_event_time)
    for event_type in sorted(by_type):
        logger.info("BRONZE_EVENT_TYPE event_type=%s rows=%s", event_type, by_type[event_type])
    for event_date in sorted(by_date):
        logger.info("BRONZE_DATE event_date=%s rows=%s", event_date, by_date[event_date])
    logger.info("BRONZE_PARTITIONS dates=%s", len(by_date))
    logger.info("BRONZE_FILES parquet_files=%s", parquet_files)
    logger.info(
        "BRONZE_SIZE csv_bytes=%s parquet_bytes=%s compression_ratio=%.4f",
        csv_bytes,
        parquet_bytes,
        ratio,
    )

    failed = False
    if null_event_time:
        logger.error("Timestamp parse produced null event_time values")
        failed = True
    if "NULL" in by_date:
        logger.error("At least one row has a null event_date")
        failed = True
    outside = [day for day in by_date if day < MONTH_START or day > MONTH_END]
    if outside:
        logger.error("Dates outside October 2019: %s", outside)
        failed = True
    known = {"view", "cart", "purchase"}
    unexpected = sorted(set(by_type) - known)
    if unexpected:
        # Bronze keeps unknown types. Silver or analysis can decide later.
        logger.warning("Unexpected event_type values kept as-is: %s", unexpected)
    if failed:
        sys.exit(1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the bronze CLI: CSV path, output path, driver memory, and master."""
    parser = argparse.ArgumentParser(description="Write the bronze events table.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--driver-memory",
        default=os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY),
        help="Spark driver heap. Capped at 12g. Default: 12g.",
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER", DEFAULT_MASTER),
        help="Spark master URL. Default: local[6].",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Parse the CSV, write date-partitioned Parquet, and validate the row count."""
    args = parse_args(argv)
    source = args.input.resolve()
    output = args.output.resolve()
    assert_safe_output(output)
    if not source.is_file():
        logger.error("Input CSV not found: %s", source)
        sys.exit(1)

    csv_bytes = source.stat().st_size
    logger.info("Input %s bytes=%s", source, csv_bytes)
    logger.info("Output %s", output)
    started = time.perf_counter()
    spark = build_spark(args.driver_memory, args.master)
    try:
        events = with_event_date(read_events(spark, source))
        write_events(events, output)
        validate(spark, output, csv_bytes)
    finally:
        spark.stop()
    elapsed = time.perf_counter() - started
    logger.info("BRONZE_ELAPSED seconds=%.1f", elapsed)
    logger.info("Bronze ingest passed")


if __name__ == "__main__":
    main()
