"""Clean bronze events into the silver table.

Rules are applied in this order. The order is part of the result:

1. Lowercase and trim ``brand`` before the duplicate check, so ``Nike`` and
   ``nike`` collapse into one key.
2. Drop exact copies on the nine source columns. ``event_date`` is not part
   of the key because it is derived from ``event_time``.
3. Drop a row with a null ``user_id`` or a null or blank ``user_session``.
   A fill of ``unknown`` would glue unrelated events into one session.
4. Flag ``price <= 0`` and keep the row. Phase 3 found zeros only on views
   and carts, so deleting them would shrink the funnel. The measured mix is
   written into ``docs/cleaning_log.md`` by this run, not copied from the profile.
5. Fill a null or blank brand with ``unknown``. Do not drop the event.
6. Fill a null or blank ``category_code`` the same way.
7. Split ``category_code`` on ``.`` into ``category_l1``, ``category_l2``,
   and ``category_l3``. A code with four or more pieces keeps the tail in
   level 3 (``music_tools.piano``), so the leaf is not discarded. A missing
   level is ``unknown``, not null.

Each rule is its own function. Counts in the cleaning log are actions from
this run. Re-runs overwrite ``data/silver/events``. ``data/raw`` is not touched.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BRONZE = PROJECT_ROOT / "data" / "bronze" / "events"
DEFAULT_SILVER = PROJECT_ROOT / "data" / "silver" / "events"
DEFAULT_LOG = PROJECT_ROOT / "docs" / "cleaning_log.md"
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
MAX_DRIVER_BYTES = 12 * 1024**3
DEFAULT_DRIVER_MEMORY = "12g"
DEFAULT_MASTER = "local[6]"
UNKNOWN = "unknown"

# Same nine columns the profile used. Brand is already standardised when
# this key is applied, which is deliberate.
SOURCE_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]
SILVER_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "category_l1",
    "category_l2",
    "category_l3",
    "brand",
    "price",
    "is_non_positive_price",
    "user_id",
    "user_session",
    "event_date",
]
# The published four-level code. Validation checks the leaf survived.
PIANO = "electronics.audio.music_tools.piano"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("clean_silver")


@dataclass
class RuleLog:
    step: int
    name: str
    rows_in: int
    rows_out: int
    removed: int
    affected: int
    reason: str


def memory_to_bytes(value: str) -> int:
    """Parse a Spark size such as ``12g`` or ``512m``."""
    text = value.strip().lower()
    if text.endswith("g"):
        return int(float(text[:-1]) * 1024**3)
    if text.endswith("m"):
        return int(float(text[:-1]) * 1024**2)
    raise ValueError(f"Use a Spark size like 12g or 512m, got {value!r}")


def assert_safe_output(output: Path) -> None:
    """Silver output stays under ``data/silver`` and never under ``data/raw``."""
    resolved = output.resolve()
    raw = RAW_ROOT.resolve()
    silver_root = (PROJECT_ROOT / "data" / "silver").resolve()
    if resolved == raw or raw in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, raw)
        sys.exit(1)
    if resolved != silver_root and silver_root not in resolved.parents:
        logger.error("Silver output must be under %s, got %s", silver_root, resolved)
        sys.exit(1)


def assert_safe_doc(path: Path) -> None:
    """The cleaning log is documentation. It must not land inside ``data/``."""
    resolved = path.resolve()
    data_root = (PROJECT_ROOT / "data").resolve()
    if resolved == data_root or data_root in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, data_root)
        sys.exit(1)


def build_spark(driver_memory: str, master: str) -> SparkSession:
    """Local session. ``-Xmx`` is passed before the JVM starts."""
    requested = memory_to_bytes(driver_memory)
    if requested > MAX_DRIVER_BYTES:
        logger.error("Driver memory %s exceeds the 12g cap", driver_memory)
        sys.exit(1)
    if not os.environ.get("JAVA_HOME"):
        logger.error("JAVA_HOME is unset. Point it at JDK 17 before running.")
        sys.exit(1)
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {driver_memory} "
        "--driver-java-options -Dderby.system.home=/tmp/rees46-derby "
        "pyspark-shell"
    )
    spark = (
        SparkSession.builder.appName("silver-clean")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(PROJECT_ROOT / "spark-warehouse"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    max_heap = spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
    logger.info("Spark version %s master %s", spark.version, master)
    logger.info("JVM maxMemory=%s bytes (requested %s)", max_heap, driver_memory)
    if max_heap < int(requested * 0.6):
        logger.error("JVM heap is smaller than requested. maxMemory=%s", max_heap)
        spark.stop()
        sys.exit(1)
    return spark


def is_blank(column: str) -> Column:
    """Null or whitespace-only. Used for brand, category, and session."""
    value = F.col(column)
    return value.isNull() | (F.trim(value) == "")


def null_user_key() -> Column:
    """A row we cannot put on the user grain or the session grain."""
    return F.col("user_id").isNull() | is_blank("user_session")


def standardise_brand(events: DataFrame) -> DataFrame:
    """Lowercase and trim brand. Null stays null for the fill rule."""
    return events.withColumn("brand", F.lower(F.trim(F.col("brand"))))


def drop_exact_duplicates(events: DataFrame) -> DataFrame:
    """Keep one row per identical source event."""
    return events.dropDuplicates(SOURCE_COLUMNS)


def drop_null_user_keys(events: DataFrame) -> DataFrame:
    """Drop rows that have no user_id or no usable session id."""
    return events.filter(~null_user_key())


def flag_non_positive_price(events: DataFrame) -> DataFrame:
    """Mark price <= 0. Keep the row and keep the original price."""
    flagged = F.when(
        F.col("price").isNotNull() & (F.col("price") <= 0),
        F.lit(True),
    ).otherwise(F.lit(False))
    return events.withColumn("is_non_positive_price", flagged)


def fill_unknown_brand(events: DataFrame) -> DataFrame:
    """Replace a missing brand with the literal unknown. Do not drop the row."""
    return events.withColumn(
        "brand",
        F.when(is_blank("brand"), F.lit(UNKNOWN)).otherwise(F.col("brand")),
    )


def fill_unknown_category_code(events: DataFrame) -> DataFrame:
    """Replace a missing category_code with the literal unknown."""
    return events.withColumn(
        "category_code",
        F.when(is_blank("category_code"), F.lit(UNKNOWN)).otherwise(F.col("category_code")),
    )


def add_category_levels(events: DataFrame) -> DataFrame:
    """Split category_code into three levels.

    Level 3 is the third token when the code has three pieces. When the code
    has four or more, level 3 is the remainder joined with dots. That keeps
    ``piano`` on ``electronics.audio.music_tools.piano`` instead of dropping
    the leaf. Shorter codes get ``unknown`` for the levels they do not have.
    """
    parts = F.split(F.col("category_code"), r"\.")
    depth = F.size(parts)
    # getItem is 0-based. slice is 1-based, so slice(..., 3, 20) starts at
    # the third token and keeps the tail.
    level_1 = F.when(depth >= 1, parts.getItem(0)).otherwise(F.lit(UNKNOWN))
    level_2 = F.when(depth >= 2, parts.getItem(1)).otherwise(F.lit(UNKNOWN))
    level_3 = (
        F.when(depth > 3, F.concat_ws(".", F.slice(parts, 3, 20)))
        .when(depth == 3, parts.getItem(2))
        .otherwise(F.lit(UNKNOWN))
    )
    return (
        events.withColumn("category_l1", level_1)
        .withColumn("category_l2", level_2)
        .withColumn("category_l3", level_3)
    )


def select_silver(events: DataFrame) -> DataFrame:
    """Fixed column order. Partition column stays in the frame."""
    return events.select(*SILVER_COLUMNS)


def apply_repairs(events: DataFrame) -> DataFrame:
    """Flag, fill, and split. Row count does not change here."""
    repaired = flag_non_positive_price(events)
    repaired = fill_unknown_brand(repaired)
    repaired = fill_unknown_category_code(repaired)
    repaired = add_category_levels(repaired)
    return select_silver(repaired)


def _as_int(value: object) -> int:
    if value is None:
        return 0
    return int(value)


def count_rows(events: DataFrame) -> int:
    return _as_int(events.count())


def count_brand_dirty(events: DataFrame) -> int:
    """Rows whose brand text would change. Null brands are not dirty."""
    brand = F.col("brand")
    return count_rows(
        events.filter(brand.isNotNull() & (brand != F.lower(F.trim(brand))))
    )


def count_null_user_keys(events: DataFrame) -> int:
    return count_rows(events.filter(null_user_key()))


def quality_counts(events: DataFrame) -> dict:
    """One pass over the rows that will be kept, before fills and the flag."""
    category = F.col("category_code")
    depth = F.size(F.split(category, r"\."))
    missing_category = is_blank("category_code")
    non_positive = F.col("price").isNotNull() & (F.col("price") <= 0)
    row = events.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(is_blank("brand"), 1).otherwise(0)).alias("blank_brand"),
        F.sum(F.when(F.col("brand") == UNKNOWN, 1).otherwise(0)).alias("brand_already_unknown"),
        F.sum(F.when(missing_category, 1).otherwise(0)).alias("blank_category"),
        F.sum(F.when(category == UNKNOWN, 1).otherwise(0)).alias("category_already_unknown"),
        F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_price"),
        F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("negative_price"),
        F.sum(F.when(F.col("price") == 0, 1).otherwise(0)).alias("zero_price"),
        F.sum(F.when(non_positive & (F.col("event_type") == "view"), 1).otherwise(0)).alias(
            "non_positive_view"
        ),
        F.sum(F.when(non_positive & (F.col("event_type") == "cart"), 1).otherwise(0)).alias(
            "non_positive_cart"
        ),
        F.sum(
            F.when(non_positive & (F.col("event_type") == "purchase"), 1).otherwise(0)
        ).alias("non_positive_purchase"),
        F.sum(
            F.when(
                non_positive & ~F.col("event_type").isin("view", "cart", "purchase"),
                1,
            ).otherwise(0)
        ).alias("non_positive_other"),
        F.sum(F.when(missing_category, 1).otherwise(0)).alias("depth_missing"),
        F.sum(F.when(~missing_category & (depth == 1), 1).otherwise(0)).alias("depth_1"),
        F.sum(F.when(~missing_category & (depth == 2), 1).otherwise(0)).alias("depth_2"),
        F.sum(F.when(~missing_category & (depth == 3), 1).otherwise(0)).alias("depth_3"),
        F.sum(F.when(~missing_category & (depth >= 4), 1).otherwise(0)).alias("depth_4_plus"),
        F.max(F.when(~missing_category, depth)).alias("max_depth"),
        F.sum(
            F.when(
                F.col("brand").isNotNull() & (F.col("brand") != F.lower(F.trim(F.col("brand")))),
                1,
            ).otherwise(0)
        ).alias("brand_still_dirty"),
    ).collect()[0]
    return {key: _as_int(row[key]) for key in row.asDict()}


def write_silver(events: DataFrame, output: Path) -> None:
    """One file per day. The duplicate drop already shuffled the rows.

    Writing that shuffle straight into ``partitionBy`` would create one file
    per shuffle task per day. ``repartition(31, event_date)`` puts each UTC
    day in one task first. October has 31 days, so the hash lands on one
    file per day even when two days share a task.
    """
    (
        events.repartition(31, "event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")
        .parquet(str(output))
    )


def directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def validate_silver(spark: SparkSession, output: Path, surviving_rows: int) -> dict:
    """Read the Parquet back. Do not trust the write lineage alone."""
    stored = spark.read.parquet(str(output))
    parts = F.split(F.col("category_code"), r"\.")
    depth = F.size(parts)
    non_positive = F.col("price").isNotNull() & (F.col("price") <= 0)
    row = stored.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("brand").isNull(), 1).otherwise(0)).alias("null_brand"),
        F.sum(F.when(F.col("category_code").isNull(), 1).otherwise(0)).alias("null_category"),
        F.sum(F.when(F.col("category_l1").isNull(), 1).otherwise(0)).alias("null_l1"),
        F.sum(F.when(F.col("category_l2").isNull(), 1).otherwise(0)).alias("null_l2"),
        F.sum(F.when(F.col("category_l3").isNull(), 1).otherwise(0)).alias("null_l3"),
        F.sum(F.when(F.col("user_id").isNull(), 1).otherwise(0)).alias("null_user"),
        F.sum(F.when(is_blank("user_session"), 1).otherwise(0)).alias("blank_session"),
        F.sum(F.when(F.col("is_non_positive_price").isNull(), 1).otherwise(0)).alias(
            "null_flag"
        ),
        F.sum(
            F.when(F.col("is_non_positive_price") != non_positive, 1).otherwise(0)
        ).alias("flag_mismatch"),
        F.sum(
            F.when(
                F.col("brand").isNotNull()
                & (F.col("brand") != F.lower(F.trim(F.col("brand")))),
                1,
            ).otherwise(0)
        ).alias("dirty_brand"),
        F.sum(F.when(F.col("is_non_positive_price"), 1).otherwise(0)).alias("flagged"),
        F.countDistinct("event_date").alias("dates"),
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date"),
        F.sum(
            F.when((depth == 2) & (F.col("category_l3") != UNKNOWN), 1).otherwise(0)
        ).alias("bad_depth_2"),
        F.sum(
            F.when(
                (depth > 3)
                & (F.col("category_l3") != F.concat_ws(".", F.slice(parts, 3, 20))),
                1,
            ).otherwise(0)
        ).alias("bad_tail"),
        F.sum(F.when(F.col("category_code") == PIANO, 1).otherwise(0)).alias("piano_rows"),
        F.sum(
            F.when(
                (F.col("category_code") == PIANO)
                & (F.col("category_l1") == "electronics")
                & (F.col("category_l2") == "audio")
                & (F.col("category_l3") == "music_tools.piano"),
                1,
            ).otherwise(0)
        ).alias("piano_ok"),
    ).collect()[0]
    checks = {key: row[key] for key in row.asDict()}
    for key in (
        "rows",
        "null_brand",
        "null_category",
        "null_l1",
        "null_l2",
        "null_l3",
        "null_user",
        "blank_session",
        "null_flag",
        "flag_mismatch",
        "dirty_brand",
        "flagged",
        "dates",
        "bad_depth_2",
        "bad_tail",
        "piano_rows",
        "piano_ok",
    ):
        checks[key] = _as_int(checks[key])
    checks["min_date"] = str(row["min_date"])
    checks["max_date"] = str(row["max_date"])
    checks["parquet_files"] = sum(1 for item in output.rglob("*.parquet") if item.is_file())
    checks["folder_bytes"] = directory_bytes(output)
    checks["duplicate_extra_rows"] = duplicate_extra_rows(spark)

    failures = []
    if checks["rows"] != surviving_rows:
        failures.append(f"rows {checks['rows']} != expected {surviving_rows}")
    for name in (
        "null_brand",
        "null_category",
        "null_l1",
        "null_l2",
        "null_l3",
        "null_user",
        "blank_session",
        "null_flag",
        "flag_mismatch",
        "dirty_brand",
        "bad_depth_2",
        "bad_tail",
    ):
        if checks[name] != 0:
            failures.append(f"{name}={checks[name]}")
    if checks["piano_rows"] != checks["piano_ok"]:
        failures.append(f"piano split {checks['piano_ok']} of {checks['piano_rows']}")
    if checks["dates"] != 31 or checks["min_date"] != "2019-10-01" or checks["max_date"] != "2019-10-31":
        failures.append(
            f"dates {checks['dates']} {checks['min_date']} {checks['max_date']}"
        )
    if checks["duplicate_extra_rows"] != 0:
        failures.append(f"duplicate extra rows {checks['duplicate_extra_rows']}")
    if failures:
        logger.error("Silver validation failed: %s", "; ".join(failures))
        sys.exit(1)
    return checks


def duplicate_extra_rows(spark: SparkSession) -> int:
    """Extra copies still present on the nine silver source columns.

    This is the same ``GROUP BY ... HAVING COUNT(*) > 1`` shape as the
    profile. It runs after the fills, so a non-zero result means the fill
    created a new collision or the drop missed a copy.
    """
    column_list = ", ".join(SOURCE_COLUMNS)
    row = spark.sql(
        f"""
        SELECT COALESCE(SUM(c - 1), 0) AS extra_rows
        FROM (
            SELECT COUNT(*) AS c
            FROM silver_events
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
        )
        """
    ).collect()[0]
    return _as_int(row["extra_rows"])


def fmt_int(value: int) -> str:
    return f"{int(value):,}"


def price_reason(quality: dict) -> str:
    """Decision text. The counts are from this run's surviving rows."""
    return (
        "Flagged with is_non_positive_price and kept. "
        f"This run has {fmt_int(quality['negative_price'])} negative prices and "
        f"{fmt_int(quality['zero_price'])} zero prices "
        f"(view {fmt_int(quality['non_positive_view'])}, "
        f"cart {fmt_int(quality['non_positive_cart'])}, "
        f"purchase {fmt_int(quality['non_positive_purchase'])}, "
        f"other {fmt_int(quality['non_positive_other'])}). "
        "Dropping those rows would remove views and carts from the funnel. "
        "Revenue queries can ignore the flag. The original price is unchanged."
    )


def build_rule_logs(
    bronze_rows: int,
    brand_dirty: int,
    deduped_rows: int,
    null_keys: int,
    quality: dict,
) -> list[RuleLog]:
    """Assemble the log from counts already taken. No extra scans."""
    surviving = quality["rows"]
    non_positive = (
        quality["non_positive_view"]
        + quality["non_positive_cart"]
        + quality["non_positive_purchase"]
        + quality["non_positive_other"]
    )
    # A naive three-token split is wrong for every code that is not depth 3.
    nontrivial_split = (
        quality["depth_missing"]
        + quality["depth_1"]
        + quality["depth_2"]
        + quality["depth_4_plus"]
    )
    return [
        RuleLog(
            1,
            "standardise_brand",
            bronze_rows,
            bronze_rows,
            0,
            brand_dirty,
            "Brand is a grouping key. Lowercase and trim make case and whitespace "
            "variants one value. Null stays null so the fill rule can see it. "
            "The rule runs even when the affected count is 0, so a later month "
            "with mixed case takes the same path. It runs before the duplicate "
            "drop so case variants are compared as one key.",
        ),
        RuleLog(
            2,
            "drop_exact_duplicates",
            bronze_rows,
            deduped_rows,
            bronze_rows - deduped_rows,
            bronze_rows - deduped_rows,
            "A second row that matches on event_time, event_type, product_id, "
            "category_id, category_code, brand, price, user_id, and user_session "
            "is a copy of the same event, not a new action. dropDuplicates keeps "
            "one. The copies are identical, so which copy survives does not matter. "
            "event_date is left out of the key because it is derived from event_time.",
        ),
        RuleLog(
            3,
            "drop_null_user_keys",
            deduped_rows,
            surviving,
            null_keys,
            null_keys,
            "user_id and user_session are the keys for the user and session grains. "
            "Filling a null session with unknown would merge unrelated events into "
            "one session. A null user_id or a null or blank user_session is dropped. "
            "Session ids that are present are not rewritten.",
        ),
        RuleLog(
            4,
            "flag_non_positive_price",
            surviving,
            surviving,
            0,
            non_positive,
            price_reason(quality),
        ),
        RuleLog(
            5,
            "fill_unknown_brand",
            surviving,
            surviving,
            0,
            quality["blank_brand"],
            "A missing brand is unknown metadata, not a reason to drop the event. "
            f"The fill is the literal {UNKNOWN}, after standardisation, so it is "
            "already lowercase. "
            f"Rows whose brand was already {UNKNOWN} before the fill: "
            f"{fmt_int(quality['brand_already_unknown'])}.",
        ),
        RuleLog(
            6,
            "fill_unknown_category_code",
            surviving,
            surviving,
            0,
            quality["blank_category"],
            "A missing category_code is unknown metadata, same as brand. The row "
            f"stays. The fill is the literal {UNKNOWN}. "
            f"Rows whose category_code was already {UNKNOWN} before the fill: "
            f"{fmt_int(quality['category_already_unknown'])}.",
        ),
        RuleLog(
            7,
            "add_category_levels",
            surviving,
            surviving,
            0,
            nontrivial_split,
            "Level 1 is the first dot-separated piece. Level 2 is the second piece, "
            "or unknown when the code is shorter. Level 3 is the third piece when "
            "there are exactly three, the third piece onward joined with dots when "
            "there are four or more, and unknown when the code has fewer than three "
            "pieces. Joining the tail keeps a leaf such as piano on "
            "electronics.audio.music_tools.piano. Missing levels are the literal "
            "unknown, not null, so a group-by never sees a null level. Affected "
            "counts codes that are not already exactly three pieces, which is where "
            "a blind split would be wrong.",
        ),
    ]


def render_log(
    rules: list[RuleLog],
    quality: dict,
    checks: dict,
    bronze_rows: int,
    spark_version: str,
    elapsed_seconds: float,
) -> str:
    """Build the cleaning log from measured values only."""
    lines = [
        "# Silver cleaning log",
        "",
        f"Source: `data/bronze/events`. Output: `data/silver/events`, partitioned by `event_date`.",
        f"Run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} "
        f"on Spark {spark_version}. Elapsed {elapsed_seconds:.1f} seconds.",
        "Removed is rows deleted by the rule. Affected is rows the rule rewrote or flagged.",
        "For the two drop rules, affected equals removed. Counts are from this run.",
        "",
        "## Rules",
        "",
        "| Step | Rule | Rows in | Rows out | Removed | Affected |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for rule in rules:
        lines.append(
            f"| {rule.step} | {rule.name} | {fmt_int(rule.rows_in)} | {fmt_int(rule.rows_out)} | "
            f"{fmt_int(rule.removed)} | {fmt_int(rule.affected)} |"
        )
    lines.append("")
    lines.append(
        f"Bronze rows {fmt_int(bronze_rows)}. Silver rows {fmt_int(checks['rows'])}. "
        f"Removed {fmt_int(bronze_rows - checks['rows'])}."
    )
    lines.append("")
    for rule in rules:
        lines.append(f"### {rule.step}. {rule.name}")
        lines.append("")
        lines.append(rule.reason)
        lines.append("")
    lines.extend(
        [
            "## Price <= 0 by event_type",
            "",
            "Counted on the rows that survived the duplicate drop and the key drop, before the flag was written.",
            "",
            "| event_type | Rows with price <= 0 |",
            "| --- | --- |",
            f"| view | {fmt_int(quality['non_positive_view'])} |",
            f"| cart | {fmt_int(quality['non_positive_cart'])} |",
            f"| purchase | {fmt_int(quality['non_positive_purchase'])} |",
            f"| other | {fmt_int(quality['non_positive_other'])} |",
            f"| price < 0 | {fmt_int(quality['negative_price'])} |",
            f"| price = 0 | {fmt_int(quality['zero_price'])} |",
            f"| null price | {fmt_int(quality['null_price'])} |",
            "",
            "These rows stay in silver. `is_non_positive_price` is true exactly when price is not null and price <= 0.",
            "",
            "## Category depth before the fill",
            "",
            "Depth is the number of pieces after splitting the original category_code on `.`. "
            "Missing includes null and blank. The fill turns missing into the literal unknown, which is one piece, so this table is the pre-fill shape.",
            "",
            "| Depth | Rows | How level 3 is set |",
            "| --- | --- | --- |",
            f"| missing | {fmt_int(quality['depth_missing'])} | category_l1, category_l2, and category_l3 are unknown |",
            f"| 1 | {fmt_int(quality['depth_1'])} | category_l2 and category_l3 are unknown |",
            f"| 2 | {fmt_int(quality['depth_2'])} | category_l3 is unknown |",
            f"| 3 | {fmt_int(quality['depth_3'])} | category_l3 is the third piece |",
            f"| 4 or more | {fmt_int(quality['depth_4_plus'])} | category_l3 is the tail joined with dots |",
            "",
            f"Maximum depth of a non-missing code: {fmt_int(quality['max_depth'])}.",
            "",
            "## Checks after write",
            "",
            "| Check | Value |",
            "| --- | --- |",
            f"| Silver rows | {fmt_int(checks['rows'])} |",
            f"| Null brand, category_code, category levels, user_id | 0 |",
            f"| Blank user_session | {fmt_int(checks['blank_session'])} |",
            f"| Flag mismatches | {fmt_int(checks['flag_mismatch'])} |",
            f"| Flagged rows | {fmt_int(checks['flagged'])} |",
            f"| Brand values still needing lower/trim | {fmt_int(checks['dirty_brand'])} |",
            f"| Depth-2 rows whose category_l3 is not unknown | {fmt_int(checks['bad_depth_2'])} |",
            f"| Deep codes whose category_l3 is not the joined tail | {fmt_int(checks['bad_tail'])} |",
            f"| `{PIANO}` rows | {fmt_int(checks['piano_rows'])} |",
            f"| Those rows with levels electronics / audio / music_tools.piano | {fmt_int(checks['piano_ok'])} |",
            f"| Distinct event_date | {fmt_int(checks['dates'])} |",
            f"| First event_date | {checks['min_date']} |",
            f"| Last event_date | {checks['max_date']} |",
            f"| Parquet files | {fmt_int(checks['parquet_files'])} |",
            f"| Silver folder bytes | {fmt_int(checks['folder_bytes'])} |",
            f"| Extra duplicate rows after clean | {fmt_int(checks['duplicate_extra_rows'])} |",
            "",
            "## Left unchanged",
            "",
            "Phase 3 measured 348 sessions tied to more than one user_id "
            "(`docs/data_profile.md`). Silver does not pick a winning user_id. "
            "The profile does not say which id is the real one.",
            "",
        ]
    )
    return "\n".join(lines)


def run_self_test(spark: SparkSession) -> None:
    """Tiny frame for the level split, the flag, and the duplicate order.

    This does not read bronze. It checks the rule functions before the
    full-month shuffle.
    """
    rows = [
        ("t", "view", 1, 1, "electronics.smartphone", " Nike ", 10.0, 1, "s-case", "2019-10-01"),
        ("t", "view", 1, 1, "electronics.smartphone", "nike", 10.0, 1, "s-case", "2019-10-01"),
        ("t", "view", 2, 1, "electronics.video.tv", "sony", 0.0, 2, "s-zero", "2019-10-02"),
        ("t", "view", 2, 1, "electronics.video.tv", "sony", 0.0, 2, "s-zero", "2019-10-02"),
        ("t", "cart", 3, 1, PIANO, "yamaha", -1.0, 3, "s-piano", "2019-10-03"),
        ("t", "view", 4, 1, None, None, 5.0, 4, "s-missing", "2019-10-04"),
        ("t", "view", 5, 1, "appliances", "  ", 5.0, 5, "s-blank-brand", "2019-10-05"),
        ("t", "view", 6, 1, "a.b.c.d.e", "lg", 5.0, 6, "s-five", "2019-10-06"),
        ("t", "view", 7, 1, "electronics.smartphone", "apple", 5.0, 7, None, "2019-10-07"),
        ("t", "view", 8, 1, "electronics.smartphone", "apple", 5.0, None, "s-no-user", "2019-10-08"),
    ]
    frame = spark.createDataFrame(
        rows,
        [
            "event_time",
            "event_type",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_id",
            "user_session",
            "event_date",
        ],
    )
    cleaned = apply_repairs(drop_null_user_keys(drop_exact_duplicates(standardise_brand(frame))))
    got = {
        row["user_session"]: row.asDict()
        for row in cleaned.collect()
    }
    expected_sessions = {"s-case", "s-zero", "s-piano", "s-missing", "s-blank-brand", "s-five"}
    if set(got) != expected_sessions:
        logger.error("Self-test sessions %s", sorted(got))
        sys.exit(1)
    case = got["s-case"]
    if case["brand"] != "nike" or cleaned.filter(F.col("user_session") == "s-case").count() != 1:
        logger.error("Self-test brand collapse failed: %s", case)
        sys.exit(1)
    zero = got["s-zero"]
    if zero["is_non_positive_price"] is not True or zero["price"] != 0:
        logger.error("Self-test zero price failed: %s", zero)
        sys.exit(1)
    piano = got["s-piano"]
    if (
        piano["is_non_positive_price"] is not True
        or piano["category_l1"] != "electronics"
        or piano["category_l2"] != "audio"
        or piano["category_l3"] != "music_tools.piano"
    ):
        logger.error("Self-test piano split failed: %s", piano)
        sys.exit(1)
    missing = got["s-missing"]
    if (
        missing["brand"] != UNKNOWN
        or missing["category_code"] != UNKNOWN
        or missing["category_l1"] != UNKNOWN
        or missing["category_l2"] != UNKNOWN
        or missing["category_l3"] != UNKNOWN
    ):
        logger.error("Self-test missing fill failed: %s", missing)
        sys.exit(1)
    blank = got["s-blank-brand"]
    if blank["brand"] != UNKNOWN or blank["category_l1"] != "appliances" or blank["category_l3"] != UNKNOWN:
        logger.error("Self-test blank brand or depth-1 split failed: %s", blank)
        sys.exit(1)
    five = got["s-five"]
    if five["category_l1"] != "a" or five["category_l2"] != "b" or five["category_l3"] != "c.d.e":
        logger.error("Self-test five-level split failed: %s", five)
        sys.exit(1)
    phone = got["s-case"]
    if phone["category_l1"] != "electronics" or phone["category_l2"] != "smartphone" or phone["category_l3"] != UNKNOWN:
        logger.error("Self-test depth-2 split failed: %s", phone)
        sys.exit(1)
    logger.info("Self-test passed")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean bronze events into silver.")
    parser.add_argument("--bronze", type=Path, default=DEFAULT_BRONZE)
    parser.add_argument("--output", type=Path, default=DEFAULT_SILVER)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument(
        "--driver-memory",
        default=os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY),
    )
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", DEFAULT_MASTER))
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the rule checks on a tiny frame and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    output = args.output.resolve()
    log_path = args.log.resolve()
    assert_safe_output(output)
    assert_safe_doc(log_path)
    started = time.perf_counter()
    spark = build_spark(args.driver_memory, args.master)
    try:
        if args.self_test:
            run_self_test(spark)
            return
        bronze_path = args.bronze.resolve()
        if not bronze_path.exists():
            logger.error("Bronze path not found: %s", bronze_path)
            sys.exit(1)

        bronze = spark.read.parquet(str(bronze_path)).persist(StorageLevel.MEMORY_AND_DISK)
        bronze_rows = count_rows(bronze)
        logger.info("SILVER_BRONZE_ROWS rows=%s", bronze_rows)
        brand_dirty = count_brand_dirty(bronze)
        logger.info("SILVER_BRAND_DIRTY rows=%s", brand_dirty)

        standardised = standardise_brand(bronze)
        deduped = drop_exact_duplicates(standardised).persist(StorageLevel.MEMORY_AND_DISK)
        deduped_rows = count_rows(deduped)
        bronze.unpersist()
        logger.info(
            "SILVER_DEDUPED rows=%s removed=%s",
            deduped_rows,
            bronze_rows - deduped_rows,
        )

        null_keys = count_null_user_keys(deduped)
        surviving = drop_null_user_keys(deduped).persist(StorageLevel.MEMORY_AND_DISK)
        surviving_rows = count_rows(surviving)
        deduped.unpersist()
        if surviving_rows != deduped_rows - null_keys:
            logger.error(
                "Null-key drop removed %s but the count said %s",
                deduped_rows - surviving_rows,
                null_keys,
            )
            sys.exit(1)
        logger.info("SILVER_NULL_KEYS removed=%s rows_out=%s", null_keys, surviving_rows)

        quality = quality_counts(surviving)
        logger.info("SILVER_QUALITY %s", quality)
        if quality["rows"] != surviving_rows:
            logger.error("Quality pass saw %s rows, expected %s", quality["rows"], surviving_rows)
            sys.exit(1)
        if quality["brand_still_dirty"] != 0:
            logger.error("Brand standardise left %s dirty rows", quality["brand_still_dirty"])
            sys.exit(1)
        non_positive = (
            quality["non_positive_view"]
            + quality["non_positive_cart"]
            + quality["non_positive_purchase"]
            + quality["non_positive_other"]
        )
        if non_positive != quality["negative_price"] + quality["zero_price"]:
            logger.error("Non-positive breakdown does not add up")
            sys.exit(1)

        silver = apply_repairs(surviving)
        write_silver(silver, output)
        surviving.unpersist()

        spark.read.parquet(str(output)).createOrReplaceTempView("silver_events")
        checks = validate_silver(spark, output, surviving_rows)
        logger.info(
            "SILVER_VALIDATED rows=%s flagged=%s files=%s folder_bytes=%s duplicate_extra=%s",
            checks["rows"],
            checks["flagged"],
            checks["parquet_files"],
            checks["folder_bytes"],
            checks["duplicate_extra_rows"],
        )
        rules = build_rule_logs(bronze_rows, brand_dirty, deduped_rows, null_keys, quality)
        elapsed = time.perf_counter() - started
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            render_log(rules, quality, checks, bronze_rows, spark.version, elapsed)
        )
        logger.info("Wrote %s", log_path)
        for rule in rules:
            logger.info(
                "SILVER_RULE step=%s name=%s rows_in=%s rows_out=%s removed=%s affected=%s",
                rule.step,
                rule.name,
                rule.rows_in,
                rule.rows_out,
                rule.removed,
                rule.affected,
            )
    finally:
        spark.stop()
    if not args.self_test:
        logger.info("SILVER_ELAPSED seconds=%.1f", time.perf_counter() - started)
        logger.info("Silver clean passed")


if __name__ == "__main__":
    main()
