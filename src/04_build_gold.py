"""Build the gold star schema from silver events.

Grain choices, measured on silver before this model was frozen:

- ``product_id`` has one category tuple. Category lives on ``dim_product``
  only. The job stops if a product ever has two categories.
- Brand does not. Thousands of products carry more than one brand, so
  ``fact_events.brand`` is the brand on that event and ``dim_product.brand``
  is only the most common brand.
- A session can contain more than one ``user_id``. ``sessions.user_id`` is
  the user with the most events in that session. Ties go to the smaller
  ``user_id``. The fact keeps every event's own ``user_id``.
- A session can cross UTC midnight, so ``sessions`` is not partitioned by date.

Revenue is the sum of ``price`` on purchase events whose
``is_non_positive_price`` flag is false. Views and carts are not revenue.
A session with no such purchase has revenue 0, not null.

Re-runs overwrite ``data/gold``. ``data/raw`` and silver are only read.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SILVER = PROJECT_ROOT / "data" / "silver" / "events"
DEFAULT_GOLD = PROJECT_ROOT / "data" / "gold"
DEFAULT_DOC = PROJECT_ROOT / "docs" / "data_model.md"
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
MAX_DRIVER_BYTES = 12 * 1024**3
DEFAULT_DRIVER_MEMORY = "12g"
DEFAULT_MASTER = "local[6]"

DAY_NAMES = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("build_gold")


def memory_to_bytes(value: str) -> int:
    """Parse a Spark size such as ``12g`` or ``512m``."""
    text = value.strip().lower()
    if text.endswith("g"):
        return int(float(text[:-1]) * 1024**3)
    if text.endswith("m"):
        return int(float(text[:-1]) * 1024**2)
    raise ValueError(f"Use a Spark size like 12g or 512m, got {value!r}")


def assert_under_gold(path: Path) -> None:
    """Gold tables stay under ``data/gold`` and never under ``data/raw``."""
    resolved = path.resolve()
    raw = RAW_ROOT.resolve()
    gold = DEFAULT_GOLD.resolve()
    if resolved == raw or raw in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, raw)
        sys.exit(1)
    if resolved != gold and gold not in resolved.parents:
        logger.error("Gold output must be under %s, got %s", gold, resolved)
        sys.exit(1)


def assert_safe_doc(path: Path) -> None:
    """Refuse to write the model document under data/."""
    resolved = path.resolve()
    data_root = (PROJECT_ROOT / "data").resolve()
    if resolved == data_root or data_root in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, data_root)
        sys.exit(1)


def build_spark(driver_memory: str, master: str) -> SparkSession:
    """Local session. ``-Xmx`` is set before the JVM starts."""
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
        SparkSession.builder.appName("gold-star")
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


def _as_int(value: object) -> int:
    """Cast a Spark aggregate to int."""
    if value is None:
        return 0
    return int(value)


def _as_float(value: object) -> float:
    """Cast a Spark aggregate to float."""
    if value is None:
        return 0.0
    return float(value)


def purchase_revenue() -> F.Column:
    """Price of a purchase that the silver flag did not mark non-positive."""
    return F.when(
        (F.col("event_type") == "purchase") & (~F.col("is_non_positive_price")),
        F.col("price"),
    ).otherwise(F.lit(0.0))


def product_variants(events: DataFrame) -> DataFrame:
    """One row per product: how many brands and category tuples it has."""
    category_key = F.concat_ws(
        "|",
        F.col("category_id").cast("string"),
        F.col("category_code"),
        F.col("category_l1"),
        F.col("category_l2"),
        F.col("category_l3"),
    )
    return events.groupBy("product_id").agg(
        F.count(F.lit(1)).alias("n_events"),
        F.countDistinct("brand").alias("n_brands"),
        F.countDistinct(category_key).alias("n_category_tuples"),
    )


def variant_summary(variants: DataFrame) -> dict:
    """Scalars that decide whether category may live only on the product dim."""
    row = variants.agg(
        F.count(F.lit(1)).alias("products"),
        F.sum(F.when(F.col("n_brands") > 1, 1).otherwise(0)).alias("multi_brand_products"),
        F.sum(F.when(F.col("n_brands") > 1, F.col("n_events")).otherwise(0)).alias(
            "multi_brand_events"
        ),
        F.sum(F.when(F.col("n_category_tuples") > 1, 1).otherwise(0)).alias(
            "multi_category_products"
        ),
    ).collect()[0]
    return {key: _as_int(row[key]) for key in row.asDict()}


def build_dim_product(events: DataFrame, variants: DataFrame) -> DataFrame:
    """One row per product_id.

    Category columns are copied from the product's only category tuple.
    Brand is the most frequent brand. Equal counts break toward the
    lexicographically smaller brand so a re-run picks the same row.
    """
    categories = events.select(
        "product_id",
        "category_id",
        "category_code",
        "category_l1",
        "category_l2",
        "category_l3",
    ).dropDuplicates(["product_id"])
    brand_counts = events.groupBy("product_id", "brand").agg(
        F.count(F.lit(1)).alias("brand_events")
    )
    ranked = brand_counts.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("product_id").orderBy(
                F.desc("brand_events"),
                F.asc("brand"),
            )
        ),
    )
    modal = ranked.filter(F.col("rn") == 1).select(
        "product_id",
        F.col("brand").alias("brand"),
        "brand_events",
    )
    return (
        categories.join(modal, "product_id", "inner")
        .join(variants, "product_id", "inner")
        .withColumn("has_brand_conflict", F.col("n_brands") > 1)
        .select(
            "product_id",
            "category_id",
            "category_code",
            "category_l1",
            "category_l2",
            "category_l3",
            "brand",
            "brand_events",
            "n_events",
            "n_brands",
            "has_brand_conflict",
        )
    )


def build_dim_user(events: DataFrame) -> DataFrame:
    """One row per user_id. ``n_sessions`` counts sessions the user appears in.

    A session attributed to someone else in ``sessions`` can still increment
    this count. The event really did have this user_id.
    """
    return events.groupBy("user_id").agg(
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.count(F.lit(1)).alias("total_events"),
        F.countDistinct("user_session").alias("n_sessions"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("n_purchases"),
    )


def build_dim_date(events: DataFrame) -> DataFrame:
    """One row per UTC event_date present in silver.

    ``weekday`` is 0 for Monday through 6 for Sunday. Stored ``day_of_week``
    shifts that to 1..7 so Monday is 1. Weekend is Saturday and Sunday.
    ``week`` is Spark ``weekofyear``: Monday-start, ISO-style week number.
    Days with no events are not invented.
    """
    day_of_week = F.weekday("event_date") + F.lit(1)
    day_name = F.lit(None).cast("string")
    for number, name in DAY_NAMES.items():
        day_name = F.when(day_of_week == number, F.lit(name)).otherwise(day_name)
    return (
        events.select("event_date")
        .dropDuplicates(["event_date"])
        .withColumn("day_of_week", day_of_week)
        .withColumn("day_name", day_name)
        .withColumn("is_weekend", F.col("day_of_week") >= 6)
        .withColumn("week", F.weekofyear("event_date"))
        .withColumn("year", F.year("event_date"))
        .select("event_date", "day_of_week", "day_name", "is_weekend", "week", "year")
    )


def build_sessions(events: DataFrame) -> DataFrame:
    """One row per user_session.

    ``user_id`` is the user with the most events. Ties use the smaller
    ``user_id``. Counts and revenue include every event in the session,
    including events from a second user when ``has_user_conflict`` is true.
    """
    totals = events.groupBy("user_session").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count(F.lit(1)).alias("n_events"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("n_views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("n_carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
            "n_purchases"
        ),
        F.sum(purchase_revenue()).alias("revenue"),
        F.countDistinct("user_id").alias("n_users"),
    )
    per_user = events.groupBy("user_session", "user_id").agg(
        F.count(F.lit(1)).alias("user_events")
    )
    attributed = (
        per_user.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_session").orderBy(
                    F.desc("user_events"),
                    F.asc("user_id"),
                )
            ),
        )
        .filter(F.col("rn") == 1)
        .select("user_session", "user_id")
    )
    return (
        totals.join(attributed, "user_session", "inner")
        .withColumn(
            "duration_seconds",
            (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).cast("long"),
        )
        .withColumn("has_user_conflict", F.col("n_users") > 1)
        .select(
            "user_session",
            "user_id",
            "has_user_conflict",
            "n_users",
            "session_start",
            "session_end",
            "duration_seconds",
            "n_events",
            "n_views",
            "n_carts",
            "n_purchases",
            "revenue",
        )
    )


def build_fact_events(events: DataFrame) -> DataFrame:
    """Event grain. Category is not repeated here; it is a product attribute.

    Brand is repeated because it is not a product attribute on this file.
    """
    return events.select(
        "event_date",
        "event_time",
        "event_type",
        "user_id",
        "user_session",
        "product_id",
        "brand",
        "price",
        "is_non_positive_price",
    )


def write_table(frame: DataFrame, path: Path, partitions: int | None = None) -> None:
    """Overwrite a gold table. ``partitions`` is a coalesce, not a date split."""
    writer = frame.coalesce(partitions) if partitions else frame
    writer.write.mode("overwrite").option("compression", "zstd").parquet(str(path))


def write_fact(frame: DataFrame, path: Path) -> None:
    """One file per UTC day. October has 31 dates."""
    (
        frame.repartition(31, "event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .option("compression", "zstd")
        .parquet(str(path))
    )


def directory_bytes(path: Path) -> int:
    """Sum the file sizes under a gold table directory."""
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def missing_keys(left: DataFrame, right: DataFrame, key: str) -> int:
    """Distinct ``key`` values in ``left`` that are absent from ``right``."""
    return (
        left.select(key)
        .distinct()
        .join(right.select(key).distinct(), key, "left_anti")
        .count()
    )


def conflict_purchase_stats(events: DataFrame, variants: DataFrame) -> dict:
    """Purchases that sit on a product with more than one brand."""
    conflict_ids = variants.filter(F.col("n_brands") > 1).select("product_id")
    flagged = events.join(F.broadcast(conflict_ids), "product_id", "left_semi")
    row = flagged.agg(
        F.count(F.lit(1)).alias("events"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum(purchase_revenue()).alias("revenue"),
    ).collect()[0]
    return {
        "events": _as_int(row["events"]),
        "purchases": _as_int(row["purchases"]),
        "revenue": _as_float(row["revenue"]),
    }


def modal_coverage(dim_product: DataFrame) -> dict:
    """How many events on multi-brand products use the modal brand."""
    conflicted = dim_product.filter(F.col("has_brand_conflict"))
    row = conflicted.agg(
        F.sum("brand_events").alias("modal_events"),
        F.sum("n_events").alias("events"),
    ).collect()[0]
    return {
        "modal_events": _as_int(row["modal_events"]),
        "events": _as_int(row["events"]),
    }


def validate(spark: SparkSession, gold: Path) -> dict:
    """Read the written tables and check grains, orphans, and additive facts."""
    fact = spark.read.parquet(str(gold / "fact_events"))
    dim_product = spark.read.parquet(str(gold / "dim_product"))
    dim_user = spark.read.parquet(str(gold / "dim_user"))
    dim_date = spark.read.parquet(str(gold / "dim_date"))
    sessions = spark.read.parquet(str(gold / "sessions"))

    fact_counts = fact.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum(purchase_revenue()).alias("revenue"),
        F.sum(F.when(F.col("brand").isNull(), 1).otherwise(0)).alias("null_brand"),
    ).collect()[0]
    session_counts = sessions.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("n_events").alias("n_events"),
        F.sum("n_views").alias("n_views"),
        F.sum("n_carts").alias("n_carts"),
        F.sum("n_purchases").alias("n_purchases"),
        F.sum("n_users").alias("user_links"),
        F.sum("revenue").alias("revenue"),
        F.sum(F.when(F.col("has_user_conflict"), 1).otherwise(0)).alias("conflict_sessions"),
        F.sum(F.when(F.col("duration_seconds") == 0, 1).otherwise(0)).alias("zero_duration"),
        F.sum(
            F.when(F.to_date("session_start") != F.to_date("session_end"), 1).otherwise(0)
        ).alias("cross_midnight"),
        F.min("duration_seconds").alias("min_duration"),
        F.max("duration_seconds").alias("max_duration"),
    ).collect()[0]
    user_counts = dim_user.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("total_events").alias("total_events"),
        F.sum("n_sessions").alias("n_sessions"),
        F.sum("n_purchases").alias("n_purchases"),
    ).collect()[0]
    product_rows = dim_product.count()
    date_rows = dim_date.count()
    date_span = dim_date.agg(
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date"),
        F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_days"),
    ).collect()[0]

    orphans = {
        "fact_user": missing_keys(fact, dim_user, "user_id"),
        "fact_product": missing_keys(fact, dim_product, "product_id"),
        "fact_date": missing_keys(fact, dim_date, "event_date"),
        "fact_session": missing_keys(fact, sessions, "user_session"),
        "session_user": missing_keys(sessions, dim_user, "user_id"),
        "user_without_fact": missing_keys(dim_user, fact, "user_id"),
        "product_without_fact": missing_keys(dim_product, fact, "product_id"),
        "date_without_fact": missing_keys(dim_date, fact, "event_date"),
        "session_without_fact": missing_keys(sessions, fact, "user_session"),
        "session_user_not_in_fact": (
            sessions.select("user_session", "user_id")
            .join(
                fact.select("user_session", "user_id").distinct(),
                ["user_session", "user_id"],
                "left_anti",
            )
            .count()
        ),
    }
    checks = {
        "fact_rows": _as_int(fact_counts["rows"]),
        "fact_views": _as_int(fact_counts["views"]),
        "fact_carts": _as_int(fact_counts["carts"]),
        "fact_purchases": _as_int(fact_counts["purchases"]),
        "fact_revenue": _as_float(fact_counts["revenue"]),
        "fact_null_brand": _as_int(fact_counts["null_brand"]),
        "session_rows": _as_int(session_counts["rows"]),
        "session_events": _as_int(session_counts["n_events"]),
        "session_views": _as_int(session_counts["n_views"]),
        "session_carts": _as_int(session_counts["n_carts"]),
        "session_purchases": _as_int(session_counts["n_purchases"]),
        "session_user_links": _as_int(session_counts["user_links"]),
        "session_revenue": _as_float(session_counts["revenue"]),
        "conflict_sessions": _as_int(session_counts["conflict_sessions"]),
        "zero_duration": _as_int(session_counts["zero_duration"]),
        "cross_midnight": _as_int(session_counts["cross_midnight"]),
        "min_duration": _as_int(session_counts["min_duration"]),
        "max_duration": _as_int(session_counts["max_duration"]),
        "user_rows": _as_int(user_counts["rows"]),
        "user_events": _as_int(user_counts["total_events"]),
        "user_session_links": _as_int(user_counts["n_sessions"]),
        "user_purchases": _as_int(user_counts["n_purchases"]),
        "product_rows": _as_int(product_rows),
        "date_rows": _as_int(date_rows),
        "min_date": str(date_span["min_date"]),
        "max_date": str(date_span["max_date"]),
        "weekend_days": _as_int(date_span["weekend_days"]),
        "orphans": orphans,
    }
    failures = []
    for name, value in orphans.items():
        if value != 0:
            failures.append(f"orphan {name}={value}")
    if checks["fact_null_brand"] != 0:
        failures.append("null brand on fact")
    for fact_name, session_name in (
        ("fact_rows", "session_events"),
        ("fact_views", "session_views"),
        ("fact_carts", "session_carts"),
        ("fact_purchases", "session_purchases"),
    ):
        if checks[fact_name] != checks[session_name]:
            failures.append(f"{fact_name} != {session_name}")
    if checks["fact_rows"] != checks["user_events"]:
        failures.append("fact rows != user total_events")
    if checks["fact_purchases"] != checks["user_purchases"]:
        failures.append("purchase counts disagree")
    if checks["user_session_links"] != checks["session_user_links"]:
        failures.append("user-session links disagree")
    if abs(checks["fact_revenue"] - checks["session_revenue"]) > 0.01:
        failures.append(
            f"revenue fact {checks['fact_revenue']} session {checks['session_revenue']}"
        )
    if checks["date_rows"] != 31 or checks["min_date"] != "2019-10-01" or checks["max_date"] != "2019-10-31":
        failures.append("date spine is not the 31 days of October 2019")
    if checks["min_duration"] < 0:
        failures.append("negative session duration")
    if failures:
        logger.error("Gold validation failed: %s", "; ".join(failures))
        sys.exit(1)
    return checks


def fmt_int(value: int) -> str:
    """Format an integer with thousands separators."""
    return f"{int(value):,}"


def fmt_money(value: float) -> str:
    """Currency to the cent when the value sits on a cent, else four places."""
    rounded = round(float(value), 2)
    if abs(float(value) - rounded) < 1e-4:
        return f"{rounded:,.2f}"
    return f"{float(value):,.4f}"


def render_model(
    summary: dict,
    purchase_conflict: dict,
    coverage: dict,
    checks: dict,
    dates: list[dict],
    sizes: dict,
    spark_version: str,
    elapsed_seconds: float,
) -> str:
    """Schema doc. Every count is a value this run just computed."""
    lines = [
        "# Gold data model",
        "",
        "Star schema built from `data/silver/events`. "
        f"Run {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} "
        f"on Spark {spark_version}. Elapsed {elapsed_seconds:.1f} seconds.",
        "",
        "Category is a property of `product_id` on this file, so it lives only on `dim_product`. "
        "Brand is not. `fact_events.brand` is the brand recorded on the event. "
        "`dim_product.brand` is the most common brand for that product, with the "
        "smaller brand string winning a tie.",
        "",
        "```mermaid",
        "erDiagram",
        "    DIM_DATE ||--o{ FACT_EVENTS : event_date",
        "    DIM_USER ||--o{ FACT_EVENTS : user_id",
        "    DIM_PRODUCT ||--o{ FACT_EVENTS : product_id",
        "    SESSIONS ||--o{ FACT_EVENTS : user_session",
        "    DIM_USER ||--o{ SESSIONS : user_id",
        "```",
        "",
        "`sessions` is a fact at session grain, not a dimension. It is drawn beside the dimensions because every event points at one session.",
        "",
        "## Grains",
        "",
        "| Table | Grain | Rows |",
        "| --- | --- | --- |",
        f"| fact_events | one silver event | {fmt_int(checks['fact_rows'])} |",
        f"| sessions | one user_session | {fmt_int(checks['session_rows'])} |",
        f"| dim_user | one user_id | {fmt_int(checks['user_rows'])} |",
        f"| dim_product | one product_id | {fmt_int(checks['product_rows'])} |",
        f"| dim_date | one event_date present in the month | {fmt_int(checks['date_rows'])} |",
        "",
        "## fact_events",
        "",
        "Partitioned by `event_date`. One file per day.",
        "",
        "| Column | Role |",
        "| --- | --- |",
        "| event_date | Foreign key to dim_date. UTC date. |",
        "| event_time | Event instant. UTC. |",
        "| event_type | view, cart, or purchase. |",
        "| user_id | Foreign key to dim_user. The user on this event. |",
        "| user_session | Foreign key to sessions. |",
        "| product_id | Foreign key to dim_product. |",
        "| brand | Degenerate dimension. The brand on this event, not the modal brand. |",
        "| price | Price recorded on the event. Zero prices are kept. |",
        "| is_non_positive_price | True when price is not null and price <= 0. |",
        "",
        f"Null brand rows: {fmt_int(checks['fact_null_brand'])}.",
        "",
        "## dim_product",
        "",
        "| Column | Role |",
        "| --- | --- |",
        "| product_id | Primary key. |",
        "| category_id, category_code, category_l1, category_l2, category_l3 | The product's only category tuple. |",
        "| brand | Modal brand. Tie breaks to the smaller brand string. |",
        "| brand_events | Events that carry the modal brand. |",
        "| n_events | Events for this product. |",
        "| n_brands | Distinct brands seen on this product. |",
        "| has_brand_conflict | True when n_brands > 1. |",
        "",
        (
            f"Products: {fmt_int(summary['products'])}. "
            f"Products with more than one category tuple: {fmt_int(summary['multi_category_products'])}. "
            f"Products with more than one brand: {fmt_int(summary['multi_brand_products'])}, "
            f"covering {fmt_int(summary['multi_brand_events'])} events. "
            f"Of those events, {fmt_int(coverage['modal_events'])} carry the modal brand. "
            f"Purchases on a multi-brand product: {fmt_int(purchase_conflict['purchases'])}, "
            f"revenue {fmt_money(purchase_conflict['revenue'])}."
        ),
        "",
        "Use `fact_events.brand` for brand revenue. Using `dim_product.brand` would relabel the other brands on a conflicted product.",
        "",
        "## dim_user",
        "",
        "| Column | Role |",
        "| --- | --- |",
        "| user_id | Primary key. |",
        "| first_seen | Minimum event_time. |",
        "| last_seen | Maximum event_time. |",
        "| total_events | Events with this user_id. |",
        "| n_sessions | Distinct user_session values on those events. |",
        "| n_purchases | Purchase events. Includes a purchase whose price flag is true. |",
        "",
        (
            f"The sum of `n_sessions` is {fmt_int(checks['user_session_links'])}. "
            f"`sessions` has {fmt_int(checks['session_rows'])} rows. "
            f"The difference is user links inside sessions that contain more than one user_id."
        ),
        "",
        "## dim_date",
        "",
        "The column is `event_date`, not `date`, so SQL joins do not need a reserved word. "
        "`day_of_week` is 1 for Monday through 7 for Sunday. "
        "`is_weekend` is Saturday or Sunday of that UTC date. "
        "`week` is Spark `weekofyear`. "
        f"The spine has {fmt_int(checks['date_rows'])} dates, "
        f"{checks['min_date']} through {checks['max_date']}, "
        f"of which {fmt_int(checks['weekend_days'])} are weekend dates.",
        "",
        "| event_date | day_of_week | day_name | is_weekend | week | year |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in dates:
        lines.append(
            f"| {row['event_date']} | {row['day_of_week']} | {row['day_name']} | "
            f"{str(row['is_weekend']).lower()} | {row['week']} | {row['year']} |"
        )
    lines.extend(
        [
            "",
            "## sessions",
            "",
            "Not partitioned by date. A session can start on one UTC date and end on the next.",
            "",
            "| Column | Role |",
            "| --- | --- |",
            "| user_session | Primary key. |",
            "| user_id | User with the most events in the session. Smaller user_id wins a tie. Foreign key to dim_user. |",
            "| has_user_conflict | True when the session contains more than one user_id. |",
            "| n_users | Distinct user_id values in the session. |",
            "| session_start, session_end | Min and max event_time. |",
            "| duration_seconds | session_end minus session_start, in seconds. A single instant has duration 0. |",
            "| n_events, n_views, n_carts, n_purchases | Counts of events in the whole session. |",
            "| revenue | Sum of price on purchases with is_non_positive_price = false. Otherwise 0. |",
            "",
            (
                f"Sessions with more than one user_id: {fmt_int(checks['conflict_sessions'])}. "
                f"Sessions whose start and end fall on different UTC dates: {fmt_int(checks['cross_midnight'])}. "
                f"Sessions with duration 0: {fmt_int(checks['zero_duration'])}. "
                f"Duration ranges from {fmt_int(checks['min_duration'])} to {fmt_int(checks['max_duration'])} seconds."
            ),
            "",
            "## Revenue",
            "",
            "Revenue is not the sum of every price. It is purchase price where `is_non_positive_price` is false. "
            f"Fact revenue is {fmt_money(checks['fact_revenue'])}. "
            f"The sum of `sessions.revenue` is {fmt_money(checks['session_revenue'])}.",
            "",
            "## Reconciliation",
            "",
            "| Check | Fact or users | Sessions |",
            "| --- | --- | --- |",
            f"| Events | {fmt_int(checks['fact_rows'])} | {fmt_int(checks['session_events'])} |",
            f"| Views | {fmt_int(checks['fact_views'])} | {fmt_int(checks['session_views'])} |",
            f"| Carts | {fmt_int(checks['fact_carts'])} | {fmt_int(checks['session_carts'])} |",
            f"| Purchases | {fmt_int(checks['fact_purchases'])} | {fmt_int(checks['session_purchases'])} |",
            f"| Revenue | {fmt_money(checks['fact_revenue'])} | {fmt_money(checks['session_revenue'])} |",
            f"| Sum of user total_events | {fmt_int(checks['user_events'])} |  |",
            f"| Sum of user n_purchases | {fmt_int(checks['user_purchases'])} |  |",
            f"| User-session links | {fmt_int(checks['user_session_links'])} | {fmt_int(checks['session_user_links'])} |",
            "",
            "## Orphan keys",
            "",
            "Each value is a count of distinct keys on the left that are missing from the right. Zero means the foreign key holds.",
            "",
            "| Check | Missing keys |",
            "| --- | --- |",
        ]
    )
    labels = {
        "fact_user": "fact_events.user_id missing from dim_user",
        "fact_product": "fact_events.product_id missing from dim_product",
        "fact_date": "fact_events.event_date missing from dim_date",
        "fact_session": "fact_events.user_session missing from sessions",
        "session_user": "sessions.user_id missing from dim_user",
        "user_without_fact": "dim_user.user_id missing from fact_events",
        "product_without_fact": "dim_product.product_id missing from fact_events",
        "date_without_fact": "dim_date.event_date missing from fact_events",
        "session_without_fact": "sessions.user_session missing from fact_events",
        "session_user_not_in_fact": "sessions.user_id not present on that session in fact_events",
    }
    for key, label in labels.items():
        lines.append(f"| {label} | {fmt_int(checks['orphans'][key])} |")
    lines.extend(
        [
            "",
            "## Folder size",
            "",
            "| Path | Bytes |",
            "| --- | --- |",
        ]
    )
    for name in ("fact_events", "sessions", "dim_user", "dim_product", "dim_date"):
        lines.append(f"| data/gold/{name} | {fmt_int(sizes[name])} |")
    lines.append(f"| data/gold total | {fmt_int(sum(sizes.values()))} |")
    lines.append("")
    return "\n".join(lines)


def run_self_test(spark: SparkSession) -> None:
    """Pin the modal brand, the session user, revenue, and the weekend rule."""
    rows = [
        ("2019-10-01 00:00:00", "2019-10-01", "view", 1, "s1", 10, "samsung", 5.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-01 00:01:00", "2019-10-01", "view", 1, "s1", 10, "samsung", 5.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-01 00:02:00", "2019-10-01", "view", 1, "s1", 10, "apple", 5.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-01 00:03:00", "2019-10-01", "view", 1, "s1", 10, "apple", 5.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-01 00:04:00", "2019-10-01", "view", 2, "s1", 10, "apple", 9.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-05 10:00:00", "2019-10-05", "view", 1, "s2", 10, "apple", 999.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-05 11:00:00", "2019-10-05", "purchase", 1, "s2", 10, "apple", 100.0, False, 1, "electronics.smartphone", "electronics", "smartphone", "unknown"),
        ("2019-10-06 12:00:00", "2019-10-06", "purchase", 3, "s3", 30, "lg", 0.0, True, 2, "appliances.kitchen.oven", "appliances", "kitchen", "oven"),
        ("2019-10-07 08:00:00", "2019-10-07", "view", 3, "s4", 30, "lg", 1.0, False, 2, "appliances.kitchen.oven", "appliances", "kitchen", "oven"),
    ]
    frame = spark.createDataFrame(
        rows,
        [
            "event_time",
            "event_date",
            "event_type",
            "user_id",
            "user_session",
            "product_id",
            "brand",
            "price",
            "is_non_positive_price",
            "category_id",
            "category_code",
            "category_l1",
            "category_l2",
            "category_l3",
        ],
    )
    frame = frame.withColumn("event_time", F.to_timestamp("event_time")).withColumn(
        "event_date", F.to_date("event_date")
    )
    variants = product_variants(frame)
    summary = variant_summary(variants)
    if summary["multi_category_products"] != 0 or summary["multi_brand_products"] != 1:
        logger.error("Self-test variant summary %s", summary)
        sys.exit(1)
    product = {row["product_id"]: row.asDict() for row in build_dim_product(frame, variants).collect()}
    # Two samsung and two apple on product 10, plus one more apple from user 2.
    # apple has 3, samsung has 2, so the modal brand is apple.
    if product[10]["brand"] != "apple" or product[10]["n_brands"] != 2:
        logger.error("Self-test modal brand failed: %s", product[10])
        sys.exit(1)
    if product[10]["category_l2"] != "smartphone" or product[30]["category_l3"] != "oven":
        logger.error("Self-test category copy failed")
        sys.exit(1)
    sessions = {row["user_session"]: row.asDict() for row in build_sessions(frame).collect()}
    if sessions["s1"]["user_id"] != 1 or sessions["s1"]["has_user_conflict"] is not True:
        logger.error("Self-test session user failed: %s", sessions["s1"])
        sys.exit(1)
    if sessions["s2"]["revenue"] != 100 or sessions["s2"]["n_views"] != 1 or sessions["s2"]["n_purchases"] != 1:
        logger.error("Self-test revenue failed: %s", sessions["s2"])
        sys.exit(1)
    if sessions["s3"]["revenue"] != 0 or sessions["s3"]["n_purchases"] != 1:
        logger.error("Self-test zero-price purchase failed: %s", sessions["s3"])
        sys.exit(1)
    if sessions["s4"]["duration_seconds"] != 0:
        logger.error("Self-test duration failed: %s", sessions["s4"])
        sys.exit(1)
    dates = {str(row["event_date"]): row.asDict() for row in build_dim_date(frame).collect()}
    tuesday = dates["2019-10-01"]
    saturday = dates["2019-10-05"]
    sunday = dates["2019-10-06"]
    monday = dates["2019-10-07"]
    if tuesday["day_name"] != "Tuesday" or tuesday["day_of_week"] != 2 or tuesday["is_weekend"] is not False:
        logger.error("Self-test Tuesday failed: %s", tuesday)
        sys.exit(1)
    if not saturday["is_weekend"] or not sunday["is_weekend"] or monday["is_weekend"]:
        logger.error("Self-test weekend flags failed")
        sys.exit(1)
    if saturday["week"] != sunday["week"] or monday["week"] == sunday["week"]:
        logger.error("Self-test week boundary failed: %s %s %s", saturday, sunday, monday)
        sys.exit(1)
    logger.info("Self-test passed")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the gold CLI, including the self-test flag."""
    parser = argparse.ArgumentParser(description="Build the gold star schema.")
    parser.add_argument("--silver", type=Path, default=DEFAULT_SILVER)
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC)
    parser.add_argument(
        "--driver-memory",
        default=os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY),
    )
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", DEFAULT_MASTER))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Build the gold tables and overwrite docs/data_model.md."""
    args = parse_args(argv)
    gold = args.gold.resolve()
    doc_path = args.doc.resolve()
    assert_under_gold(gold)
    assert_safe_doc(doc_path)
    started = time.perf_counter()
    spark = build_spark(args.driver_memory, args.master)
    try:
        if args.self_test:
            run_self_test(spark)
            return
        silver_path = args.silver.resolve()
        if not silver_path.exists():
            logger.error("Silver path not found: %s", silver_path)
            sys.exit(1)
        events = spark.read.parquet(str(silver_path))

        variants = product_variants(events)
        summary = variant_summary(variants)
        logger.info("GOLD_VARIANTS %s", summary)
        if summary["multi_category_products"] != 0:
            logger.error(
                "A product has more than one category tuple (%s). "
                "dim_product cannot hold the only category.",
                summary["multi_category_products"],
            )
            sys.exit(1)

        dim_product = build_dim_product(events, variants)
        coverage = modal_coverage(dim_product)
        purchase_conflict = conflict_purchase_stats(events, variants)
        logger.info("GOLD_MODAL_COVERAGE %s", coverage)
        logger.info("GOLD_CONFLICT_PURCHASES %s", purchase_conflict)

        dim_user = build_dim_user(events)
        dim_date = build_dim_date(events)
        sessions = build_sessions(events)
        fact = build_fact_events(events)

        write_fact(fact, gold / "fact_events")
        write_table(sessions, gold / "sessions", partitions=8)
        write_table(dim_user, gold / "dim_user", partitions=4)
        write_table(dim_product, gold / "dim_product", partitions=1)
        write_table(dim_date, gold / "dim_date", partitions=1)
        logger.info("GOLD_WRITTEN")

        checks = validate(spark, gold)
        logger.info(
            "GOLD_VALIDATED fact_rows=%s sessions=%s users=%s products=%s revenue=%s",
            checks["fact_rows"],
            checks["session_rows"],
            checks["user_rows"],
            checks["product_rows"],
            checks["fact_revenue"],
        )
        dates = [
            row.asDict()
            for row in spark.read.parquet(str(gold / "dim_date"))
            .orderBy("event_date")
            .collect()
        ]
        for row in dates:
            row["event_date"] = str(row["event_date"])
        sizes = {
            name: directory_bytes(gold / name)
            for name in ("fact_events", "sessions", "dim_user", "dim_product", "dim_date")
        }
        logger.info("GOLD_BYTES %s", sizes)
        elapsed = time.perf_counter() - started
        doc_path.parent.mkdir(parents=True, exist_ok=True)
        doc_path.write_text(
            render_model(
                summary,
                purchase_conflict,
                coverage,
                checks,
                dates,
                sizes,
                spark.version,
                elapsed,
            )
        )
        logger.info("Wrote %s", doc_path)
    finally:
        spark.stop()
    if not args.self_test:
        logger.info("GOLD_ELAPSED seconds=%.1f", time.perf_counter() - started)
        logger.info("Gold build passed")


if __name__ == "__main__":
    main()
