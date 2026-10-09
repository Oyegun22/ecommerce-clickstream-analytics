"""Profile bronze events and write ``docs/data_profile.md``.

The silver rules in the next phase are chosen from this file, so every
figure is a query result. The script reads ``data/bronze/events`` and does
not write under ``data/``.

Decisions worth defending:

- Profile bronze, not the CSV. Bronze is the typed landing the rest of the
  pipeline reads, and the row count was already tied back to ``wc -l``.
- Duplicate detection is ``GROUP BY`` all nine source columns ``HAVING
  COUNT(*) > 1``. A self-join would shuffle the month twice. ``event_date``
  is left out of the key because it is derived from ``event_time``.
- Median and p99 use Spark's exact ``percentile`` aggregate, not
  ``percentile_approx``. Spark interpolates when the rank falls between two
  prices, so a median can sit halfway between two observed prices.
- A null ``user_session`` is not a session. Session stats drop those rows
  and the null count is reported beside them. The same rule is used for
  ``user_id``.
- Driver heap is 8g. The Parquet is about 700 MB, which is inside the 12g
  project cap and leaves more of the 24 GB machine for macOS.
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
DEFAULT_BRONZE = PROJECT_ROOT / "data" / "bronze" / "events"
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "data_profile.md"
DATA_ROOT = PROJECT_ROOT / "data"
MAX_DRIVER_BYTES = 12 * 1024**3
DEFAULT_DRIVER_MEMORY = "8g"
DEFAULT_MASTER = "local[6]"

# Nine fields as they arrived in the CSV. event_date is not part of an
# exact-duplicate key: it is to_date(event_time) in UTC.
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
PROFILE_COLUMNS = SOURCE_COLUMNS + ["event_date"]

# Fixed cuts so the tables are readable. The counts are measured; the cuts
# are a presentation choice, not clusters learned from the data.
SESSION_BUCKETS = ("0", "1", "2", "3-5", "6-10", "11-20", "21+")
EVENT_BUCKETS = ("1", "2-5", "6-20", "21-50", "51-200", "201+")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("profile")


def memory_to_bytes(value: str) -> int:
    """Parse a Spark size such as ``8g`` or ``512m``."""
    text = value.strip().lower()
    if text.endswith("g"):
        return int(float(text[:-1]) * 1024**3)
    if text.endswith("m"):
        return int(float(text[:-1]) * 1024**2)
    raise ValueError(f"Use a Spark size like 8g or 512m, got {value!r}")


def assert_safe_output(path: Path) -> None:
    """The profile is documentation. It must not land inside ``data/``."""
    resolved = path.resolve()
    data_root = DATA_ROOT.resolve()
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
        SparkSession.builder.appName("bronze-profile")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
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


def load_events(spark: SparkSession, bronze: Path) -> DataFrame:
    """Read the partitioned bronze table. Missing path is a hard failure."""
    if not bronze.exists():
        logger.error("Bronze path not found: %s", bronze)
        sys.exit(1)
    return spark.read.parquet(str(bronze))


def _as_int(value: object) -> int:
    """Cast a Spark aggregate to int."""
    if value is None:
        return 0
    return int(value)


def _as_float(value: object) -> float:
    """Cast a Spark aggregate to float."""
    return float(value)


def column_and_price_stats(events: DataFrame) -> dict:
    """One pass: nulls, price shape, event mix, brand cleanup, date span.

    ``percentile`` is exact. Null prices are ignored by min, max, mean, and
    percentile, so those four numbers describe non-null prices only.
    ``price <= 0`` does not count nulls, because a null comparison is null
    and ``when`` then falls through to 0.
    """
    pieces = [F.count(F.lit(1)).alias("rows")]
    for column in PROFILE_COLUMNS:
        pieces.append(
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"null__{column}")
        )
    for column in ("event_type", "category_code", "brand", "user_session"):
        pieces.append(
            F.sum(F.when(F.col(column) == "", 1).otherwise(0)).alias(f"empty__{column}")
        )
    brand = F.col("brand")
    normalised = F.lower(F.trim(brand))
    pieces.extend(
        [
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("events_view"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("events_cart"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "events_purchase"
            ),
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            F.avg("price").alias("price_mean"),
            F.percentile("price", [0.5, 0.99], F.lit(1)).alias("price_quantiles"),
            F.sum(F.when(F.col("price") < 0, 1).otherwise(0)).alias("price_negative"),
            F.sum(F.when(F.col("price") == 0, 1).otherwise(0)).alias("price_zero"),
            F.sum(F.when(F.col("price") > 0, 1).otherwise(0)).alias("price_positive"),
            F.sum(
                F.when(brand.isNotNull() & (brand != F.trim(brand)), 1).otherwise(0)
            ).alias("brand_untrimmed"),
            F.sum(
                F.when(
                    brand.isNotNull()
                    & (brand == F.trim(brand))
                    & (brand != F.lower(brand)),
                    1,
                ).otherwise(0)
            ).alias("brand_mixed_case"),
            F.sum(F.when(brand.isNotNull() & (brand != normalised), 1).otherwise(0)).alias(
                "brand_needs_normalising"
            ),
            F.min("event_date").alias("min_date"),
            F.max("event_date").alias("max_date"),
            F.countDistinct("event_date").alias("n_dates"),
        ]
    )
    row = events.agg(*pieces).collect()[0]
    quantiles = list(row["price_quantiles"])
    stats = {
        "rows": _as_int(row["rows"]),
        "nulls": {column: _as_int(row[f"null__{column}"]) for column in PROFILE_COLUMNS},
        "empties": {
            column: _as_int(row[f"empty__{column}"])
            for column in ("event_type", "category_code", "brand", "user_session")
        },
        "events_view": _as_int(row["events_view"]),
        "events_cart": _as_int(row["events_cart"]),
        "events_purchase": _as_int(row["events_purchase"]),
        "price_min": _as_float(row["price_min"]),
        "price_max": _as_float(row["price_max"]),
        "price_mean": _as_float(row["price_mean"]),
        "price_median": _as_float(quantiles[0]),
        "price_p99": _as_float(quantiles[1]),
        "price_negative": _as_int(row["price_negative"]),
        "price_zero": _as_int(row["price_zero"]),
        "price_positive": _as_int(row["price_positive"]),
        "brand_untrimmed": _as_int(row["brand_untrimmed"]),
        "brand_mixed_case": _as_int(row["brand_mixed_case"]),
        "brand_needs_normalising": _as_int(row["brand_needs_normalising"]),
        "min_date": str(row["min_date"]),
        "max_date": str(row["max_date"]),
        "n_dates": _as_int(row["n_dates"]),
    }
    known = stats["events_view"] + stats["events_cart"] + stats["events_purchase"]
    stats["events_other"] = stats["rows"] - known
    price_accounted = (
        stats["nulls"]["price"]
        + stats["price_negative"]
        + stats["price_zero"]
        + stats["price_positive"]
    )
    if price_accounted != stats["rows"]:
        logger.error("Price buckets sum to %s, rows=%s", price_accounted, stats["rows"])
        sys.exit(1)
    if known + stats["events_other"] != stats["rows"]:
        logger.error("Event type buckets do not sum to the row count")
        sys.exit(1)
    return stats


def cardinality_stats(events: DataFrame) -> dict:
    """Exact distinct counts. Nulls are not counted as a category or a brand."""
    row = events.agg(
        F.countDistinct("product_id").alias("products"),
        F.countDistinct("category_id").alias("category_ids"),
        F.countDistinct("category_code").alias("category_codes"),
        F.countDistinct("brand").alias("brands"),
        F.countDistinct(F.lower(F.trim(F.col("brand")))).alias("brands_normalised"),
    ).collect()[0]
    return {key: _as_int(row[key]) for key in row.asDict()}


def category_shape(events: DataFrame) -> list[dict]:
    """How many dot-separated levels ``category_code`` uses.

    The published codes look like ``appliances.environment.water_heater``.
    Silver will split on ``.`` into three levels, so a depth other than 3,
    or an empty token, would make that split lossy.
    """
    depth = F.when(
        F.col("category_code").isNull(),
        F.lit(None).cast("int"),
    ).otherwise(F.size(F.split(F.col("category_code"), r"\.")))
    shaped = events.withColumn("depth", depth)
    totals = (
        shaped.groupBy("depth")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("category_code").alias("codes"),
            F.sum(
                F.when(
                    F.array_contains(F.split(F.col("category_code"), r"\."), F.lit("")),
                    1,
                ).otherwise(0)
            ).alias("empty_token_rows"),
        )
        .collect()
    )
    # One example per depth: the most common code, ties broken by the code
    # itself so a re-run does not flip the example.
    window = Window.partitionBy("depth").orderBy(F.desc("count"), F.asc("category_code"))
    examples = {
        row["depth"]: row["category_code"]
        for row in (
            shaped.filter(F.col("category_code").isNotNull())
            .groupBy("depth", "category_code")
            .count()
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .collect()
        )
    }
    records = []
    for row in totals:
        depth_value = row["depth"]
        records.append(
            {
                "depth": "missing" if depth_value is None else str(int(depth_value)),
                "depth_sort": -1 if depth_value is None else int(depth_value),
                "rows": _as_int(row["rows"]),
                "codes": _as_int(row["codes"]),
                "empty_token_rows": _as_int(row["empty_token_rows"]),
                "example": examples.get(depth_value, ""),
            }
        )
    records.sort(key=lambda item: item["depth_sort"])
    return records


def duplicate_stats(spark: SparkSession) -> dict:
    """Count exact duplicate groups without collecting them.

    The inner query is the efficient form: one hash aggregation and a
    ``HAVING`` filter. Only the three scalars are collected. Unique rows
    never leave the executors.
    """
    column_list = ", ".join(SOURCE_COLUMNS)
    row = spark.sql(
        f"""
        SELECT
            COUNT(*) AS duplicate_groups,
            COALESCE(SUM(c), 0) AS rows_in_duplicate_groups,
            COALESCE(SUM(c - 1), 0) AS extra_rows
        FROM (
            SELECT COUNT(*) AS c
            FROM bronze_events
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
        )
        """
    ).collect()[0]
    return {
        "duplicate_groups": _as_int(row["duplicate_groups"]),
        "rows_in_duplicate_groups": _as_int(row["rows_in_duplicate_groups"]),
        "extra_rows": _as_int(row["extra_rows"]),
    }


def _distribution(frame: DataFrame, value_column: str) -> dict:
    """Min, mean, exact median, exact p99, and max for one grain."""
    row = frame.agg(
        F.count(F.lit(1)).alias("n"),
        F.min(value_column).alias("min"),
        F.avg(value_column).alias("mean"),
        F.percentile(F.col(value_column).cast("double"), [0.5, 0.99], F.lit(1)).alias(
            "quantiles"
        ),
        F.max(value_column).alias("max"),
        F.sum(F.col(value_column)).alias("total"),
    ).collect()[0]
    quantiles = list(row["quantiles"])
    return {
        "n": _as_int(row["n"]),
        "min": _as_float(row["min"]),
        "mean": _as_float(row["mean"]),
        "median": _as_float(quantiles[0]),
        "p99": _as_float(quantiles[1]),
        "max": _as_float(row["max"]),
        "total": _as_int(row["total"]),
    }


def _buckets(frame: DataFrame, value_column: str, labels: tuple[str, ...], bucket_expr) -> list[dict]:
    """Count rows into the requested labels."""
    rows = (
        frame.groupBy(bucket_expr.alias("bucket"))
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    )
    counts = {row["bucket"]: _as_int(row["n"]) for row in rows}
    return [{"bucket": label, "n": counts.get(label, 0)} for label in labels]


def session_bucket(column):
    """Map a session count to a fixed bucket. Zero means the user has only null sessions."""
    return (
        F.when(column == 0, "0")
        .when(column == 1, "1")
        .when(column == 2, "2")
        .when(column.between(3, 5), "3-5")
        .when(column.between(6, 10), "6-10")
        .when(column.between(11, 20), "11-20")
        .otherwise("21+")
    )


def event_bucket(column):
    """Map an event count to a fixed bucket."""
    return (
        F.when(column == 1, "1")
        .when(column.between(2, 5), "2-5")
        .when(column.between(6, 20), "6-20")
        .when(column.between(21, 50), "21-50")
        .when(column.between(51, 200), "51-200")
        .otherwise("201+")
    )


def session_and_user_stats(events: DataFrame) -> dict:
    """Session grain first, then user grain. Cache one grain at a time.

    Holding both aggregates in an 8g heap at once is unnecessary. Each
    grain is summarised, then unpersisted, before the next shuffle.
    """
    sessions = (
        events.filter(F.col("user_session").isNotNull())
        .groupBy("user_session")
        .agg(
            F.count(F.lit(1)).alias("n_events"),
            F.countDistinct("user_id").alias("n_users"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "n_purchases"
            ),
        )
    )
    sessions = sessions.persist()
    try:
        event_dist = _distribution(sessions, "n_events")
        event_buckets = _buckets(sessions, "n_events", EVENT_BUCKETS, event_bucket(F.col("n_events")))
        flags = sessions.agg(
            F.sum(F.when(F.col("n_purchases") > 0, 1).otherwise(0)).alias("sessions_with_purchase"),
            F.sum(F.when(F.col("n_users") > 1, 1).otherwise(0)).alias("sessions_multi_user"),
            F.sum(F.when(F.col("n_users") == 0, 1).otherwise(0)).alias("sessions_without_user"),
        ).collect()[0]
    finally:
        sessions.unpersist()

    users = (
        events.filter(F.col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(F.countDistinct("user_session").alias("n_sessions"))
    )
    users = users.persist()
    try:
        user_dist = _distribution(users, "n_sessions")
        user_buckets = _buckets(
            users, "n_sessions", SESSION_BUCKETS, session_bucket(F.col("n_sessions"))
        )
    finally:
        users.unpersist()

    return {
        "event_dist": event_dist,
        "event_buckets": event_buckets,
        "user_dist": user_dist,
        "user_buckets": user_buckets,
        "sessions_with_purchase": _as_int(flags["sessions_with_purchase"]),
        "sessions_multi_user": _as_int(flags["sessions_multi_user"]),
        "sessions_without_user": _as_int(flags["sessions_without_user"]),
    }


def fmt_int(value: int) -> str:
    """Format an integer with thousands separators."""
    return f"{int(value):,}"


def fmt_pct(numerator: int, denominator: int) -> str:
    """Percent of ``denominator``. Tiny shares keep enough digits to stay non-zero."""
    if denominator == 0:
        return "n/a"
    if numerator == 0:
        return "0%"
    value = 100.0 * numerator / denominator
    if value >= 0.01:
        return f"{value:.2f}%"
    if value >= 0.0001:
        return f"{value:.4f}%"
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return f"{text}%"


def fmt_price(value: float) -> str:
    """Show cents when the value lands on a cent. Otherwise keep four places."""
    rounded = round(float(value), 2)
    if abs(float(value) - rounded) < 1e-6:
        return f"{rounded:,.2f}"
    return f"{float(value):,.4f}"


def fmt_count_stat(value: float) -> str:
    """Session and event counts are whole numbers unless percentile interpolates."""
    if abs(float(value) - round(float(value))) < 1e-6:
        return fmt_int(int(round(float(value))))
    return f"{float(value):,.2f}"


def _sentence(*parts: str) -> str:
    """Join interpretation fragments into one sentence."""
    text = " ".join(part.strip() for part in parts if part.strip())
    if not text.endswith("."):
        text += "."
    return text


def interpret_population(metrics: dict) -> str:
    """Describe the profiled row, user, session, and product counts."""
    cards = metrics["cardinality"]
    users = metrics["sessions"]["user_dist"]["n"]
    sessions = metrics["sessions"]["event_dist"]["n"]
    return _sentence(
        f"The month contains {fmt_int(metrics['rows'])} events from "
        f"{fmt_int(users)} users, {fmt_int(sessions)} sessions, and "
        f"{fmt_int(cards['products'])} products."
    )


def interpret_events(metrics: dict) -> str:
    """Describe the view, cart, and purchase mix."""
    rows = metrics["rows"]
    other = (
        "no other event_type is present"
        if metrics["events_other"] == 0
        else f"{fmt_int(metrics['events_other'])} events have another event_type"
    )
    return _sentence(
        f"Views are {fmt_pct(metrics['events_view'], rows)} of events, "
        f"carts {fmt_pct(metrics['events_cart'], rows)}, and purchases "
        f"{fmt_pct(metrics['events_purchase'], rows)}; {other}"
    )


def interpret_nulls(metrics: dict) -> str:
    """Describe which columns are null and how often."""
    nulls = metrics["nulls"]
    present = [(name, count) for name, count in nulls.items() if count]
    if not present:
        return "No profiled column contains a null."
    present.sort(key=lambda item: -item[1])
    listed = ", ".join(
        f"{name} ({fmt_int(count)} rows, {fmt_pct(count, metrics['rows'])})"
        for name, count in present
    )
    return _sentence(f"Nulls occur only in {listed}")


def interpret_price(metrics: dict) -> str:
    """Describe the price distribution, including the zero prices."""
    skew = (
        "right-skewed"
        if metrics["price_mean"] > metrics["price_median"]
        else "not right-skewed"
    )
    if metrics["price_negative"] == 0 and metrics["price_zero"] > 0:
        non_positive = (
            f"{fmt_int(metrics['price_zero'])} events are priced at exactly 0 and none are negative"
        )
    elif metrics["price_negative"] == 0 and metrics["price_zero"] == 0:
        non_positive = "no event has a price of zero or below"
    else:
        non_positive = (
            f"{fmt_int(metrics['price_negative'])} events have a negative price and "
            f"{fmt_int(metrics['price_zero'])} have a price of 0"
        )
    return _sentence(
        f"The median price is {fmt_price(metrics['price_median'])} against a mean of "
        f"{fmt_price(metrics['price_mean'])} and a p99 of {fmt_price(metrics['price_p99'])}, "
        f"so the distribution is {skew}, and {non_positive}"
    )


def non_positive_prices(events: DataFrame) -> list[dict]:
    """Break price <= 0 down by event_type. Empty when every price is positive."""
    rows = (
        events.filter(F.col("price") <= 0)
        .groupBy("event_type")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
        .orderBy("event_type")
        .collect()
    )
    return [
        {
            "event_type": row["event_type"] if row["event_type"] is not None else "NULL",
            "rows": _as_int(row["rows"]),
            "min_price": _as_float(row["min_price"]),
            "max_price": _as_float(row["max_price"]),
        }
        for row in rows
    ]


def interpret_non_positive(metrics: dict) -> str:
    """Describe where the non-positive prices sit by event type."""
    total = sum(row["rows"] for row in metrics["price_by_type"])
    listed = ", ".join(
        f"{row['event_type']} {fmt_int(row['rows'])}" for row in metrics["price_by_type"]
    )
    return _sentence(
        f"The {fmt_int(total)} non-positive prices are split across event types as {listed}"
    )


def interpret_duplicates(metrics: dict) -> str:
    """Describe the exact-duplicate groups and the extra rows they add."""
    extra = metrics["duplicates"]["extra_rows"]
    groups = metrics["duplicates"]["duplicate_groups"]
    return _sentence(
        f"Exact matches on the nine source columns produce {fmt_int(groups)} repeated "
        f"groups and {fmt_int(extra)} extra rows "
        f"({fmt_pct(extra, metrics['rows'])} of events)"
    )


def interpret_categories(metrics: dict) -> str:
    """Describe category depth, including the four-part piano code."""
    rows = [row for row in metrics["categories"] if row["depth"] != "missing"]
    if not rows:
        return "Every category_code is null."
    listed = ", ".join(f"depth {row['depth']} ({fmt_int(row['rows'])} rows)" for row in rows)
    empty_tokens = sum(row["empty_token_rows"] for row in metrics["categories"])
    return _sentence(
        f"Non-null category codes are {listed}, and {fmt_int(empty_tokens)} rows "
        f"contain an empty token between dots"
    )


def interpret_brands(metrics: dict) -> str:
    """Describe brand cardinality and the null brand rate."""
    raw = metrics["cardinality"]["brands"]
    normalised = metrics["cardinality"]["brands_normalised"]
    changed = metrics["brand_needs_normalising"]
    if changed == 0 and raw == normalised:
        return _sentence(
            f"Every non-null brand is already trimmed and lowercase, so the distinct "
            f"brand count stays {fmt_int(raw)}"
        )
    return _sentence(
        f"Lowercasing and trimming would change {fmt_int(changed)} brand values and "
        f"move the distinct brand count from {fmt_int(raw)} to {fmt_int(normalised)}"
    )


def interpret_users(metrics: dict) -> str:
    """Describe how sessions are spread across users."""
    dist = metrics["sessions"]["user_dist"]
    single = next(bucket for bucket in metrics["sessions"]["user_buckets"] if bucket["bucket"] == "1")
    return _sentence(
        f"The median user has {fmt_count_stat(dist['median'])} distinct sessions, "
        f"{fmt_pct(single['n'], dist['n'])} of users have exactly one, and the busiest "
        f"user has {fmt_count_stat(dist['max'])}"
    )


def interpret_session_length(metrics: dict) -> str:
    """Describe how events are spread across sessions."""
    dist = metrics["sessions"]["event_dist"]
    multi = metrics["sessions"]["sessions_multi_user"]
    return _sentence(
        f"The median session has {fmt_count_stat(dist['median'])} events, the longest "
        f"has {fmt_count_stat(dist['max'])}, and {fmt_int(multi)} sessions are tied to "
        f"more than one user_id"
    )


def interpret_purchase_sessions(metrics: dict) -> str:
    """Describe how many sessions contain a purchase."""
    sessions = metrics["sessions"]["event_dist"]["n"]
    with_purchase = metrics["sessions"]["sessions_with_purchase"]
    return _sentence(
        f"{fmt_int(with_purchase)} sessions contain at least one purchase "
        f"({fmt_pct(with_purchase, sessions)} of non-null sessions); "
        f"this is co-occurrence inside the session, not an ordered funnel"
    )


def render_markdown(metrics: dict) -> str:
    """Build the profile from measured values only."""
    rows = metrics["rows"]
    lines: list[str] = []
    lines.append("# Bronze data profile")
    lines.append("")
    lines.append(
        f"Source: `data/bronze/events`, profiled {metrics['profiled_at']} by "
        f"`src/02_profile.py` on Spark {metrics['spark_version']}. "
        f"Median and p99 are Spark SQL `percentile` (exact, with interpolation). "
        f"Session stats exclude null `user_session` values. "
        f"User stats exclude null `user_id` values."
    )
    lines.append("")
    lines.append("## Population")
    lines.append("")
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Events | {fmt_int(rows)} |")
    lines.append(f"| First event_date | {metrics['min_date']} |")
    lines.append(f"| Last event_date | {metrics['max_date']} |")
    lines.append(f"| Distinct event_date | {fmt_int(metrics['n_dates'])} |")
    lines.append(f"| Distinct user_id | {fmt_int(metrics['sessions']['user_dist']['n'])} |")
    lines.append(
        f"| Distinct user_session | {fmt_int(metrics['sessions']['event_dist']['n'])} |"
    )
    lines.append(f"| Distinct product_id | {fmt_int(metrics['cardinality']['products'])} |")
    lines.append(f"| Distinct category_id | {fmt_int(metrics['cardinality']['category_ids'])} |")
    lines.append(
        f"| Distinct category_code | {fmt_int(metrics['cardinality']['category_codes'])} |"
    )
    lines.append(f"| Distinct brand | {fmt_int(metrics['cardinality']['brands'])} |")
    lines.append("")
    lines.append(interpret_population(metrics))
    lines.append("")
    lines.append("## Event type mix")
    lines.append("")
    lines.append("| event_type | Events | Share |")
    lines.append("| --- | --- | --- |")
    for name, key in (
        ("view", "events_view"),
        ("cart", "events_cart"),
        ("purchase", "events_purchase"),
        ("other", "events_other"),
    ):
        lines.append(f"| {name} | {fmt_int(metrics[key])} | {fmt_pct(metrics[key], rows)} |")
    lines.append("")
    lines.append(interpret_events(metrics))
    lines.append("")
    lines.append("## Null rates")
    lines.append("")
    lines.append("| Column | Null rows | Null rate | Empty-string rows |")
    lines.append("| --- | --- | --- | --- |")
    for column in PROFILE_COLUMNS:
        nulls = metrics["nulls"][column]
        empty = metrics["empties"].get(column)
        empty_text = "n/a" if empty is None else fmt_int(empty)
        lines.append(
            f"| {column} | {fmt_int(nulls)} | {fmt_pct(nulls, rows)} | {empty_text} |"
        )
    lines.append("")
    lines.append(interpret_nulls(metrics))
    lines.append("")
    lines.append("## Price")
    lines.append("")
    lines.append("Median, mean, and p99 are computed on non-null prices.")
    lines.append("")
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Min | {fmt_price(metrics['price_min'])} |")
    lines.append(f"| Median | {fmt_price(metrics['price_median'])} |")
    lines.append(f"| Mean | {fmt_price(metrics['price_mean'])} |")
    lines.append(f"| p99 | {fmt_price(metrics['price_p99'])} |")
    lines.append(f"| Max | {fmt_price(metrics['price_max'])} |")
    lines.append(f"| Null price rows | {fmt_int(metrics['nulls']['price'])} |")
    lines.append(f"| Price < 0 | {fmt_int(metrics['price_negative'])} |")
    lines.append(f"| Price = 0 | {fmt_int(metrics['price_zero'])} |")
    lines.append(f"| Price > 0 | {fmt_int(metrics['price_positive'])} |")
    lines.append("")
    lines.append(interpret_price(metrics))
    lines.append("")
    if metrics["price_by_type"]:
        lines.append("| event_type | Events with price <= 0 | Min price | Max price |")
        lines.append("| --- | --- | --- | --- |")
        for row in metrics["price_by_type"]:
            lines.append(
                f"| {row['event_type']} | {fmt_int(row['rows'])} | "
                f"{fmt_price(row['min_price'])} | {fmt_price(row['max_price'])} |"
            )
        lines.append("")
        lines.append(interpret_non_positive(metrics))
        lines.append("")
    lines.append("## Exact duplicates")
    lines.append("")
    lines.append(
        "A duplicate group is a set of rows equal on `event_time`, `event_type`, "
        "`product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, "
        "and `user_session`. Extra rows are the copies a `dropDuplicates` on those "
        "columns would remove (`sum(count - 1)` over groups with `count > 1`)."
    )
    lines.append("")
    duplicates = metrics["duplicates"]
    distinct_rows = rows - duplicates["extra_rows"]
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Duplicate groups | {fmt_int(duplicates['duplicate_groups'])} |")
    lines.append(
        f"| Rows in those groups | {fmt_int(duplicates['rows_in_duplicate_groups'])} |"
    )
    lines.append(f"| Extra rows | {fmt_int(duplicates['extra_rows'])} |")
    lines.append(f"| Distinct source rows | {fmt_int(distinct_rows)} |")
    lines.append(f"| Extra rows as a share of events | {fmt_pct(duplicates['extra_rows'], rows)} |")
    lines.append("")
    lines.append(interpret_duplicates(metrics))
    lines.append("")
    lines.append("## Category code shape")
    lines.append("")
    lines.append(
        "Depth is the number of pieces after splitting `category_code` on `.`. "
        "Missing codes are null, not depth 0. The example is the most common code "
        "at that depth."
    )
    lines.append("")
    lines.append("| Depth | Rows | Distinct codes | Empty-token rows | Most common code |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in metrics["categories"]:
        example = row["example"] if row["example"] else "—"
        lines.append(
            f"| {row['depth']} | {fmt_int(row['rows'])} | {fmt_int(row['codes'])} | "
            f"{fmt_int(row['empty_token_rows'])} | {example} |"
        )
    lines.append("")
    lines.append(interpret_categories(metrics))
    lines.append("")
    lines.append("## Brand text")
    lines.append("")
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Null brand rows | {fmt_int(metrics['nulls']['brand'])} |")
    lines.append(f"| Empty-string brand rows | {fmt_int(metrics['empties']['brand'])} |")
    lines.append(f"| Rows with surrounding whitespace | {fmt_int(metrics['brand_untrimmed'])} |")
    lines.append(
        f"| Rows already trimmed but not lowercase | {fmt_int(metrics['brand_mixed_case'])} |"
    )
    lines.append(
        f"| Rows that lowercase+trim would change | {fmt_int(metrics['brand_needs_normalising'])} |"
    )
    lines.append(f"| Distinct brand strings | {fmt_int(metrics['cardinality']['brands'])} |")
    lines.append(
        "| Distinct brand strings after lowercase+trim | "
        f"{fmt_int(metrics['cardinality']['brands_normalised'])} |"
    )
    lines.append("")
    lines.append(interpret_brands(metrics))
    lines.append("")
    lines.append("## Sessions per user")
    lines.append("")
    lines.append(
        "A user's session count is the number of distinct non-null `user_session` "
        "values on that `user_id`. A count of 0 means every event for that user "
        "has a null session. Bucket edges are fixed cuts."
    )
    lines.append("")
    user_dist = metrics["sessions"]["user_dist"]
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Users | {fmt_int(user_dist['n'])} |")
    lines.append(f"| Min sessions | {fmt_count_stat(user_dist['min'])} |")
    lines.append(f"| Median sessions | {fmt_count_stat(user_dist['median'])} |")
    lines.append(f"| Mean sessions | {user_dist['mean']:.4f} |")
    lines.append(f"| p99 sessions | {fmt_count_stat(user_dist['p99'])} |")
    lines.append(f"| Max sessions | {fmt_count_stat(user_dist['max'])} |")
    lines.append("")
    lines.append("| Sessions per user | Users | Share of users |")
    lines.append("| --- | --- | --- |")
    for bucket in metrics["sessions"]["user_buckets"]:
        lines.append(
            f"| {bucket['bucket']} | {fmt_int(bucket['n'])} | "
            f"{fmt_pct(bucket['n'], user_dist['n'])} |"
        )
    lines.append("")
    lines.append(interpret_users(metrics))
    lines.append("")
    lines.append("## Events per session")
    lines.append("")
    lines.append("Null `user_session` rows are excluded from this table and counted below.")
    lines.append("")
    event_dist = metrics["sessions"]["event_dist"]
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Sessions | {fmt_int(event_dist['n'])} |")
    lines.append(f"| Events in those sessions | {fmt_int(event_dist['total'])} |")
    lines.append(f"| Null user_session rows excluded | {fmt_int(metrics['nulls']['user_session'])} |")
    lines.append(f"| Min events | {fmt_count_stat(event_dist['min'])} |")
    lines.append(f"| Median events | {fmt_count_stat(event_dist['median'])} |")
    lines.append(f"| Mean events | {event_dist['mean']:.4f} |")
    lines.append(f"| p99 events | {fmt_count_stat(event_dist['p99'])} |")
    lines.append(f"| Max events | {fmt_count_stat(event_dist['max'])} |")
    lines.append(f"| Sessions with more than one user_id | {fmt_int(metrics['sessions']['sessions_multi_user'])} |")
    lines.append(
        f"| Sessions with no user_id | {fmt_int(metrics['sessions']['sessions_without_user'])} |"
    )
    lines.append("")
    lines.append("| Events per session | Sessions | Share of sessions |")
    lines.append("| --- | --- | --- |")
    for bucket in metrics["sessions"]["event_buckets"]:
        lines.append(
            f"| {bucket['bucket']} | {fmt_int(bucket['n'])} | "
            f"{fmt_pct(bucket['n'], event_dist['n'])} |"
        )
    lines.append("")
    lines.append(interpret_session_length(metrics))
    lines.append("")
    lines.append("## Sessions that contain a purchase")
    lines.append("")
    lines.append("| Measure | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Sessions | {fmt_int(event_dist['n'])} |")
    lines.append(
        f"| Sessions with at least one purchase | "
        f"{fmt_int(metrics['sessions']['sessions_with_purchase'])} |"
    )
    lines.append(
        f"| Share of sessions | "
        f"{fmt_pct(metrics['sessions']['sessions_with_purchase'], event_dist['n'])} |"
    )
    lines.append(f"| Purchase events | {fmt_int(metrics['events_purchase'])} |")
    lines.append("")
    lines.append(interpret_purchase_sessions(metrics))
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Read the profile CLI: bronze path, markdown path, memory, and master."""
    parser = argparse.ArgumentParser(description="Profile the bronze events table.")
    parser.add_argument("--bronze", type=Path, default=DEFAULT_BRONZE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--driver-memory",
        default=os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY),
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER", DEFAULT_MASTER),
    )
    return parser.parse_args(argv)


def _check_session_accounting(metrics: dict) -> None:
    """Events in the session grain plus null session rows must equal all rows."""
    in_sessions = metrics["sessions"]["event_dist"]["total"]
    excluded = metrics["nulls"]["user_session"]
    if in_sessions + excluded != metrics["rows"]:
        logger.error(
            "Session events %s plus null sessions %s != rows %s",
            in_sessions,
            excluded,
            metrics["rows"],
        )
        sys.exit(1)
    duplicates = metrics["duplicates"]
    if duplicates["extra_rows"] != (
        duplicates["rows_in_duplicate_groups"] - duplicates["duplicate_groups"]
    ):
        logger.error("Duplicate arithmetic is inconsistent: %s", duplicates)
        sys.exit(1)
    bucket_users = sum(bucket["n"] for bucket in metrics["sessions"]["user_buckets"])
    if bucket_users != metrics["sessions"]["user_dist"]["n"]:
        logger.error("User buckets sum to %s", bucket_users)
        sys.exit(1)
    bucket_sessions = sum(bucket["n"] for bucket in metrics["sessions"]["event_buckets"])
    if bucket_sessions != metrics["sessions"]["event_dist"]["n"]:
        logger.error("Session buckets sum to %s", bucket_sessions)
        sys.exit(1)
    non_positive = sum(row["rows"] for row in metrics["price_by_type"])
    expected_non_positive = metrics["price_negative"] + metrics["price_zero"]
    if non_positive != expected_non_positive:
        logger.error(
            "Non-positive breakdown sums to %s, expected %s",
            non_positive,
            expected_non_positive,
        )
        sys.exit(1)
    category_rows = sum(row["rows"] for row in metrics["categories"])
    if category_rows != metrics["rows"]:
        logger.error("Category depth rows sum to %s", category_rows)
        sys.exit(1)


def main(argv: list[str] | None = None) -> None:
    """Profile bronze and overwrite docs/data_profile.md."""
    args = parse_args(argv)
    bronze = args.bronze.resolve()
    output = args.output.resolve()
    assert_safe_output(output)
    started = time.perf_counter()
    spark = build_spark(args.driver_memory, args.master)
    try:
        events = load_events(spark, bronze)
        events.createOrReplaceTempView("bronze_events")

        logger.info("Profiling column nulls, prices, and event mix")
        base = column_and_price_stats(events)
        logger.info("PROFILE_ROWS rows=%s", base["rows"])
        logger.info(
            "PROFILE_PRICE min=%s median=%s mean=%s p99=%s max=%s negative=%s zero=%s",
            base["price_min"],
            base["price_median"],
            base["price_mean"],
            base["price_p99"],
            base["price_max"],
            base["price_negative"],
            base["price_zero"],
        )

        logger.info("Profiling non-positive prices by event type")
        price_by_type = non_positive_prices(events)
        for row in price_by_type:
            logger.info(
                "PROFILE_NON_POSITIVE event_type=%s rows=%s min=%s max=%s",
                row["event_type"],
                row["rows"],
                row["min_price"],
                row["max_price"],
            )

        logger.info("Profiling cardinality")
        cards = cardinality_stats(events)
        logger.info("PROFILE_CARDINALITY %s", cards)

        logger.info("Profiling category-code depth")
        categories = category_shape(events)
        for row in categories:
            logger.info(
                "PROFILE_CATEGORY_DEPTH depth=%s rows=%s codes=%s empty_tokens=%s",
                row["depth"],
                row["rows"],
                row["codes"],
                row["empty_token_rows"],
            )

        logger.info("Profiling exact duplicates")
        duplicates = duplicate_stats(spark)
        logger.info(
            "PROFILE_DUPLICATES groups=%s rows_in_groups=%s extra_rows=%s",
            duplicates["duplicate_groups"],
            duplicates["rows_in_duplicate_groups"],
            duplicates["extra_rows"],
        )

        logger.info("Profiling sessions and users")
        sessions = session_and_user_stats(events)
        logger.info(
            "PROFILE_SESSIONS sessions=%s with_purchase=%s multi_user=%s",
            sessions["event_dist"]["n"],
            sessions["sessions_with_purchase"],
            sessions["sessions_multi_user"],
        )
        logger.info(
            "PROFILE_USERS users=%s median_sessions=%s max_sessions=%s",
            sessions["user_dist"]["n"],
            sessions["user_dist"]["median"],
            sessions["user_dist"]["max"],
        )
        logger.info(
            "PROFILE_SESSION_LENGTH median_events=%s p99_events=%s max_events=%s",
            sessions["event_dist"]["median"],
            sessions["event_dist"]["p99"],
            sessions["event_dist"]["max"],
        )

        metrics = {
            **base,
            "cardinality": cards,
            "categories": categories,
            "duplicates": duplicates,
            "sessions": sessions,
            "price_by_type": price_by_type,
            "spark_version": spark.version,
            "profiled_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        _check_session_accounting(metrics)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_markdown(metrics))
        logger.info("Wrote %s", output)
    finally:
        spark.stop()
    logger.info("PROFILE_ELAPSED seconds=%.1f", time.perf_counter() - started)
    logger.info("Bronze profile passed")


if __name__ == "__main__":
    main()
