"""Export small dashboard aggregates from the gold tables.

DuckDB reads gold Parquet and writes dashboard/data. The files are the only
data this project commits. Definitions match sql/analysis.sql: the script
loads that file's views and macros, so price bands and RFM labels stay in
one place.

Revenue is purchase price where is_non_positive_price is false. Category is
dim_product. Brand is fact_events.brand. Hours are UTC.

Distinct users cannot be summed across days. daily_totals and month_totals
store exact distinct counts for one day and for the whole month. The app
uses those rows and labels a partial date range as an average of daily users.

Brand rows keep the top 12 brands inside each category, always keep
"unknown", and roll the rest into "other". "All" is a pre-aggregated rollup
and is not the sum of the category rows' "other" buckets.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "dashboard" / "data"
DATA_ROOT = PROJECT_ROOT / "data"
RAW_ROOT = DATA_ROOT / "raw"
SQL_PATH = PROJECT_ROOT / "sql" / "analysis.sql"
ANALYSIS_PATH = PROJECT_ROOT / "src" / "05_run_analysis.py"

EXPECTED_REVENUE = 229_933_212.63
EXPECTED_PURCHASES = 742_773
EXPECTED_VIEWS = 40_777_328
EXPECTED_CARTS = 898_441
EXPECTED_EVENTS = 42_418_542
EXPECTED_USERS = 3_022_290
EXPECTED_BUYERS = 347_118
EXPECTED_CART_PAIRS = 628_207
EXPECTED_ABANDONED_PAIRS = 323_267
EXPECTED_ABANDONED_VALUE = 107_866_053.25
EXPECTED_PURCHASE_NO_CART = 337_699
EXPECTED_HOUR0_OCT1 = 1_083
EXPECTED_OCT16_REVENUE = 9_747_164.72
EXPECTED_ELECTRONICS_REVENUE = 176_445_577.86
EXPECTED_ELECTRONICS_PURCHASES = 422_979
EXPECTED_ELECTRONICS_BUYERS = 200_031
REVENUE_TOLERANCE = 0.05
MAX_BYTES = 50 * 1000 * 1000

TABLES = (
    "daily_category",
    "daily_totals",
    "month_category",
    "month_totals",
    "category_l2",
    "brands",
    "hourly",
    "price_bands",
    "abandonment",
    "rfm_segments",
    "rfm_score_bounds",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("export_dashboard")


def assert_safe_output(path: Path) -> None:
    resolved = path.resolve()
    raw = RAW_ROOT.resolve()
    data_root = DATA_ROOT.resolve()
    expected = DEFAULT_OUTPUT.resolve()
    if resolved == raw or raw in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, raw)
        sys.exit(1)
    if resolved == data_root or data_root in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, data_root)
        sys.exit(1)
    if resolved != expected:
        logger.error("Dashboard extracts must be written to %s, got %s", expected, resolved)
        sys.exit(1)


def load_analysis():
    spec = importlib.util.spec_from_file_location("run_analysis", ANALYSIS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def connect() -> duckdb.DuckDBPyConnection:
    os.chdir(PROJECT_ROOT)
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET memory_limit = '8GB'")
    connection.execute("SET threads = 6")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def fetch(connection: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return connection.execute(sql).fetchdf()


def prepare_connection(connection: duckdb.DuckDBPyConnection, analysis) -> None:
    sql_text = SQL_PATH.read_text()
    prelude, queries = analysis.parse_script(sql_text)
    for statement in analysis.statements(prelude):
        connection.execute(statement)
    timezone = connection.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    if timezone != "UTC":
        logger.error("TimeZone is %s", timezone)
        sys.exit(1)
    for name, _title, body in queries:
        if name != "build_buyer_rfm":
            continue
        for statement in analysis.statements(body):
            connection.execute(statement)
        return
    logger.error("build_buyer_rfm is missing from sql/analysis.sql")
    sys.exit(1)


def build_frames(connection: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    daily_category = fetch(
        connection,
        """
        WITH by_cat AS (
          SELECT
            f.event_date,
            p.category_l1,
            count(*) FILTER (WHERE f.event_type = 'view') AS views,
            count(*) FILTER (WHERE f.event_type = 'cart') AS carts,
            count(*) FILTER (
              WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
            ) AS purchases,
            coalesce(sum(f.price) FILTER (
              WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
            ), 0) AS revenue,
            count(DISTINCT f.user_id) AS users,
            count(DISTINCT f.user_id) FILTER (
              WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
            ) AS buyers,
            count(DISTINCT f.user_session) AS sessions
          FROM fact_events AS f
          JOIN dim_product AS p ON f.product_id = p.product_id
          GROUP BY f.event_date, p.category_l1
        ),
        spine AS (
          SELECT d.event_date, d.day_name, d.day_of_week, d.is_weekend, c.category_l1
          FROM dim_date AS d
          CROSS JOIN (SELECT DISTINCT category_l1 FROM dim_product) AS c
        )
        SELECT
          s.event_date,
          s.day_name,
          s.day_of_week,
          s.is_weekend,
          s.category_l1,
          coalesce(b.views, 0) AS views,
          coalesce(b.carts, 0) AS carts,
          coalesce(b.purchases, 0) AS purchases,
          coalesce(b.revenue, 0) AS revenue,
          coalesce(b.users, 0) AS users,
          coalesce(b.buyers, 0) AS buyers,
          coalesce(b.sessions, 0) AS sessions
        FROM spine AS s
        LEFT JOIN by_cat AS b
          ON s.event_date = b.event_date AND s.category_l1 = b.category_l1
        ORDER BY s.category_l1, s.event_date
        """,
    )
    daily_totals = fetch(
        connection,
        """
        WITH by_day AS (
          SELECT
            event_date,
            count(*) FILTER (WHERE event_type = 'view') AS views,
            count(*) FILTER (WHERE event_type = 'cart') AS carts,
            count(*) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ) AS purchases,
            coalesce(sum(price) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ), 0) AS revenue,
            count(DISTINCT user_id) AS users,
            count(DISTINCT user_id) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ) AS buyers,
            count(DISTINCT user_session) AS sessions
          FROM fact_events
          GROUP BY event_date
        )
        SELECT
          d.event_date,
          d.day_name,
          d.day_of_week,
          d.is_weekend,
          coalesce(b.views, 0) AS views,
          coalesce(b.carts, 0) AS carts,
          coalesce(b.purchases, 0) AS purchases,
          coalesce(b.revenue, 0) AS revenue,
          coalesce(b.users, 0) AS users,
          coalesce(b.buyers, 0) AS buyers,
          coalesce(b.sessions, 0) AS sessions
        FROM dim_date AS d
        LEFT JOIN by_day AS b ON d.event_date = b.event_date
        ORDER BY d.event_date
        """,
    )
    month_category = fetch(
        connection,
        """
        SELECT
          p.category_l1,
          count(*) FILTER (WHERE f.event_type = 'view') AS views,
          count(*) FILTER (WHERE f.event_type = 'cart') AS carts,
          count(*) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ) AS purchases,
          coalesce(sum(f.price) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ), 0) AS revenue,
          count(DISTINCT f.user_id) AS users,
          count(DISTINCT f.user_id) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ) AS buyers
        FROM fact_events AS f
        JOIN dim_product AS p ON f.product_id = p.product_id
        GROUP BY p.category_l1
        ORDER BY revenue DESC
        """,
    )
    month_totals = fetch(
        connection,
        """
        WITH events AS (
          SELECT
            count(*) FILTER (WHERE event_type = 'view') AS views,
            count(*) FILTER (WHERE event_type = 'cart') AS carts,
            count(*) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ) AS purchases,
            coalesce(sum(price) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ), 0) AS revenue,
            count(DISTINCT user_id) AS users,
            count(DISTINCT user_id) FILTER (
              WHERE event_type = 'purchase' AND NOT is_non_positive_price
            ) AS buyers,
            count(*) AS events
          FROM fact_events
        ),
        sess AS (
          SELECT
            count(*) FILTER (WHERE n_views > 0) AS view_sessions,
            count(*) FILTER (WHERE n_views > 0 AND n_carts > 0) AS view_and_cart_sessions,
            count(*) FILTER (
              WHERE n_views > 0 AND n_carts > 0 AND n_purchases > 0
            ) AS view_cart_purchase_sessions,
            count(*) FILTER (WHERE n_purchases > 0) AS purchase_sessions,
            count(*) FILTER (WHERE n_purchases > 0 AND n_carts = 0) AS purchase_no_cart_sessions,
            count(*) FILTER (WHERE n_carts > 0) AS cart_sessions,
            count(*) FILTER (WHERE n_carts > 0 AND n_purchases = 0) AS cart_no_purchase_sessions
          FROM sessions
        )
        SELECT * FROM events CROSS JOIN sess
        """,
    )
    category_l2 = fetch(
        connection,
        """
        SELECT
          f.event_date,
          p.category_l1,
          p.category_l2,
          count(*) FILTER (WHERE f.event_type = 'view') AS views,
          count(*) FILTER (WHERE f.event_type = 'cart') AS carts,
          count(*) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ) AS purchases,
          coalesce(sum(f.price) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ), 0) AS revenue
        FROM fact_events AS f
        JOIN dim_product AS p ON f.product_id = p.product_id
        GROUP BY f.event_date, p.category_l1, p.category_l2
        """,
    )
    other_brand = connection.execute(
        "SELECT count(*) FROM fact_events WHERE brand = 'other'"
    ).fetchone()[0]
    other_label = "other brands" if other_brand else "other"
    brands = fetch(
        connection,
        f"""
        WITH daily AS (
          SELECT
            f.event_date,
            p.category_l1,
            f.brand,
            count(*) FILTER (WHERE f.event_type = 'view') AS views,
            count(*) FILTER (WHERE f.event_type = 'cart') AS carts,
            count(*) FILTER (
              WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
            ) AS purchases,
            coalesce(sum(f.price) FILTER (
              WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
            ), 0) AS revenue
          FROM fact_events AS f
          JOIN dim_product AS p ON f.product_id = p.product_id
          GROUP BY f.event_date, p.category_l1, f.brand
        ),
        unioned AS (
          SELECT * FROM daily
          UNION ALL
          SELECT event_date, 'All' AS category_l1, brand,
                 sum(views), sum(carts), sum(purchases), sum(revenue)
          FROM daily
          GROUP BY event_date, brand
        ),
        keep AS (
          SELECT category_l1, brand
          FROM (
            SELECT
              category_l1,
              brand,
              row_number() OVER (
                PARTITION BY category_l1
                ORDER BY sum(revenue) DESC, brand ASC
              ) AS rn
            FROM unioned
            GROUP BY category_l1, brand
          )
          WHERE rn <= 12 OR brand = 'unknown'
        )
        SELECT
          u.event_date,
          u.category_l1,
          CASE WHEN k.brand IS NULL THEN '{other_label}' ELSE u.brand END AS brand,
          sum(u.views) AS views,
          sum(u.carts) AS carts,
          sum(u.purchases) AS purchases,
          sum(u.revenue) AS revenue
        FROM unioned AS u
        LEFT JOIN keep AS k
          ON u.category_l1 = k.category_l1 AND u.brand = k.brand
        GROUP BY u.event_date, u.category_l1, 3
        """,
    )
    hourly = fetch(
        connection,
        """
        SELECT
          f.event_date,
          d.day_of_week,
          d.day_name,
          d.is_weekend,
          hour(f.event_time) AS hour_utc,
          p.category_l1,
          f.event_type,
          count(*) AS events,
          coalesce(sum(f.price) FILTER (
            WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          ), 0) AS revenue
        FROM fact_events AS f
        JOIN dim_product AS p ON f.product_id = p.product_id
        JOIN dim_date AS d ON f.event_date = d.event_date
        GROUP BY
          f.event_date, d.day_of_week, d.day_name, d.is_weekend,
          hour(f.event_time), p.category_l1, f.event_type
        """,
    )
    price_bands = fetch(
        connection,
        """
        WITH raw AS (
          SELECT
            f.event_date,
            p.category_l1,
            CASE
              WHEN f.price <= 0 THEN '00_non_positive'
              ELSE price_band(f.price)
            END AS band_key,
            count(*) AS purchases,
            sum(f.price) AS revenue
          FROM fact_events AS f
          JOIN dim_product AS p ON f.product_id = p.product_id
          WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
          GROUP BY 1, 2, 3
        )
        SELECT
          event_date,
          category_l1,
          band_key,
          price_band_label(band_key) AS band_label,
          purchases,
          revenue
        FROM raw
        """,
    )
    abandonment = fetch(
        connection,
        """
        WITH carted AS (
          SELECT
            user_session,
            product_id,
            max(price) AS cart_price,
            min(event_date) AS event_date
          FROM fact_events
          WHERE event_type = 'cart'
          GROUP BY user_session, product_id
        ),
        purchased AS (
          SELECT DISTINCT user_session, product_id
          FROM fact_events
          WHERE event_type = 'purchase'
        ),
        pairs AS (
          SELECT
            carted.event_date,
            product.category_l1,
            CASE
              WHEN carted.cart_price <= 0 THEN '00_non_positive'
              ELSE price_band(carted.cart_price)
            END AS band_key,
            carted.cart_price,
            purchased.product_id IS NULL AS abandoned
          FROM carted
          LEFT JOIN purchased
            ON carted.user_session = purchased.user_session
           AND carted.product_id = purchased.product_id
          JOIN dim_product AS product ON carted.product_id = product.product_id
        )
        SELECT
          event_date,
          category_l1,
          band_key,
          price_band_label(band_key) AS band_label,
          count(*) AS cart_pairs,
          count(*) FILTER (WHERE abandoned) AS abandoned_pairs,
          coalesce(sum(cart_price) FILTER (WHERE abandoned), 0) AS abandoned_cart_value
        FROM pairs
        GROUP BY event_date, category_l1, band_key
        """,
    )
    rfm_segments = fetch(
        connection,
        """
        SELECT
          segment,
          count(*) AS buyers,
          sum(monetary) AS revenue,
          avg(recency_days) AS avg_recency_days,
          min(recency_days) AS min_recency_days,
          max(recency_days) AS max_recency_days,
          avg(purchase_sessions) AS avg_purchase_sessions,
          min(purchase_sessions) AS min_purchase_sessions,
          max(purchase_sessions) AS max_purchase_sessions,
          avg(monetary) AS avg_monetary
        FROM buyer_rfm
        GROUP BY segment
        ORDER BY revenue DESC
        """,
    )
    rfm_score_bounds = fetch(
        connection,
        """
        SELECT 'recency_days' AS metric, r_score AS score, count(*) AS buyers,
               min(recency_days) AS min_value, max(recency_days) AS max_value
        FROM buyer_rfm GROUP BY r_score
        UNION ALL
        SELECT 'purchase_sessions', f_score, count(*),
               min(purchase_sessions), max(purchase_sessions)
        FROM buyer_rfm GROUP BY f_score
        UNION ALL
        SELECT 'monetary', m_score, count(*),
               min(monetary), max(monetary)
        FROM buyer_rfm GROUP BY m_score
        ORDER BY metric, score
        """,
    )
    return {
        "daily_category": daily_category,
        "daily_totals": daily_totals,
        "month_category": month_category,
        "month_totals": month_totals,
        "category_l2": category_l2,
        "brands": brands,
        "hourly": hourly,
        "price_bands": price_bands,
        "abandonment": abandonment,
        "rfm_segments": rfm_segments,
        "rfm_score_bounds": rfm_score_bounds,
    }


def require(condition: bool, message: str) -> None:
    if not condition:
        logger.error("Check failed: %s", message)
        sys.exit(1)


def close(value: float, expected: float, label: str) -> None:
    gap = abs(float(value) - expected)
    require(gap <= REVENUE_TOLERANCE, f"{label} {value} expected {expected} gap {gap}")


def validate(frames: dict[str, pd.DataFrame], connection: duckdb.DuckDBPyConnection) -> None:
    totals = frames["month_totals"].iloc[0]
    close(totals["revenue"], EXPECTED_REVENUE, "month revenue")
    require(int(totals["purchases"]) == EXPECTED_PURCHASES, "purchases")
    require(int(totals["views"]) == EXPECTED_VIEWS, "views")
    require(int(totals["carts"]) == EXPECTED_CARTS, "carts")
    require(int(totals["events"]) == EXPECTED_EVENTS, "events")
    require(int(totals["users"]) == EXPECTED_USERS, "users")
    require(int(totals["buyers"]) == EXPECTED_BUYERS, "buyers")
    require(int(totals["purchase_no_cart_sessions"]) == EXPECTED_PURCHASE_NO_CART, "purchase no cart")

    daily = frames["daily_totals"]
    close(daily["revenue"].sum(), EXPECTED_REVENUE, "daily revenue")
    require(int(daily["purchases"].sum()) == EXPECTED_PURCHASES, "daily purchases")
    require(len(daily) == 31, "daily rows")
    oct16 = daily.loc[pd.to_datetime(daily["event_date"]).dt.date.astype(str) == "2019-10-16", "revenue"]
    require(len(oct16) == 1, "16 October row")
    close(oct16.iloc[0], EXPECTED_OCT16_REVENUE, "16 October revenue")

    by_cat = frames["daily_category"]
    close(by_cat["revenue"].sum(), EXPECTED_REVENUE, "category-day revenue")
    electronics = frames["month_category"].loc[
        frames["month_category"]["category_l1"] == "electronics"
    ].iloc[0]
    close(electronics["revenue"], EXPECTED_ELECTRONICS_REVENUE, "electronics revenue")
    require(int(electronics["purchases"]) == EXPECTED_ELECTRONICS_PURCHASES, "electronics purchases")
    require(int(electronics["buyers"]) == EXPECTED_ELECTRONICS_BUYERS, "electronics buyers")

    close(frames["category_l2"]["revenue"].sum(), EXPECTED_REVENUE, "l2 revenue")
    all_brands = frames["brands"].loc[frames["brands"]["category_l1"] == "All", "revenue"].sum()
    close(all_brands, EXPECTED_REVENUE, "brand rollup revenue")
    close(frames["price_bands"]["revenue"].sum(), EXPECTED_REVENUE, "price band revenue")
    close(frames["hourly"]["revenue"].sum(), EXPECTED_REVENUE, "hourly revenue")
    require(int(frames["hourly"]["events"].sum()) == EXPECTED_EVENTS, "hourly events")

    hour0 = connection.execute(
        """
        SELECT count(*)
        FROM fact_events
        WHERE event_date = DATE '2019-10-01' AND hour(event_time) = 0
        """
    ).fetchone()[0]
    require(int(hour0) == EXPECTED_HOUR0_OCT1, f"UTC hour 0 on 1 October is {hour0}")

    abandon = frames["abandonment"]
    require(int(abandon["cart_pairs"].sum()) == EXPECTED_CART_PAIRS, "cart pairs")
    require(int(abandon["abandoned_pairs"].sum()) == EXPECTED_ABANDONED_PAIRS, "abandoned pairs")
    close(abandon["abandoned_cart_value"].sum(), EXPECTED_ABANDONED_VALUE, "abandoned value")

    rfm = frames["rfm_segments"]
    require(int(rfm["buyers"].sum()) == EXPECTED_BUYERS, "rfm buyers")
    close(rfm["revenue"].sum(), EXPECTED_REVENUE, "rfm revenue")
    require(set(rfm["segment"]) >= {"champions", "loyal repeaters", "big spenders"}, "rfm segments")
    freq = frames["rfm_score_bounds"]
    freq = freq.loc[freq["metric"] == "purchase_sessions"]
    ones = freq.loc[freq["score"].isin([1, 2, 3])]
    require(len(ones) == 3, "frequency tiles")
    require(float(ones["min_value"].min()) == 1 and float(ones["max_value"].max()) == 1, "tiles 1-3")
    logger.info(
        "DASHBOARD_VALIDATED revenue=%.2f purchases=%s users=%s",
        float(totals["revenue"]),
        int(totals["purchases"]),
        int(totals["users"]),
    )


def write_frames(frames: dict[str, pd.DataFrame], output: Path) -> int:
    output.mkdir(parents=True, exist_ok=True)
    for stale in output.glob("*.parquet"):
        stale.unlink()
    for name in TABLES:
        frames[name].to_parquet(output / f"{name}.parquet", index=False, compression="zstd")
        logger.info("WROTE %s rows=%s", name, len(frames[name]))
    total = sum(path.stat().st_size for path in output.glob("*.parquet"))
    logger.info("DASHBOARD_BYTES %s", total)
    if total > MAX_BYTES:
        logger.error("dashboard/data is %s bytes, above the 50 MB cap", total)
        sys.exit(1)
    return total


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export dashboard aggregates from gold.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    output = args.output.resolve()
    assert_safe_output(output)
    if not SQL_PATH.exists():
        logger.error("Missing %s", SQL_PATH)
        sys.exit(1)
    started = time.perf_counter()
    analysis = load_analysis()
    connection = connect()
    prepare_connection(connection, analysis)
    frames = build_frames(connection)
    validate(frames, connection)
    total = write_frames(frames, output)
    # Read the written revenue back so a bad parquet write fails the job.
    written = pd.read_parquet(output / "month_totals.parquet")
    close(written["revenue"].iloc[0], EXPECTED_REVENUE, "written revenue")
    connection.close()
    logger.info("EXPORT_ELAPSED seconds=%.1f bytes=%s", time.perf_counter() - started, total)


if __name__ == "__main__":
    main()
