"""Run sql/analysis.sql against the gold Parquet tables and write the results.

DuckDB reads the gold tables directly. The connection time zone is UTC, so
hour(event_time) is the UTC hour stored on the fact. Revenue, brand, and
category follow the gold model: eligible purchase price, the event brand, and
dim_product for category.

The script overwrites docs/analysis_results.md only after the reconciliation
checks pass. It does not write under data/.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL = PROJECT_ROOT / "sql" / "analysis.sql"
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "analysis_results.md"
DATA_ROOT = PROJECT_ROOT / "data"

# Totals locked by the Phase 5 gold validation.
EXPECTED_FACT_ROWS = 42_418_542
EXPECTED_PURCHASES = 742_773
EXPECTED_SESSIONS = 9_244_421
EXPECTED_REVENUE = 229_933_212.63
# 2019-10-01 00:00:00 UTC. Confirms TimeZone=UTC did not shift the instant.
EXPECTED_MIN_EPOCH = 1_569_888_000
REVENUE_TOLERANCE = 0.01

NAME_RE = re.compile(r"(?m)^-- name: ([a-z0-9_]+)\s*$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("run_analysis")


def assert_safe_output(path: Path) -> None:
    resolved = path.resolve()
    data_root = DATA_ROOT.resolve()
    if resolved == data_root or data_root in resolved.parents:
        logger.error("Refusing to write %s inside %s", resolved, data_root)
        sys.exit(1)


def statements(sql: str) -> list[str]:
    """Split on semicolons. Comment-only chunks are dropped."""
    chunks = []
    for part in sql.split(";"):
        lines = []
        for line in part.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("--"):
                lines.append(line)
        if lines:
            chunks.append(part.strip())
    return chunks


def parse_script(sql: str) -> tuple[str, list[tuple[str, str, str]]]:
    """Return the prelude and (name, title, body) for each named query."""
    parts = NAME_RE.split(sql)
    prelude = parts[0]
    queries = []
    iterator = iter(parts[1:])
    for name, body in zip(iterator, iterator):
        title = name
        for line in body.splitlines():
            stripped = line.strip()
            if stripped.startswith("-- "):
                title = stripped[3:].strip()
                break
            if stripped and not stripped.startswith("--"):
                break
        queries.append((name, title, body.strip()))
    if not queries:
        logger.error("No named queries in the SQL file")
        sys.exit(1)
    return prelude, queries


def connect() -> duckdb.DuckDBPyConnection:
    os.chdir(PROJECT_ROOT)
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    return connection


def execute(connection: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame | None:
    connection.execute(sql)
    if connection.description is None:
        return None
    return connection.fetchdf()


def run_self_test(sql_text: str) -> None:
    """Pin price bands, segment priority, and the recency NTILE direction."""
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")
    for statement in statements(sql_text):
        code = ""
        for line in statement.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("--"):
                code = stripped.upper()
                break
        if code.startswith("CREATE OR REPLACE MACRO"):
            connection.execute(statement)
    bands = connection.execute(
        """
        SELECT
          price_band(49.99), price_band(50), price_band(149.99),
          price_band(150), price_band(299.99), price_band(300),
          price_band(599.99), price_band(600), price_band(999.99),
          price_band(1000), price_band(2574.07),
          price_band_label('01_under_50')
        """
    ).fetchone()
    expected_bands = (
        "01_under_50",
        "02_50_to_150",
        "02_50_to_150",
        "03_150_to_300",
        "03_150_to_300",
        "04_300_to_600",
        "04_300_to_600",
        "05_600_to_1000",
        "05_600_to_1000",
        "06_1000_and_above",
        "06_1000_and_above",
        "under 50",
    )
    if tuple(bands) != expected_bands:
        logger.error("Self-test price bands failed: %s", bands)
        sys.exit(1)
    segments = connection.execute(
        """
        SELECT
          rfm_segment(5, 5, 3),
          rfm_segment(5, 5, 1),
          rfm_segment(5, 3, 1),
          rfm_segment(1, 3, 1),
          rfm_segment(1, 2, 2),
          rfm_segment(4, 4, 2),
          rfm_segment(5, 3, 3),
          rfm_segment(3, 2, 1)
        """
    ).fetchone()
    expected_segments = (
        "champions",
        "big spenders",
        "recent one-time",
        "early one-time",
        "need attention",
        "loyal repeaters",
        "loyal repeaters",
        "other",
    )
    if tuple(segments) != expected_segments:
        logger.error("Self-test segments failed: %s", segments)
        sys.exit(1)
    scores = connection.execute(
        """
        SELECT user_id, 6 - ntile(5) OVER (ORDER BY recency ASC, user_id ASC) AS r_score
        FROM (VALUES (1, 0), (2, 1), (3, 2), (4, 3), (5, 4)) AS t(user_id, recency)
        ORDER BY user_id
        """
    ).fetchall()
    if scores[0][1] != 5 or scores[-1][1] != 1:
        logger.error("Self-test recency NTILE failed: %s", scores)
        sys.exit(1)
    connection.close()
    logger.info("Self-test passed")


def _scalar(value: object) -> object:
    if value is None:
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except ValueError:
            return value
    return value


def _missing(value: object) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except TypeError:
        return False
    return bool(missing)


def format_value(column: str, value: object) -> str:
    value = _scalar(value)
    if _missing(value):
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    name = column.lower()
    if isinstance(value, float):
        money = any(token in name for token in ("revenue", "aov", "value", "monetary", "price", "ma7"))
        if money and "share" not in name:
            return f"{value:,.2f}"
        if (
            "gap" in name
            or "share" in name
            or name == "rate"
            or name.endswith("_rate")
            or "per_" in name
        ):
            return f"{value:.6f}"
        if abs(value - round(value)) < 1e-9:
            return f"{int(round(value)):,}"
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text


def to_markdown(frame: pd.DataFrame) -> str:
    columns = [str(column) for column in frame.columns]
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in frame.itertuples(index=False):
        cells = [format_value(column, value) for column, value in zip(columns, row)]
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([header, separator, *body])


def as_float(value: object) -> float:
    return float(_scalar(value))


def as_int(value: object) -> int:
    return int(_scalar(value))


def stage_sessions(frame: pd.DataFrame, stage: str) -> int:
    hit = frame.loc[frame["stage"] == stage, "sessions"]
    if len(hit) != 1:
        logger.error("Funnel stage %s not found once", stage)
        sys.exit(1)
    return as_int(hit.iloc[0])


def require(condition: bool, message: str) -> None:
    if not condition:
        logger.error("Check failed: %s", message)
        sys.exit(1)


def validate(frames: dict[str, pd.DataFrame], min_epoch: int) -> None:
    """Fail the run when a result disagrees with the gold contract or itself."""
    require(min_epoch == EXPECTED_MIN_EPOCH, f"min event epoch {min_epoch}")
    kpis = frames["q01_kpis"].iloc[0]
    require(as_int(kpis["events"]) == EXPECTED_FACT_ROWS, "KPI event rows")
    require(as_int(kpis["purchases"]) == EXPECTED_PURCHASES, "KPI purchases")
    require(as_int(kpis["sessions"]) == EXPECTED_SESSIONS, "KPI sessions")
    require(as_int(kpis["fact_sessions"]) == EXPECTED_SESSIONS, "fact session distinct")
    require(
        abs(as_float(kpis["revenue"]) - EXPECTED_REVENUE) <= REVENUE_TOLERANCE,
        f"KPI revenue {kpis['revenue']}",
    )
    require(
        abs(as_float(kpis["session_revenue"]) - as_float(kpis["revenue"])) <= REVENUE_TOLERANCE,
        "session revenue vs fact revenue",
    )

    funnel = frames["q02_session_funnel"]
    view = stage_sessions(funnel, "View")
    view_cart = stage_sessions(funnel, "View and cart")
    nested_purchase = stage_sessions(funnel, "View, cart, and purchase")
    path_full = stage_sessions(funnel, "Purchase with view and cart")
    path_cart = stage_sessions(funnel, "Purchase with cart and no view")
    path_direct = stage_sessions(funnel, "Purchase with no cart")
    purchase = stage_sessions(funnel, "Purchase")
    cart = stage_sessions(funnel, "Cart")
    cart_left = stage_sessions(funnel, "Cart and no purchase")
    require(view >= view_cart >= nested_purchase, "nested funnel is not decreasing")
    require(nested_purchase == path_full, "nested purchase differs from purchase path")
    require(path_full + path_cart + path_direct == purchase, "purchase path does not partition")
    require(cart_left == cart - (path_full + path_cart), "cart abandonment does not match")

    require(len(frames["q06_hourly"]) == 24, "hourly rows")
    require(int(frames["q06_hourly"]["hour_utc"].min()) == 0, "hour starts at 0")
    require(int(frames["q06_hourly"]["hour_utc"].max()) == 23, "hour ends at 23")
    require(len(frames["q07_dow"]) == 9, "weekday rows")
    require(len(frames["q13_daily_revenue"]) == 31, "daily rows")
    require(int((frames["q13_daily_revenue"]["ma_days"] == 7).sum()) == 25, "full moving-average days")
    require(len(frames["q11_abandon_price_band"]) == 7, "abandon bands")
    require(len(frames["q12_price_band_revenue"]) == 7, "revenue bands")
    require(len(frames["q15_rfm_score_bounds"]) == 15, "RFM score bounds")

    reconcile = frames["q18_reconcile"].iloc[0]
    require(as_int(reconcile["fact_rows"]) == EXPECTED_FACT_ROWS, "reconcile rows")
    require(as_int(reconcile["purchases"]) == EXPECTED_PURCHASES, "reconcile purchases")
    require(as_int(reconcile["session_rows"]) == EXPECTED_SESSIONS, "reconcile sessions")
    require(as_int(reconcile["buyer_gap"]) == 0, "buyer gap")
    require(as_int(reconcile["cart_event_gap"]) == 0, "cart event gap")
    for column in (
        "session_revenue_gap",
        "day_revenue_gap",
        "category_revenue_gap",
        "brand_revenue_gap",
        "band_revenue_gap",
        "rfm_revenue_gap",
    ):
        gap = as_float(reconcile[column])
        require(gap <= REVENUE_TOLERANCE, f"{column}={gap}")
    segment_buyers = int(frames["q14_rfm_segments"]["buyers"].sum())
    require(segment_buyers == as_int(reconcile["rfm_buyers"]), "segment buyers")
    logger.info(
        "ANALYSIS_VALIDATED revenue=%s purchases=%s buyers=%s",
        format_value("revenue", kpis["revenue"]),
        format_value("purchases", kpis["purchases"]),
        format_value("buyers", kpis["buyers"]),
    )


def render_results(
    frames: list[tuple[str, str, pd.DataFrame, float]],
    duckdb_version: str,
    min_epoch: int,
    elapsed: float,
) -> str:
    generated = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    lines = [
        "# Analysis results",
        "",
        "Generated by `src/05_run_analysis.py` from `sql/analysis.sql`.",
        f"DuckDB {duckdb_version}. Time zone UTC. Generated {generated}.",
        f"Elapsed {elapsed:.1f} seconds.",
        "",
        "Revenue is purchase price where `is_non_positive_price` is false. "
        "Category is `dim_product`. Brand is `fact_events.brand`. "
        f"The earliest event instant has epoch {min_epoch}, which is 2019-10-01 00:00:00 UTC.",
        "",
        "Every figure in this file is a cell the named query returned.",
        "",
    ]
    for name, title, frame, seconds in frames:
        lines.append(f"## {name}")
        lines.append("")
        lines.append(title)
        lines.append("")
        lines.append(f"Rows: {len(frame):,}. Query time: {seconds:.1f} seconds.")
        lines.append("")
        lines.append(to_markdown(frame))
        lines.append("")
    return "\n".join(lines)


def run_queries(sql_path: Path, output_path: Path) -> None:
    sql_text = sql_path.read_text()
    run_self_test(sql_text)
    prelude, queries = parse_script(sql_text)
    started = time.perf_counter()
    connection = connect()
    version_row = connection.execute("SELECT version()").fetchone()
    duckdb_version = str(version_row[0])
    logger.info("DuckDB %s", duckdb_version)
    for statement in statements(prelude):
        execute(connection, statement)
    timezone = connection.execute("SELECT current_setting('TimeZone')").fetchone()[0]
    if timezone != "UTC":
        logger.error("TimeZone is %s", timezone)
        sys.exit(1)
    min_epoch = as_int(
        connection.execute("SELECT epoch(min(event_time)) FROM fact_events").fetchone()[0]
    )
    logger.info("MIN_EVENT_EPOCH %s", min_epoch)
    frames: dict[str, pd.DataFrame] = {}
    rendered: list[tuple[str, str, pd.DataFrame, float]] = []
    for name, title, body in queries:
        query_started = time.perf_counter()
        logger.info("QUERY_START %s", name)
        try:
            frame = None
            for statement in statements(body):
                frame = execute(connection, statement)
        except Exception:
            logger.exception("QUERY_FAILED %s", name)
            sys.exit(1)
        seconds = time.perf_counter() - query_started
        if frame is None:
            logger.info("QUERY_OK %s seconds=%.1f result=none", name, seconds)
            continue
        frames[name] = frame
        rendered.append((name, title, frame, seconds))
        logger.info("QUERY_OK %s rows=%s seconds=%.1f", name, len(frame), seconds)
    validate(frames, min_epoch)
    elapsed = time.perf_counter() - started
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_results(rendered, duckdb_version, min_epoch, elapsed))
    logger.info("Wrote %s", output_path)
    logger.info("ANALYSIS_ELAPSED seconds=%.1f", elapsed)
    connection.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the gold-layer analysis queries.")
    parser.add_argument("--sql", type=Path, default=DEFAULT_SQL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    sql_path = args.sql.resolve()
    output_path = args.output.resolve()
    assert_safe_output(output_path)
    if not sql_path.exists():
        logger.error("SQL file not found: %s", sql_path)
        sys.exit(1)
    if args.self_test:
        run_self_test(sql_path.read_text())
        return
    run_queries(sql_path, output_path)


if __name__ == "__main__":
    main()
