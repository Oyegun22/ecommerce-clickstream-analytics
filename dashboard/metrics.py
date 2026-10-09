"""Filter the dashboard extracts. Additive metrics are summed. Distinct users are not.

"All" is a pre-aggregated row in the brand extract. Summing it with the
category rows would double revenue. Callers pass category "All" or one
category_l1 value and this module picks the matching rows.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

# Band order matches the price_band macro in sql/analysis.sql.
BANDS = (
    ("00_non_positive", "non-positive"),
    ("01_under_50", "under 50"),
    ("02_50_to_150", "50 to 150"),
    ("03_150_to_300", "150 to 300"),
    ("04_300_to_600", "300 to 600"),
    ("05_600_to_1000", "600 to 1000"),
    ("06_1000_and_above", "1000 and above"),
)
DAY_ORDER = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)


def load_all(data_dir: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    for path in sorted(data_dir.glob("*.parquet")):
        frame = pd.read_parquet(path)
        if "event_date" in frame.columns:
            frame["event_date"] = pd.to_datetime(frame["event_date"]).dt.date
        tables[path.stem] = frame
    return tables


def bounds(tables: dict[str, pd.DataFrame]) -> tuple[date, date]:
    values = tables["daily_totals"]["event_date"]
    return min(values), max(values)


def in_range(frame: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    dates = frame["event_date"]
    return frame.loc[(dates >= start) & (dates <= end)].copy()


def is_full_month(start: date, end: date, first: date, last: date) -> bool:
    return start == first and end == last


def overview_daily(
    tables: dict[str, pd.DataFrame], start: date, end: date, category: str
) -> pd.DataFrame:
    """One row per selected date. Users on each row are distinct that day."""
    if category == "All":
        frame = tables["daily_totals"]
    else:
        frame = tables["daily_category"]
        frame = frame.loc[frame["category_l1"] == category]
    out = in_range(frame, start, end).sort_values("event_date")
    out["revenue_ma7"] = out["revenue"].rolling(7, min_periods=7).mean()
    return out


def kpis(daily: pd.DataFrame) -> dict[str, float]:
    views = float(daily["views"].sum())
    carts = float(daily["carts"].sum())
    purchases = float(daily["purchases"].sum())
    revenue = float(daily["revenue"].sum())
    return {
        "views": views,
        "carts": carts,
        "purchases": purchases,
        "revenue": revenue,
        "conversion": purchases / views if views else float("nan"),
        "aov": revenue / purchases if purchases else float("nan"),
    }


def active_users(
    tables: dict[str, pd.DataFrame],
    daily: pd.DataFrame,
    start: date,
    end: date,
    category: str,
) -> tuple[str, float, bool]:
    """Exact distinct users for the full month or a single day.

    A multi-day slice returns the average of the daily distinct counts.
    Summing those counts would treat a returning user as a new user each day.
    """
    first, last = bounds(tables)
    full = is_full_month(start, end, first, last)
    if full and category == "All":
        return "Active users", float(tables["month_totals"].iloc[0]["users"]), True
    if full and category != "All":
        match = tables["month_category"].loc[
            tables["month_category"]["category_l1"] == category, "users"
        ]
        if len(match) == 1:
            return "Active users", float(match.iloc[0]), True
    if start == end and len(daily):
        return "Active users", float(daily["users"].sum()), True
    if len(daily) == 0:
        return "Avg daily active users", float("nan"), False
    return "Avg daily active users", float(daily["users"].mean()), False


def category_revenue(tables: dict[str, pd.DataFrame], start: date, end: date) -> pd.DataFrame:
    frame = in_range(tables["daily_category"], start, end)
    grouped = (
        frame.groupby("category_l1", as_index=False)[["views", "carts", "purchases", "revenue"]]
        .sum()
        .sort_values("revenue", ascending=False)
    )
    total = grouped["revenue"].sum()
    grouped["revenue_share"] = grouped["revenue"] / total if total else 0.0
    return grouped


def level2_revenue(
    tables: dict[str, pd.DataFrame], start: date, end: date, category: str, limit: int = 10
) -> pd.DataFrame:
    frame = in_range(tables["category_l2"], start, end)
    if category != "All":
        frame = frame.loc[frame["category_l1"] == category]
    grouped = frame.groupby(["category_l1", "category_l2"], as_index=False)[
        ["views", "carts", "purchases", "revenue"]
    ].sum()
    grouped["category"] = grouped["category_l1"] + " / " + grouped["category_l2"]
    grouped = grouped.sort_values("revenue", ascending=False).head(limit)
    total = float(in_range(tables["daily_totals"] if category == "All" else tables["daily_category"].loc[
        tables["daily_category"]["category_l1"] == category
    ], start, end)["revenue"].sum()) if category == "All" or category else 0.0
    # Share of the selected scope, computed from that scope's own revenue.
    scope = overview_daily(tables, start, end, category)
    scope_revenue = float(scope["revenue"].sum())
    grouped["revenue_share"] = grouped["revenue"] / scope_revenue if scope_revenue else 0.0
    return grouped


def brand_revenue(tables: dict[str, pd.DataFrame], start: date, end: date, category: str) -> pd.DataFrame:
    frame = tables["brands"]
    frame = frame.loc[frame["category_l1"] == category]
    frame = in_range(frame, start, end)
    grouped = (
        frame.groupby("brand", as_index=False)[["views", "carts", "purchases", "revenue"]]
        .sum()
        .sort_values(["revenue", "brand"], ascending=[False, True])
    )
    total = grouped["revenue"].sum()
    grouped["revenue_share"] = grouped["revenue"] / total if total else 0.0
    grouped["purchase_per_view"] = grouped["purchases"] / grouped["views"].where(grouped["views"] > 0)
    grouped["aov"] = grouped["revenue"] / grouped["purchases"].where(grouped["purchases"] > 0)
    return grouped


def price_band_revenue(
    tables: dict[str, pd.DataFrame], start: date, end: date, category: str
) -> pd.DataFrame:
    frame = in_range(tables["price_bands"], start, end)
    if category != "All":
        frame = frame.loc[frame["category_l1"] == category]
    grouped = frame.groupby(["band_key", "band_label"], as_index=False)[["purchases", "revenue"]].sum()
    order = pd.DataFrame(BANDS, columns=["band_key", "band_label"])
    # Drop the empty non-positive band from the revenue mix. Eligible purchases
    # do not land there. Abandonment still keeps that band.
    order = order.loc[order["band_key"] != "00_non_positive"]
    merged = order.merge(grouped.drop(columns=["band_label"]), on="band_key", how="left")
    merged["purchases"] = merged["purchases"].fillna(0)
    merged["revenue"] = merged["revenue"].fillna(0.0)
    purchase_total = merged["purchases"].sum()
    revenue_total = merged["revenue"].sum()
    merged["purchase_share"] = merged["purchases"] / purchase_total if purchase_total else 0.0
    merged["revenue_share"] = merged["revenue"] / revenue_total if revenue_total else 0.0
    return merged


def abandonment(
    tables: dict[str, pd.DataFrame], start: date, end: date, category: str
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    frame = in_range(tables["abandonment"], start, end)
    if category != "All":
        scoped = frame.loc[frame["category_l1"] == category]
    else:
        scoped = frame
    by_category = (
        frame.groupby("category_l1", as_index=False)[
            ["cart_pairs", "abandoned_pairs", "abandoned_cart_value"]
        ]
        .sum()
        .sort_values("abandoned_cart_value", ascending=False)
    )
    by_category["abandon_rate"] = by_category["abandoned_pairs"] / by_category["cart_pairs"].where(
        by_category["cart_pairs"] > 0
    )
    by_band = scoped.groupby(["band_key", "band_label"], as_index=False)[
        ["cart_pairs", "abandoned_pairs", "abandoned_cart_value"]
    ].sum()
    order = pd.DataFrame(BANDS, columns=["band_key", "band_label"])
    by_band = order.merge(by_band.drop(columns=["band_label"]), on="band_key", how="left")
    for column in ("cart_pairs", "abandoned_pairs", "abandoned_cart_value"):
        by_band[column] = by_band[column].fillna(0)
    by_band["abandon_rate"] = by_band["abandoned_pairs"] / by_band["cart_pairs"].where(by_band["cart_pairs"] > 0)
    totals = {
        "cart_pairs": float(scoped["cart_pairs"].sum()),
        "abandoned_pairs": float(scoped["abandoned_pairs"].sum()),
        "abandoned_cart_value": float(scoped["abandoned_cart_value"].sum()),
    }
    totals["abandon_rate"] = (
        totals["abandoned_pairs"] / totals["cart_pairs"] if totals["cart_pairs"] else float("nan")
    )
    return by_category, by_band, totals


def heatmap(
    tables: dict[str, pd.DataFrame],
    start: date,
    end: date,
    category: str,
    event_type: str,
) -> pd.DataFrame:
    """Rows are weekdays Monday–Sunday. Columns are UTC hours 0–23."""
    frame = in_range(tables["hourly"], start, end)
    if category != "All":
        frame = frame.loc[frame["category_l1"] == category]
    if event_type != "All":
        frame = frame.loc[frame["event_type"] == event_type]
    grouped = frame.groupby(["day_name", "hour_utc"], as_index=False)["events"].sum()
    pivot = grouped.pivot(index="day_name", columns="hour_utc", values="events")
    pivot = pivot.reindex(index=list(DAY_ORDER), columns=list(range(24))).fillna(0.0)
    return pivot


def rfm_table(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frame = tables["rfm_segments"].copy()
    buyers = frame["buyers"].sum()
    revenue = frame["revenue"].sum()
    frame["buyer_share"] = frame["buyers"] / buyers if buyers else 0.0
    frame["revenue_share"] = frame["revenue"] / revenue if revenue else 0.0
    return frame.sort_values("revenue", ascending=False)
