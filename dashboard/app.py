"""October 2019 clickstream dashboard.

Reads the Parquet extracts in ``dashboard/data``. ``src/06_export_dashboard_data.py``
builds those extracts from the gold tables. Revenue is the sum of purchase
prices where ``is_non_positive_price`` is false. Brand charts use the brand
on the event. Category charts use ``dim_product``, which has one category
per product. Distinct users are read from a pre-aggregated row for the full
month or for one day. A multi-day slice shows the average of the daily
distinct counts, because those counts are not additive.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

import metrics

DATA_DIR = DASHBOARD_DIR / "data"

# Same palette as notebooks/eda.ipynb.
BLUE = "#4C78A8"
ORANGE = "#F58518"
GREEN = "#54A24B"
RED = "#E45756"
PURPLE = "#B279A2"
TEAL = "#72B7B2"
GRAY = "#9D9D9D"

PAGES = ("Overview", "Products", "Customers and timing")
EVENT_TYPES = ("All", "view", "cart", "purchase")


@st.cache_data(max_entries=1)
def load_tables() -> dict[str, pd.DataFrame]:
    """Load the dashboard extracts once per session."""
    return metrics.load_all(DATA_DIR)


def as_date(value: object) -> date | None:
    """Normalize a date widget value. A datetime would shift the UTC date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def coerce_range(value: object, first: date, last: date) -> tuple[date, date]:
    """Accept one date or a pair. Streamlit returns either while a range is open."""
    if isinstance(value, (tuple, list)):
        picked = [item for item in (as_date(piece) for piece in value) if item is not None]
    else:
        single = as_date(value)
        picked = [] if single is None else [single]
    if len(picked) >= 2:
        start, end = picked[0], picked[1]
    elif len(picked) == 1:
        start = end = picked[0]
    else:
        start, end = first, last
    if start > end:
        start, end = end, start
    return start, end


def missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def fmt_money(value: object) -> str:
    if missing(value):
        return "—"
    return f"{float(value):,.2f}"


def fmt_count(value: object) -> str:
    if missing(value):
        return "—"
    return f"{int(round(float(value))):,}"


def fmt_users(value: object, exact: bool) -> str:
    if missing(value):
        return "—"
    number = float(value)
    if exact:
        return f"{int(round(number)):,}"
    return f"{number:,.1f}"


def fmt_rate(value: object) -> str:
    if missing(value):
        return "—"
    return f"{float(value) * 100:.2f}%"


def fmt_rate_detail(value: object) -> str:
    """Four decimal places so 0.018215 stays visible as 1.8215%."""
    if missing(value):
        return "—"
    return f"{float(value) * 100:.4f}%"


def apply_layout(fig: go.Figure, title: str, *, height: int = 420) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        font={"family": "Arial", "size": 13},
        title={"text": title, "font": {"size": 16}},
        margin={"l": 16, "r": 16, "t": 56, "b": 16},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        height=height,
    )
    return fig


def show_chart(fig: go.Figure, alt: str) -> None:
    # theme=None keeps the notebook palette. The Streamlit theme would replace it.
    st.plotly_chart(fig, theme=None, alt=alt)


def bar_colors(names: pd.Series, known: str) -> list[str]:
    colors = []
    for name in names:
        if name == "unknown":
            colors.append(ORANGE)
        elif name == "other":
            colors.append(GRAY)
        else:
            colors.append(known)
    return colors


def horizontal_bars(
    frame: pd.DataFrame,
    label_column: str,
    value_column: str,
    title: str,
    *,
    known_color: str,
    alt: str,
) -> None:
    plot = frame.sort_values(value_column, ascending=True)
    fig = go.Figure(
        go.Bar(
            x=plot[value_column],
            y=plot[label_column],
            orientation="h",
            marker_color=bar_colors(plot[label_column], known_color),
            hovertemplate="%{y}<br>%{x:,.2f}<extra></extra>",
        )
    )
    apply_layout(fig, title, height=max(360, 28 * len(plot) + 120))
    fig.update_xaxes(separatethousands=True)
    show_chart(fig, alt)


def category_options(tables: dict[str, pd.DataFrame]) -> list[str]:
    ranked = tables["month_category"].sort_values(
        ["revenue", "category_l1"], ascending=[False, True]
    )
    return ["All", *ranked["category_l1"].tolist()]


def sidebar(tables: dict[str, pd.DataFrame], first: date, last: date) -> tuple[str, date, date, str, str]:
    with st.sidebar:
        st.header("Filters")
        page = st.radio("Page", PAGES, key="page")
        picked = st.date_input(
            "Date range",
            value=(first, last),
            min_value=first,
            max_value=last,
            key="dates",
            help="Event dates are UTC dates.",
        )
        category = st.selectbox("Category", category_options(tables), key="category")
        event_type = st.selectbox(
            "Event type",
            EVENT_TYPES,
            key="event_type",
            help="Filters the hour and weekday heatmap. Revenue, orders, and the funnel stay on their own definitions.",
        )
    start, end = coerce_range(picked, first, last)
    return str(page), start, end, str(category), str(event_type)


def render_overview(
    tables: dict[str, pd.DataFrame],
    start: date,
    end: date,
    category: str,
    event_type: str,
) -> None:
    daily = metrics.overview_daily(tables, start, end, category)
    cards = metrics.kpis(daily)
    user_label, user_value, user_exact = metrics.active_users(tables, daily, start, end, category)
    first, last = metrics.bounds(tables)
    full_month = metrics.is_full_month(start, end, first, last)

    share = None
    if category != "All":
        total_revenue = metrics.kpis(metrics.overview_daily(tables, start, end, "All"))["revenue"]
        if total_revenue:
            share = cards["revenue"] / total_revenue

    with st.container(horizontal=True):
        st.metric(
            "Revenue",
            fmt_money(cards["revenue"]),
            None if share is None else f"{share * 100:.2f}%",
            delta_color="off",
            delta_arrow="off",
            delta_description=None if share is None else "of revenue on these dates",
            border=True,
            icon=":material/payments:",
            help="Sum of positive purchase prices.",
            chart_data=None if daily.empty else daily["revenue"].tolist(),
            chart_type="bar",
        )
        st.metric(
            "Orders",
            fmt_count(cards["purchases"]),
            border=True,
            icon=":material/shopping_bag:",
            help="Purchase events with a positive price.",
        )
        st.metric(
            "Conversion",
            fmt_rate_detail(cards["conversion"]),
            border=True,
            icon=":material/percent:",
            help="Positive-price purchases divided by views.",
        )
        st.metric(
            "AOV",
            fmt_money(cards["aov"]),
            border=True,
            icon=":material/receipt_long:",
            help="Revenue divided by positive-price purchases.",
        )
        st.metric(
            user_label,
            fmt_users(user_value, user_exact),
            border=True,
            icon=":material/group:",
            help=(
                "Distinct users in this selection."
                if user_exact
                else "Average of the daily distinct counts. A user who returns on another day is counted on each day."
            ),
        )

    if not user_exact:
        st.caption(
            "Active users are an average of daily distinct counts. "
            "Adding those counts would treat a returning user as a new user each day."
        )
    if event_type != "All":
        st.caption(
            f"Event type is {event_type}. It filters the heatmap on Customers and timing. "
            "These cards stay on views, carts, and positive-price purchases."
        )

    funnel_col, trend_col = st.columns(2)
    with funnel_col:
        st.subheader("Event funnel")
        if daily.empty or cards["views"] == 0:
            st.caption("No views in this selection.")
        else:
            fig = go.Figure(
                go.Funnel(
                    y=["Views", "Carts", "Purchases"],
                    x=[cards["views"], cards["carts"], cards["purchases"]],
                    text=[
                        fmt_count(cards["views"]),
                        fmt_count(cards["carts"]),
                        fmt_count(cards["purchases"]),
                    ],
                    textinfo="text+percent initial",
                    marker={"color": [BLUE, ORANGE, GREEN]},
                )
            )
            apply_layout(fig, "Views, carts, and purchases")
            show_chart(fig, "Event funnel from views to carts to purchases")
        if cards["purchases"] > cards["carts"]:
            st.caption(
                "Purchases exceed carts in this selection. A purchase does not require a cart, so the last step can be wider than the cart step."
            )
        else:
            st.caption("This funnel counts events in the selection. Views, carts, and purchases add across days.")
        if full_month and category == "All":
            row = tables["month_totals"].iloc[0]
            view_sessions = float(row["view_sessions"])
            view_and_cart = float(row["view_and_cart_sessions"])
            view_cart_purchase = float(row["view_cart_purchase_sessions"])
            purchase_sessions = float(row["purchase_sessions"])
            purchase_no_cart = float(row["purchase_no_cart_sessions"])
            st.caption(
                "Session path for the full month, counted once per session: "
                f"{fmt_count(view_sessions)} sessions contain a view, "
                f"{fmt_count(view_and_cart)} of those also contain a cart "
                f"({fmt_rate(view_and_cart / view_sessions)} of view sessions), and "
                f"{fmt_count(view_cart_purchase)} contain a view, a cart, and a purchase "
                f"({fmt_rate(view_cart_purchase / view_sessions)}). "
                f"{fmt_count(purchase_no_cart)} of {fmt_count(purchase_sessions)} purchase sessions have no cart "
                f"({fmt_rate(purchase_no_cart / purchase_sessions)}). "
                "A session can cross midnight, so this path is not rebuilt by adding days."
            )

    with trend_col:
        st.subheader("Daily revenue")
        if daily.empty:
            st.caption("No dates in this selection.")
        else:
            labels = [item.isoformat() for item in daily["event_date"]]
            colors = [ORANGE if flag else GREEN for flag in daily["is_weekend"].tolist()]
            fig = go.Figure()
            fig.add_bar(
                x=labels,
                y=daily["revenue"],
                name="Revenue",
                marker_color=colors,
                hovertemplate="%{x}<br>%{y:,.2f}<extra>Revenue</extra>",
            )
            fig.add_scatter(
                x=labels,
                y=daily["revenue_ma7"],
                name="7-day average",
                mode="lines",
                line={"color": PURPLE, "width": 2},
                connectgaps=False,
                hovertemplate="%{x}<br>%{y:,.2f}<extra>7-day average</extra>",
            )
            apply_layout(fig, "Revenue by UTC date")
            fig.update_xaxes(type="category", tickangle=-45)
            fig.update_yaxes(separatethousands=True, title="Revenue")
            show_chart(fig, "Daily revenue with weekend days in orange and a 7-day average")
            peak = daily.loc[daily["revenue"].idxmax()]
            low = daily.loc[daily["revenue"].idxmin()]
            st.caption(
                f"Highest day {peak['event_date'].isoformat()} at {fmt_money(peak['revenue'])}. "
                f"Lowest day {low['event_date'].isoformat()} at {fmt_money(low['revenue'])}. "
                "Weekend bars are orange. The 7-day average is computed inside the selected dates and stays blank until the window holds seven dates."
            )
            full_window = daily.dropna(subset=["revenue_ma7"])
            if full_window.empty:
                st.caption("The selected range has fewer than seven dates, so the 7-day average is blank.")
            else:
                ma_peak = full_window.loc[full_window["revenue_ma7"].idxmax()]
                st.caption(
                    f"The full 7-day average peaks on {ma_peak['event_date'].isoformat()} at {fmt_money(ma_peak['revenue_ma7'])}."
                )

    with st.expander("Daily figures"):
        shown = daily[
            [
                "event_date",
                "day_name",
                "views",
                "carts",
                "purchases",
                "revenue",
                "users",
                "revenue_ma7",
            ]
        ].copy()
        st.dataframe(
            shown,
            hide_index=True,
            column_config={
                "event_date": st.column_config.DateColumn("Date"),
                "day_name": st.column_config.TextColumn("Weekday"),
                "views": st.column_config.NumberColumn("Views", format="%,.0f"),
                "carts": st.column_config.NumberColumn("Carts", format="%,.0f"),
                "purchases": st.column_config.NumberColumn("Purchases", format="%,.0f"),
                "revenue": st.column_config.NumberColumn("Revenue", format="%,.2f"),
                "users": st.column_config.NumberColumn("Distinct users", format="%,.0f"),
                "revenue_ma7": st.column_config.NumberColumn("7-day average", format="%,.2f"),
            },
            alt="Daily views, carts, purchases, revenue, and distinct users",
        )


def render_products(
    tables: dict[str, pd.DataFrame],
    start: date,
    end: date,
    category: str,
) -> None:
    scoped = metrics.kpis(metrics.overview_daily(tables, start, end, category))
    st.caption(f"Revenue in this selection is {fmt_money(scoped['revenue'])}.")

    if category == "All":
        st.subheader("Category revenue")
        categories = metrics.category_revenue(tables, start, end)
        if categories.empty or float(categories["revenue"].sum()) == 0:
            st.caption("No category revenue in this selection.")
        else:
            top = categories.iloc[0]
            horizontal_bars(
                categories,
                "category_l1",
                "revenue",
                "Revenue by top-level category",
                known_color=BLUE,
                alt=f"Category revenue, highest for {top['category_l1']}",
            )
            st.caption(
                f"{top['category_l1']} is {fmt_money(top['revenue'])} "
                f"({fmt_rate(top['revenue_share'])} of revenue in this selection). "
                "Unknown is the filled value for a missing category code. Orange marks that bar."
            )
        st.subheader("Second-level categories")
    else:
        st.subheader(f"Inside {category}")

    level2 = metrics.level2_revenue(tables, start, end, category)
    if level2.empty or float(level2["revenue"].sum()) == 0:
        st.caption("No second-level revenue in this selection.")
    else:
        top_l2 = level2.iloc[0]
        horizontal_bars(
            level2,
            "category",
            "revenue",
            "Top second-level categories by revenue",
            known_color=TEAL,
            alt=f"Second-level revenue, highest for {top_l2['category']}",
        )
        st.caption(
            f"Highest second level is {top_l2['category']} at {fmt_money(top_l2['revenue'])} "
            f"({fmt_rate(top_l2['revenue_share'])} of revenue in this selection)."
        )

    st.subheader("Brands")
    brands = metrics.brand_revenue(tables, start, end, category)
    if brands.empty or float(brands["revenue"].sum()) == 0:
        st.caption("No brand revenue in this selection.")
    else:
        top_brand = brands.iloc[0]
        horizontal_bars(
            brands,
            "brand",
            "revenue",
            "Brand revenue",
            known_color=BLUE,
            alt=f"Brand revenue, highest for {top_brand['brand']}",
        )
        st.caption(
            f"Highest revenue brand is {top_brand['brand']} at {fmt_money(top_brand['revenue'])} "
            f"({fmt_rate(top_brand['revenue_share'])} of this selection). "
            "Brand is the brand on the event. Unknown fills a missing brand. "
            "Other rolls up every brand outside the top 12 and unknown. "
            "The All category is its own rollup, so these bars are not added to the category rows."
        )
        with st.expander("Brand figures"):
            st.dataframe(
                brands,
                hide_index=True,
                column_config={
                    "brand": st.column_config.TextColumn("Brand"),
                    "views": st.column_config.NumberColumn("Views", format="%,.0f"),
                    "carts": st.column_config.NumberColumn("Carts", format="%,.0f"),
                    "purchases": st.column_config.NumberColumn("Purchases", format="%,.0f"),
                    "revenue": st.column_config.NumberColumn("Revenue", format="%,.2f"),
                    "revenue_share": st.column_config.NumberColumn("Revenue share", format="percent"),
                    "purchase_per_view": st.column_config.NumberColumn("Purchases per view", format="percent"),
                    "aov": st.column_config.NumberColumn("AOV", format="%,.2f"),
                },
                alt="Brand views, purchases, revenue, and average order value",
            )

    band_col, abandon_col = st.columns(2)
    with band_col:
        st.subheader("Price bands")
        bands = metrics.price_band_revenue(tables, start, end, category)
        fig = go.Figure(
            go.Bar(
                x=bands["band_label"],
                y=bands["revenue"],
                marker_color=GREEN,
                hovertemplate="%{x}<br>%{y:,.2f}<extra>Revenue</extra>",
            )
        )
        apply_layout(fig, "Purchase revenue by price band")
        fig.update_yaxes(separatethousands=True, title="Revenue")
        show_chart(fig, "Purchase revenue by price band, excluding the non-positive band")
        if float(bands["revenue"].sum()) == 0:
            st.caption("No positive-price purchases in this selection.")
        else:
            richest = bands.loc[bands["revenue"].idxmax()]
            st.caption(
                f"The {richest['band_label']} band holds {fmt_money(richest['revenue'])} "
                f"({fmt_rate(richest['revenue_share'])} of purchase revenue here). "
                "Eligible purchases have a positive price, so the non-positive band is omitted. "
                "Cart abandonment still includes it."
            )

    with abandon_col:
        st.subheader("Cart abandonment")
        by_category, by_band, totals = metrics.abandonment(tables, start, end, category)
        if category == "All":
            plot = by_category.sort_values("abandoned_cart_value", ascending=True)
            fig = go.Figure(
                go.Bar(
                    x=plot["abandoned_cart_value"],
                    y=plot["category_l1"],
                    orientation="h",
                    marker_color=bar_colors(plot["category_l1"], RED),
                    hovertemplate="%{y}<br>%{x:,.2f}<extra>Abandoned cart value</extra>",
                )
            )
            apply_layout(fig, "Abandoned cart value by category", height=420)
            fig.update_xaxes(separatethousands=True)
            show_chart(fig, "Abandoned cart value by category")
        else:
            fig = go.Figure(
                go.Bar(
                    x=by_band["band_label"],
                    y=by_band["abandon_rate"],
                    marker_color=RED,
                    hovertemplate="%{x}<br>%{y:.2%}<extra>Abandon rate</extra>",
                )
            )
            apply_layout(fig, f"Abandonment rate inside {category}")
            fig.update_yaxes(tickformat=".0%", title="Abandoned pairs / cart pairs")
            show_chart(fig, f"Cart abandonment rate by price band inside {category}")
        st.caption(
            f"{fmt_count(totals['abandoned_pairs'])} of {fmt_count(totals['cart_pairs'])} "
            f"session-product carts were abandoned ({fmt_rate(totals['abandon_rate'])}). "
            f"Abandoned cart value is {fmt_money(totals['abandoned_cart_value'])}. "
            "A pair is one product carted in one session. Abandoned means that session has no purchase of that same product. "
            "The value is the maximum cart price, not revenue."
        )
        rated = by_band.loc[by_band["cart_pairs"] > 0]
        if not rated.empty:
            highest_rate = rated.loc[rated["abandon_rate"].idxmax()]
            highest_value = by_band.loc[by_band["abandoned_cart_value"].idxmax()]
            st.caption(
                f"The highest abandon rate is {fmt_rate(highest_rate['abandon_rate'])} in {highest_rate['band_label']}. "
                f"The largest abandoned value is {fmt_money(highest_value['abandoned_cart_value'])} in {highest_value['band_label']}."
            )


def render_customers(
    tables: dict[str, pd.DataFrame],
    start: date,
    end: date,
    category: str,
    event_type: str,
) -> None:
    st.subheader("RFM segments")
    segments = metrics.rfm_table(tables)
    buyers = float(segments["buyers"].sum())
    last = metrics.bounds(tables)[1]
    st.caption(
        f"Scores were fit on all {fmt_count(buyers)} October buyers. "
        f"Recency is days from the last purchase to {last.isoformat()}. "
        "The date and category filters do not recompute the segments."
    )
    bounds = tables["rfm_score_bounds"]
    frequency = bounds.loc[bounds["metric"] == "purchase_sessions"]
    low_scores = frequency.loc[frequency["score"].isin([1, 2, 3])]
    if (
        len(low_scores) == 3
        and (low_scores["min_value"] == 1).all()
        and (low_scores["max_value"] == 1).all()
    ):
        st.caption(
            "Frequency scores 1, 2, and 3 each have a purchase-session minimum and maximum of 1. "
            "The segment label uses the raw session count, because an NTILE of one-session buyers is not a loyalty scale."
        )

    plot = segments.iloc[::-1]
    fig = go.Figure()
    fig.add_bar(
        y=plot["segment"],
        x=plot["buyer_share"],
        name="Buyer share",
        orientation="h",
        marker_color=BLUE,
        hovertemplate="%{y}<br>%{x:.2%}<extra>Buyer share</extra>",
    )
    fig.add_bar(
        y=plot["segment"],
        x=plot["revenue_share"],
        name="Revenue share",
        orientation="h",
        marker_color=GREEN,
        hovertemplate="%{y}<br>%{x:.2%}<extra>Revenue share</extra>",
    )
    apply_layout(fig, "Buyer share and revenue share")
    fig.update_xaxes(tickformat=".0%", title="Share of October buyers or revenue")
    fig.update_layout(barmode="group")
    show_chart(fig, "RFM segments by buyer share and revenue share")

    champions = segments.loc[segments["segment"] == "champions"]
    if len(champions) == 1:
        row = champions.iloc[0]
        st.caption(
            f"Champions are {fmt_count(row['buyers'])} buyers ({fmt_rate(row['buyer_share'])}) "
            f"and {fmt_money(row['revenue'])} of revenue ({fmt_rate(row['revenue_share'])})."
        )

    st.dataframe(
        segments[
            [
                "segment",
                "buyers",
                "buyer_share",
                "revenue",
                "revenue_share",
                "avg_recency_days",
                "avg_purchase_sessions",
                "avg_monetary",
            ]
        ],
        hide_index=True,
        column_config={
            "segment": st.column_config.TextColumn("Segment"),
            "buyers": st.column_config.NumberColumn("Buyers", format="%,.0f"),
            "buyer_share": st.column_config.NumberColumn("Buyer share", format="percent"),
            "revenue": st.column_config.NumberColumn("Revenue", format="%,.2f"),
            "revenue_share": st.column_config.NumberColumn("Revenue share", format="percent"),
            "avg_recency_days": st.column_config.NumberColumn("Avg recency, days", format="%.2f"),
            "avg_purchase_sessions": st.column_config.NumberColumn("Avg purchase sessions", format="%.2f"),
            "avg_monetary": st.column_config.NumberColumn("Avg monetary", format="%,.2f"),
        },
        alt="RFM segment buyers, revenue, recency, and purchase sessions",
    )

    with st.expander("Score bounds"):
        st.dataframe(
            bounds.sort_values(["metric", "score"]),
            hide_index=True,
            column_config={
                "metric": st.column_config.TextColumn("Metric"),
                "score": st.column_config.NumberColumn("Score", format="%d"),
                "buyers": st.column_config.NumberColumn("Buyers", format="%,.0f"),
                "min_value": st.column_config.NumberColumn("Minimum", format="%,.2f"),
                "max_value": st.column_config.NumberColumn("Maximum", format="%,.2f"),
            },
            alt="Observed minimum and maximum inside each RFM score",
        )

    st.subheader("Hour and weekday")
    pivot = metrics.heatmap(tables, start, end, category, event_type)
    total_events = float(pivot.to_numpy().sum())
    fig = go.Figure(
        go.Heatmap(
            z=pivot.to_numpy(),
            x=[f"{int(hour):02d}" for hour in pivot.columns],
            y=list(pivot.index),
            colorscale=[[0, "#F4F6F8"], [1, BLUE]],
            hovertemplate="%{y} %{x}:00 UTC<br>%{z:,.0f} events<extra></extra>",
            colorbar={"title": "Events"},
        )
    )
    apply_layout(fig, f"{event_type} events by weekday and UTC hour", height=460)
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(title="Hour UTC")
    show_chart(fig, f"{event_type} events by weekday and UTC hour")
    if total_events == 0:
        st.caption("No events in this heatmap selection.")
    else:
        stacked = pivot.stack()
        day_name, hour = stacked.idxmax()
        st.caption(
            f"Heatmap events: {fmt_count(total_events)}. "
            f"The busiest cell is {day_name} {int(hour):02d}:00 UTC with {fmt_count(stacked.max())} events. "
            "Hours are UTC. The file does not record a store time zone. "
            "Event counts add across the selected dates. Distinct users do not."
        )


def main() -> None:
    st.set_page_config(
        page_title="October 2019 clickstream",
        page_icon=":material/monitoring:",
        layout="wide",
    )
    if not DATA_DIR.is_dir() or not any(DATA_DIR.glob("*.parquet")):
        st.error("Dashboard extracts are missing. Run src/06_export_dashboard_data.py.")
        st.stop()

    tables = load_tables()
    first, last = metrics.bounds(tables)
    page, start, end, category, event_type = sidebar(tables, first, last)

    st.title("October 2019 clickstream")
    if start == end:
        window = start.isoformat()
    else:
        window = f"{start.isoformat()} to {end.isoformat()}"
    st.caption(f"{window}. Category {category}. Revenue uses positive purchase prices.")

    if page == "Overview":
        render_overview(tables, start, end, category, event_type)
    elif page == "Products":
        render_products(tables, start, end, category)
    else:
        render_customers(tables, start, end, category, event_type)


main()
