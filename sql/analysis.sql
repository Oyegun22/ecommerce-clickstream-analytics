-- Gold-layer analysis for the REES46 October 2019 clickstream.
--
-- Run from the project root so the parquet paths resolve. src/05_run_analysis.py
-- does that, sets DuckDB TimeZone to UTC, and writes docs/analysis_results.md.
--
-- Definitions locked to the gold model:
--   Revenue is purchase price where is_non_positive_price is false.
--   Category comes from dim_product. On this file a product has one category.
--   Brand metrics use fact_events.brand, the brand on the event.
--   A session key is user_session. sessions.user_id is the dominant user and
--   is not used to filter events.
--   Hour of day is the UTC hour of event_time. The runner sets TimeZone=UTC
--   before these statements, because the column is TIMESTAMP WITH TIME ZONE.
--
-- Price bands are half-open round cuts placed across the purchase-price
-- distribution (the quantile query prints that distribution):
--   under 50, 50 to 150, 150 to 300, 300 to 600, 600 to 1000, 1000 and above.
--   A non-positive cart price stays in its own band.

SET TimeZone = 'UTC';
SET memory_limit = '8GB';
SET threads = 6;
SET preserve_insertion_order = false;
SET enable_progress_bar = false;

CREATE OR REPLACE VIEW fact_events AS
SELECT *
FROM read_parquet('data/gold/fact_events/**/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW dim_product AS
SELECT *
FROM read_parquet('data/gold/dim_product/*.parquet');

-- Registered for ad hoc joins back to first_seen and last_seen.
-- The RFM query aggregates fact_events, because monetary and recency are
-- purchase facts and dim_user.n_purchases counts every purchase event.
CREATE OR REPLACE VIEW dim_user AS
SELECT *
FROM read_parquet('data/gold/dim_user/*.parquet');

CREATE OR REPLACE VIEW dim_date AS
SELECT *
FROM read_parquet('data/gold/dim_date/*.parquet');

CREATE OR REPLACE VIEW sessions AS
SELECT *
FROM read_parquet('data/gold/sessions/*.parquet');

CREATE OR REPLACE MACRO price_band(price_value) AS
CASE
  WHEN price_value < 50 THEN '01_under_50'
  WHEN price_value < 150 THEN '02_50_to_150'
  WHEN price_value < 300 THEN '03_150_to_300'
  WHEN price_value < 600 THEN '04_300_to_600'
  WHEN price_value < 1000 THEN '05_600_to_1000'
  ELSE '06_1000_and_above'
END;

CREATE OR REPLACE MACRO price_band_label(band_key) AS
CASE band_key
  WHEN '00_non_positive' THEN 'non-positive'
  WHEN '01_under_50' THEN 'under 50'
  WHEN '02_50_to_150' THEN '50 to 150'
  WHEN '03_150_to_300' THEN '150 to 300'
  WHEN '04_300_to_600' THEN '300 to 600'
  WHEN '05_600_to_1000' THEN '600 to 1000'
  WHEN '06_1000_and_above' THEN '1000 and above'
  ELSE band_key
END;

-- Segment labels use recency and monetary NTILE scores plus the raw number of
-- purchase sessions. Frequency NTILE is still computed and published. It is
-- not the frequency leg of the label: most buyers have one purchase session,
-- and NTILE splits that tie across several scores.
CREATE OR REPLACE MACRO rfm_segment(r_score, m_score, purchase_sessions) AS
CASE
  WHEN r_score >= 4 AND purchase_sessions >= 3 AND m_score >= 4 THEN 'champions'
  WHEN purchase_sessions >= 2 AND m_score >= 3 AND r_score >= 3 THEN 'loyal repeaters'
  WHEN m_score = 5 AND purchase_sessions = 1 THEN 'big spenders'
  WHEN purchase_sessions >= 2 AND r_score <= 2 THEN 'need attention'
  WHEN purchase_sessions = 1 AND r_score >= 4 THEN 'recent one-time'
  WHEN purchase_sessions = 1 AND r_score <= 2 THEN 'early one-time'
  ELSE 'other'
END;

-- name: q01_kpis
-- Month totals, event rates, and buyer rate.
WITH events AS (
  SELECT
    count(*) AS events,
    count(*) FILTER (WHERE event_type = 'view') AS views,
    count(*) FILTER (WHERE event_type = 'cart') AS carts,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    round(
      sum(price) FILTER (
        WHERE event_type = 'purchase' AND NOT is_non_positive_price
      ),
      2
    ) AS revenue,
    count(DISTINCT user_id) AS users,
    count(DISTINCT user_id) FILTER (WHERE event_type = 'purchase') AS buyers,
    count(DISTINCT user_session) AS fact_sessions
  FROM fact_events
),
sess AS (
  SELECT
    count(*) AS sessions,
    count(*) FILTER (WHERE n_views > 0) AS view_sessions,
    count(*) FILTER (WHERE n_carts > 0) AS cart_sessions,
    count(*) FILTER (WHERE n_purchases > 0) AS purchase_sessions,
    round(sum(revenue), 2) AS session_revenue
  FROM sessions
)
SELECT
  events.events,
  events.views,
  events.carts,
  events.purchases,
  events.revenue,
  round(events.revenue / events.purchases, 2) AS aov,
  round(events.carts * 1.0 / events.views, 6) AS cart_per_view,
  round(events.purchases * 1.0 / events.views, 6) AS purchase_per_view,
  round(events.purchases * 1.0 / events.carts, 6) AS purchase_per_cart,
  events.users,
  events.buyers,
  round(events.buyers * 1.0 / events.users, 6) AS buyer_rate,
  sess.sessions,
  events.fact_sessions,
  sess.view_sessions,
  sess.cart_sessions,
  sess.purchase_sessions,
  round(sess.purchase_sessions * 1.0 / sess.view_sessions, 6) AS session_purchase_per_view,
  round(events.revenue / sess.purchase_sessions, 2) AS revenue_per_purchase_session,
  sess.session_revenue
FROM events
CROSS JOIN sess;

-- name: q02_session_funnel
-- Session reach, the nested view-cart-purchase path, and the purchase partition.
-- The nested path requires every earlier event type in the same session.
-- The purchase-path rows partition purchase sessions and sum to that count.
WITH totals AS (
  SELECT
    count(*) AS all_sessions,
    count(*) FILTER (WHERE n_views > 0) AS view_sessions,
    count(*) FILTER (WHERE n_views > 0 AND n_carts > 0) AS view_and_cart,
    count(*) FILTER (
      WHERE n_views > 0 AND n_carts > 0 AND n_purchases > 0
    ) AS view_cart_purchase,
    count(*) FILTER (WHERE n_purchases > 0) AS purchase_sessions,
    count(*) FILTER (
      WHERE n_purchases > 0 AND n_carts > 0 AND n_views > 0
    ) AS purchase_view_cart,
    count(*) FILTER (
      WHERE n_purchases > 0 AND n_carts > 0 AND n_views = 0
    ) AS purchase_cart_no_view,
    count(*) FILTER (WHERE n_purchases > 0 AND n_carts = 0) AS purchase_no_cart,
    count(*) FILTER (WHERE n_carts > 0) AS cart_sessions,
    count(*) FILTER (WHERE n_carts > 0 AND n_purchases = 0) AS cart_no_purchase
  FROM sessions
),
stages AS (
  SELECT 1 AS stage_order, 'nested' AS stage_group, 'View' AS stage,
    view_sessions AS sessions, view_sessions AS rate_base, 'view sessions' AS rate_basis
  FROM totals
  UNION ALL
  SELECT 2, 'nested', 'View and cart', view_and_cart, view_sessions, 'view sessions' FROM totals
  UNION ALL
  SELECT 3, 'nested', 'View, cart, and purchase', view_cart_purchase, view_sessions, 'view sessions' FROM totals
  UNION ALL
  SELECT 4, 'purchase_path', 'Purchase with view and cart', purchase_view_cart, purchase_sessions, 'purchase sessions' FROM totals
  UNION ALL
  SELECT 5, 'purchase_path', 'Purchase with cart and no view', purchase_cart_no_view, purchase_sessions, 'purchase sessions' FROM totals
  UNION ALL
  SELECT 6, 'purchase_path', 'Purchase with no cart', purchase_no_cart, purchase_sessions, 'purchase sessions' FROM totals
  UNION ALL
  SELECT 7, 'reach', 'Purchase', purchase_sessions, all_sessions, 'sessions' FROM totals
  UNION ALL
  SELECT 8, 'reach', 'Cart', cart_sessions, all_sessions, 'sessions' FROM totals
  UNION ALL
  SELECT 9, 'cart', 'Cart and no purchase', cart_no_purchase, cart_sessions, 'cart sessions' FROM totals
)
SELECT
  stage_order,
  stage_group,
  stage,
  sessions,
  rate_basis,
  round(sessions * 1.0 / rate_base, 6) AS rate
FROM stages
ORDER BY stage_order;

-- name: q03_category_l1
-- Category scorecard. category_l1 is the product attribute from dim_product.
WITH base AS (
  SELECT
    p.category_l1,
    f.user_session,
    f.user_id,
    f.event_type,
    f.price,
    f.is_non_positive_price
  FROM fact_events AS f
  JOIN dim_product AS p ON f.product_id = p.product_id
),
agg AS (
  SELECT
    category_l1,
    count(*) FILTER (WHERE event_type = 'view') AS views,
    count(*) FILTER (WHERE event_type = 'cart') AS carts,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    round(
      sum(price) FILTER (
        WHERE event_type = 'purchase' AND NOT is_non_positive_price
      ),
      2
    ) AS revenue,
    count(DISTINCT user_session) FILTER (WHERE event_type = 'view') AS view_sessions,
    count(DISTINCT user_session) FILTER (WHERE event_type = 'cart') AS cart_sessions,
    count(DISTINCT user_session) FILTER (WHERE event_type = 'purchase') AS purchase_sessions,
    count(DISTINCT user_id) FILTER (WHERE event_type = 'view') AS viewers,
    count(DISTINCT user_id) FILTER (WHERE event_type = 'purchase') AS buyers
  FROM base
  GROUP BY category_l1
)
SELECT
  category_l1,
  views,
  carts,
  purchases,
  revenue,
  round(revenue / nullif(sum(revenue) OVER (), 0), 6) AS revenue_share,
  rank() OVER (ORDER BY revenue DESC, category_l1 ASC) AS revenue_rank,
  view_sessions,
  cart_sessions,
  purchase_sessions,
  viewers,
  buyers,
  round(carts * 1.0 / nullif(views, 0), 6) AS cart_per_view,
  round(purchases * 1.0 / nullif(views, 0), 6) AS purchase_per_view,
  round(purchases * 1.0 / nullif(carts, 0), 6) AS purchase_per_cart,
  round(purchase_sessions * 1.0 / nullif(view_sessions, 0), 6) AS session_purchase_per_view,
  round(buyers * 1.0 / nullif(viewers, 0), 6) AS buyer_per_viewer,
  round(revenue / nullif(purchases, 0), 2) AS aov
FROM agg
ORDER BY revenue DESC, category_l1 ASC;

-- name: q04_category_l2
-- Top category pairs by revenue, plus the unknown category row.
WITH base AS (
  SELECT
    p.category_l1,
    p.category_l2,
    f.user_session,
    f.event_type,
    f.price,
    f.is_non_positive_price
  FROM fact_events AS f
  JOIN dim_product AS p ON f.product_id = p.product_id
),
agg AS (
  SELECT
    category_l1,
    category_l2,
    count(*) FILTER (WHERE event_type = 'view') AS views,
    count(*) FILTER (WHERE event_type = 'cart') AS carts,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue,
    count(DISTINCT user_session) FILTER (WHERE event_type = 'view') AS view_sessions,
    count(DISTINCT user_session) FILTER (WHERE event_type = 'purchase') AS purchase_sessions
  FROM base
  GROUP BY category_l1, category_l2
),
scored AS (
  SELECT
    category_l1,
    category_l2,
    category_l1 || ' / ' || category_l2 AS category,
    views,
    carts,
    purchases,
    round(revenue, 2) AS revenue,
    round(revenue / nullif(sum(revenue) OVER (), 0), 6) AS revenue_share,
    rank() OVER (ORDER BY revenue DESC, category_l1 ASC, category_l2 ASC) AS revenue_rank,
    row_number() OVER (
      PARTITION BY category_l2 = 'unknown'
      ORDER BY revenue DESC, category_l1 ASC, category_l2 ASC
    ) AS known_rank,
    view_sessions,
    purchase_sessions,
    round(purchases * 1.0 / nullif(views, 0), 6) AS purchase_per_view,
    round(purchase_sessions * 1.0 / nullif(view_sessions, 0), 6) AS session_purchase_per_view,
    round(revenue / nullif(purchases, 0), 2) AS aov
  FROM agg
)
SELECT
  category_l1,
  category_l2,
  category,
  views,
  carts,
  purchases,
  revenue,
  revenue_share,
  revenue_rank,
  CASE WHEN category_l2 = 'unknown' THEN NULL ELSE known_rank END AS known_rank,
  view_sessions,
  purchase_sessions,
  purchase_per_view,
  session_purchase_per_view,
  aov
FROM scored
WHERE category_l2 = 'unknown' OR known_rank <= 15
ORDER BY revenue DESC, category_l1 ASC, category_l2 ASC;

-- name: q05_brands
-- Top brands by revenue. brand is the event brand on fact_events.
WITH agg AS (
  SELECT
    brand,
    count(*) FILTER (WHERE event_type = 'view') AS views,
    count(*) FILTER (WHERE event_type = 'cart') AS carts,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue
  FROM fact_events
  GROUP BY brand
),
scored AS (
  SELECT
    brand,
    views,
    carts,
    purchases,
    round(revenue, 2) AS revenue,
    round(revenue / nullif(sum(revenue) OVER (), 0), 6) AS revenue_share,
    rank() OVER (ORDER BY revenue DESC, brand ASC) AS revenue_rank,
    row_number() OVER (
      PARTITION BY brand = 'unknown'
      ORDER BY revenue DESC, brand ASC
    ) AS known_rank,
    round(purchases * 1.0 / nullif(views, 0), 6) AS purchase_per_view,
    round(purchases * 1.0 / nullif(carts, 0), 6) AS purchase_per_cart,
    round(revenue / nullif(purchases, 0), 2) AS aov
  FROM agg
)
SELECT
  brand,
  views,
  carts,
  purchases,
  revenue,
  revenue_share,
  revenue_rank,
  CASE WHEN brand = 'unknown' THEN NULL ELSE known_rank END AS known_rank,
  purchase_per_view,
  purchase_per_cart,
  aov
FROM scored
WHERE brand = 'unknown' OR known_rank <= 15
ORDER BY revenue DESC, brand ASC;

-- name: q06_hourly
-- UTC hour of traffic and purchases. Missing hours stay in the result with zeros.
WITH hours AS (
  SELECT unnest(range(24)) AS hour_utc
),
agg AS (
  SELECT
    hour(event_time) AS hour_utc,
    count(*) AS events,
    count(*) FILTER (WHERE event_type = 'view') AS views,
    count(*) FILTER (WHERE event_type = 'cart') AS carts,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue
  FROM fact_events
  GROUP BY hour(event_time)
)
SELECT
  hours.hour_utc,
  coalesce(agg.events, 0) AS events,
  coalesce(agg.views, 0) AS views,
  coalesce(agg.carts, 0) AS carts,
  coalesce(agg.purchases, 0) AS purchases,
  round(coalesce(agg.revenue, 0), 2) AS revenue,
  round(coalesce(agg.events, 0) * 1.0 / sum(coalesce(agg.events, 0)) OVER (), 6) AS event_share,
  round(
    coalesce(agg.purchases, 0) * 1.0 / nullif(sum(coalesce(agg.purchases, 0)) OVER (), 0),
    6
  ) AS purchase_share,
  round(
    coalesce(agg.revenue, 0) / nullif(sum(coalesce(agg.revenue, 0)) OVER (), 0),
    6
  ) AS revenue_share
FROM hours
LEFT JOIN agg ON hours.hour_utc = agg.hour_utc
ORDER BY hours.hour_utc;

-- name: q07_dow
-- Traffic and revenue by UTC weekday, plus weekend and weekday rollups.
-- The rollup counts sessions from the events, so a session on both Saturday
-- and Sunday counts once in Weekend. Day rows still count a session on each
-- weekday it touches. revenue_share uses the seven day rows as the denominator.
WITH base AS (
  SELECT
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    f.user_session,
    f.event_type,
    f.price,
    f.is_non_positive_price
  FROM fact_events AS f
  JOIN dim_date AS d ON f.event_date = d.event_date
),
by_day AS (
  SELECT
    day_of_week,
    any_value(day_name) AS day_name,
    any_value(is_weekend) AS is_weekend,
    count(*) AS events,
    count(DISTINCT user_session) AS sessions,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue
  FROM base
  GROUP BY day_of_week
),
by_weekend AS (
  SELECT
    is_weekend,
    count(*) AS events,
    count(DISTINCT user_session) AS sessions,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue
  FROM base
  GROUP BY is_weekend
),
stacked AS (
  SELECT
    day_of_week,
    day_name,
    is_weekend,
    events,
    sessions,
    purchases,
    revenue,
    TRUE AS is_day
  FROM by_day
  UNION ALL
  SELECT
    CASE WHEN is_weekend THEN 8 ELSE 9 END,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END,
    is_weekend,
    events,
    sessions,
    purchases,
    revenue,
    FALSE
  FROM by_weekend
)
SELECT
  day_of_week,
  day_name,
  is_weekend,
  events,
  sessions,
  purchases,
  round(revenue, 2) AS revenue,
  round(
    revenue / nullif(sum(CASE WHEN is_day THEN revenue END) OVER (), 0),
    6
  ) AS revenue_share,
  round(purchases * 1.0 / nullif(events, 0), 6) AS purchase_per_event,
  round(revenue / nullif(purchases, 0), 2) AS aov
FROM stacked
ORDER BY day_of_week;

-- name: q08_purchase_price_quantiles
-- Eligible purchase prices. These are the distribution the price bands sit on.
SELECT
  count(*) AS purchases,
  round(min(price), 2) AS min_price,
  round(quantile_cont(price, 0.25), 2) AS p25,
  round(quantile_cont(price, 0.50), 2) AS p50,
  round(quantile_cont(price, 0.75), 2) AS p75,
  round(quantile_cont(price, 0.90), 2) AS p90,
  round(quantile_cont(price, 0.99), 2) AS p99,
  round(max(price), 2) AS max_price,
  round(avg(price), 2) AS mean_price
FROM fact_events
WHERE event_type = 'purchase' AND NOT is_non_positive_price;

-- name: build_cart_pairs
-- One row per session and product that was carted.
-- Abandoned means that same session has no purchase event for that product.
-- cart_price is the maximum cart-event price for the pair.
CREATE OR REPLACE TEMP TABLE cart_pairs AS
WITH carted AS (
  SELECT
    user_session,
    product_id,
    count(*) AS cart_events,
    max(price) AS cart_price
  FROM fact_events
  WHERE event_type = 'cart'
  GROUP BY user_session, product_id
),
purchased AS (
  SELECT
    user_session,
    product_id,
    count(*) AS purchase_events
  FROM fact_events
  WHERE event_type = 'purchase'
  GROUP BY user_session, product_id
)
SELECT
  carted.user_session,
  carted.product_id,
  carted.cart_events,
  carted.cart_price,
  coalesce(purchased.purchase_events, 0) AS purchase_events,
  coalesce(purchased.purchase_events, 0) = 0 AS abandoned,
  product.category_l1,
  CASE
    WHEN carted.cart_price <= 0 THEN '00_non_positive'
    ELSE price_band(carted.cart_price)
  END AS band_key
FROM carted
LEFT JOIN purchased
  ON carted.user_session = purchased.user_session
 AND carted.product_id = purchased.product_id
JOIN dim_product AS product ON carted.product_id = product.product_id;

-- name: q09_abandon_overall
-- Cart abandonment at session-product grain.
SELECT
  count(*) AS cart_pairs,
  count(*) FILTER (WHERE abandoned) AS abandoned_pairs,
  count(*) FILTER (WHERE NOT abandoned) AS purchased_pairs,
  round(count(*) FILTER (WHERE abandoned) * 1.0 / count(*), 6) AS abandon_rate,
  sum(cart_events) AS cart_events,
  count(DISTINCT user_session) AS cart_sessions,
  count(DISTINCT user_session) FILTER (WHERE abandoned) AS sessions_with_an_abandoned_pair,
  round(sum(cart_price) FILTER (WHERE abandoned), 2) AS abandoned_cart_value,
  round(sum(cart_price) FILTER (WHERE NOT abandoned), 2) AS purchased_cart_value,
  round(quantile_cont(cart_price, 0.5) FILTER (WHERE abandoned), 2) AS abandoned_median_cart_price,
  round(quantile_cont(cart_price, 0.5) FILTER (WHERE NOT abandoned), 2) AS purchased_median_cart_price
FROM cart_pairs;

-- name: q10_abandon_category
-- Abandonment by category_l1, ordered by abandoned cart value.
SELECT
  category_l1,
  count(*) AS cart_pairs,
  count(*) FILTER (WHERE abandoned) AS abandoned_pairs,
  round(count(*) FILTER (WHERE abandoned) * 1.0 / count(*), 6) AS abandon_rate,
  round(sum(cart_price) FILTER (WHERE abandoned), 2) AS abandoned_cart_value,
  round(
    sum(cart_price) FILTER (WHERE abandoned)
      / nullif(sum(sum(cart_price) FILTER (WHERE abandoned)) OVER (), 0),
    6
  ) AS abandoned_value_share,
  round(avg(cart_price) FILTER (WHERE abandoned), 2) AS avg_abandoned_cart_price
FROM cart_pairs
GROUP BY category_l1
ORDER BY abandoned_cart_value DESC, category_l1 ASC;

-- name: q11_abandon_price_band
-- Abandonment by the cart price band. Every band is present.
WITH bands AS (
  SELECT *
  FROM (
    VALUES
      ('00_non_positive', 'non-positive', 0),
      ('01_under_50', 'under 50', 1),
      ('02_50_to_150', '50 to 150', 2),
      ('03_150_to_300', '150 to 300', 3),
      ('04_300_to_600', '300 to 600', 4),
      ('05_600_to_1000', '600 to 1000', 5),
      ('06_1000_and_above', '1000 and above', 6)
  ) AS t(band_key, band_label, band_order)
),
agg AS (
  SELECT
    band_key,
    count(*) AS cart_pairs,
    count(*) FILTER (WHERE abandoned) AS abandoned_pairs,
    sum(cart_price) FILTER (WHERE abandoned) AS abandoned_cart_value
  FROM cart_pairs
  GROUP BY band_key
)
SELECT
  bands.band_order,
  bands.band_key,
  bands.band_label,
  coalesce(agg.cart_pairs, 0) AS cart_pairs,
  coalesce(agg.abandoned_pairs, 0) AS abandoned_pairs,
  round(
    coalesce(agg.abandoned_pairs, 0) * 1.0 / nullif(agg.cart_pairs, 0),
    6
  ) AS abandon_rate,
  round(coalesce(agg.abandoned_cart_value, 0), 2) AS abandoned_cart_value
FROM bands
LEFT JOIN agg ON bands.band_key = agg.band_key
ORDER BY bands.band_order;

-- name: q12_price_band_revenue
-- Eligible purchase revenue by the purchase price band.
WITH bands AS (
  SELECT *
  FROM (
    VALUES
      ('00_non_positive', 'non-positive', 0),
      ('01_under_50', 'under 50', 1),
      ('02_50_to_150', '50 to 150', 2),
      ('03_150_to_300', '150 to 300', 3),
      ('04_300_to_600', '300 to 600', 4),
      ('05_600_to_1000', '600 to 1000', 5),
      ('06_1000_and_above', '1000 and above', 6)
  ) AS t(band_key, band_label, band_order)
),
agg AS (
  SELECT
    CASE
      WHEN price <= 0 THEN '00_non_positive'
      ELSE price_band(price)
    END AS band_key,
    count(*) AS purchases,
    sum(price) AS revenue
  FROM fact_events
  WHERE event_type = 'purchase' AND NOT is_non_positive_price
  GROUP BY 1
)
SELECT
  bands.band_order,
  bands.band_key,
  bands.band_label,
  coalesce(agg.purchases, 0) AS purchases,
  round(coalesce(agg.revenue, 0), 2) AS revenue,
  round(coalesce(agg.purchases, 0) * 1.0 / nullif(sum(coalesce(agg.purchases, 0)) OVER (), 0), 6) AS purchase_share,
  round(coalesce(agg.revenue, 0) / nullif(sum(coalesce(agg.revenue, 0)) OVER (), 0), 6) AS revenue_share,
  round(coalesce(agg.revenue, 0) / nullif(agg.purchases, 0), 2) AS aov
FROM bands
LEFT JOIN agg ON bands.band_key = agg.band_key
ORDER BY bands.band_order;

-- name: q13_daily_revenue
-- Daily eligible revenue and a trailing 7-date average.
-- ma_days is the number of dates in the window. The first six dates have a
-- shorter window. revenue_ma7_full is null on those dates.
WITH daily AS (
  SELECT
    d.event_date,
    d.day_name,
    d.is_weekend,
    count(f.event_time) FILTER (
      WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
    ) AS purchases,
    coalesce(
      sum(f.price) FILTER (
        WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
      ),
      0
    ) AS revenue
  FROM dim_date AS d
  LEFT JOIN fact_events AS f ON d.event_date = f.event_date
  GROUP BY d.event_date, d.day_name, d.is_weekend
),
windowed AS (
  SELECT
    event_date,
    day_name,
    is_weekend,
    purchases,
    revenue,
    avg(revenue) OVER (
      ORDER BY event_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS revenue_ma7,
    count(*) OVER (
      ORDER BY event_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_days
  FROM daily
)
SELECT
  event_date,
  day_name,
  is_weekend,
  purchases,
  round(revenue, 2) AS revenue,
  round(revenue_ma7, 2) AS revenue_ma7,
  ma_days,
  CASE WHEN ma_days = 7 THEN round(revenue_ma7, 2) END AS revenue_ma7_full
FROM windowed
ORDER BY event_date;

-- name: build_buyer_rfm
-- One row per buyer. Recency is days from the last purchase date to 2019-10-31.
-- Frequency for the NTILE is purchase sessions. Monetary is eligible revenue.
-- r_score 5 is the most recent tile. f_score 5 and m_score 5 are the highest tiles.
-- Ties break on user_id so a re-run assigns the same score.
CREATE OR REPLACE TEMP TABLE buyer_rfm AS
WITH purchases AS (
  SELECT
    user_id,
    max(event_date) AS last_purchase_date,
    count(DISTINCT user_session) AS purchase_sessions,
    count(*) AS purchase_events,
    sum(price) FILTER (WHERE NOT is_non_positive_price) AS monetary
  FROM fact_events
  WHERE event_type = 'purchase'
  GROUP BY user_id
),
scored AS (
  SELECT
    user_id,
    last_purchase_date,
    date_diff('day', last_purchase_date, DATE '2019-10-31') AS recency_days,
    purchase_sessions,
    purchase_events,
    monetary,
    6 - ntile(5) OVER (
      ORDER BY date_diff('day', last_purchase_date, DATE '2019-10-31') ASC, user_id ASC
    ) AS r_score,
    ntile(5) OVER (
      ORDER BY purchase_sessions ASC, purchase_events ASC, user_id ASC
    ) AS f_score,
    ntile(5) OVER (
      ORDER BY monetary ASC, user_id ASC
    ) AS m_score
  FROM purchases
)
SELECT
  user_id,
  last_purchase_date,
  recency_days,
  purchase_sessions,
  purchase_events,
  monetary,
  r_score,
  f_score,
  m_score,
  rfm_segment(r_score, m_score, purchase_sessions) AS segment
FROM scored;

-- name: q14_rfm_segments
-- Segment size and revenue share. Labels are the rfm_segment macro.
SELECT
  segment,
  count(*) AS buyers,
  round(count(*) * 1.0 / sum(count(*)) OVER (), 6) AS buyer_share,
  round(sum(monetary), 2) AS revenue,
  round(sum(monetary) / sum(sum(monetary)) OVER (), 6) AS revenue_share,
  round(avg(recency_days), 2) AS avg_recency_days,
  min(recency_days) AS min_recency_days,
  max(recency_days) AS max_recency_days,
  round(avg(purchase_sessions), 2) AS avg_purchase_sessions,
  min(purchase_sessions) AS min_purchase_sessions,
  max(purchase_sessions) AS max_purchase_sessions,
  round(avg(monetary), 2) AS avg_monetary
FROM buyer_rfm
GROUP BY segment
ORDER BY revenue DESC, segment ASC;

-- name: q15_rfm_score_bounds
-- Observed range inside each NTILE. This shows where ties were split.
SELECT
  'recency_days' AS metric,
  r_score AS score,
  count(*) AS buyers,
  min(recency_days)::DOUBLE AS min_value,
  max(recency_days)::DOUBLE AS max_value,
  round(avg(recency_days), 2) AS avg_value
FROM buyer_rfm
GROUP BY r_score
UNION ALL
SELECT
  'purchase_sessions',
  f_score,
  count(*),
  min(purchase_sessions)::DOUBLE,
  max(purchase_sessions)::DOUBLE,
  round(avg(purchase_sessions), 2)
FROM buyer_rfm
GROUP BY f_score
UNION ALL
SELECT
  'monetary',
  m_score,
  count(*),
  round(min(monetary), 2),
  round(max(monetary), 2),
  round(avg(monetary), 2)
FROM buyer_rfm
GROUP BY m_score
ORDER BY metric ASC, score ASC;

-- name: q16_frequency_hist
-- Buyers and revenue by purchase-session count. 5 and above are one bucket.
WITH grouped AS (
  SELECT
    CASE
      WHEN purchase_sessions >= 5 THEN 5
      ELSE purchase_sessions
    END AS session_bucket,
    count(*) AS buyers,
    sum(monetary) AS revenue
  FROM buyer_rfm
  GROUP BY 1
)
SELECT
  CASE
    WHEN session_bucket = 5 THEN '5+'
    ELSE session_bucket::VARCHAR
  END AS purchase_sessions,
  buyers,
  round(buyers * 1.0 / sum(buyers) OVER (), 6) AS buyer_share,
  round(revenue, 2) AS revenue,
  round(revenue / sum(revenue) OVER (), 6) AS revenue_share
FROM grouped
ORDER BY session_bucket;

-- name: q17_repeat_buyers
-- One purchase session against every repeat buyer.
SELECT
  CASE
    WHEN purchase_sessions = 1 THEN 'one purchase session'
    ELSE 'two or more purchase sessions'
  END AS buyer_group,
  count(*) AS buyers,
  round(count(*) * 1.0 / sum(count(*)) OVER (), 6) AS buyer_share,
  round(sum(monetary), 2) AS revenue,
  round(sum(monetary) / sum(sum(monetary)) OVER (), 6) AS revenue_share,
  round(avg(monetary), 2) AS avg_monetary,
  round(avg(purchase_sessions), 2) AS avg_purchase_sessions
FROM buyer_rfm
GROUP BY 1
ORDER BY 1;

-- name: q18_reconcile
-- Internal checks. Gaps are absolute differences. The runner fails when a gap is large.
WITH fact AS (
  SELECT
    count(*) AS fact_rows,
    count(*) FILTER (WHERE event_type = 'cart') AS cart_events,
    count(*) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS purchases,
    count(DISTINCT user_session) AS fact_sessions,
    count(DISTINCT user_id) FILTER (WHERE event_type = 'purchase') AS buyers,
    sum(price) FILTER (
      WHERE event_type = 'purchase' AND NOT is_non_positive_price
    ) AS revenue
  FROM fact_events
),
session_tot AS (
  SELECT count(*) AS session_rows, sum(revenue) AS revenue
  FROM sessions
),
day_tot AS (
  SELECT sum(price) FILTER (
    WHERE event_type = 'purchase' AND NOT is_non_positive_price
  ) AS revenue
  FROM fact_events
  GROUP BY event_date
),
category_tot AS (
  SELECT sum(f.price) FILTER (
    WHERE f.event_type = 'purchase' AND NOT f.is_non_positive_price
  ) AS revenue
  FROM fact_events AS f
  JOIN dim_product AS p ON f.product_id = p.product_id
),
brand_tot AS (
  SELECT sum(price) FILTER (
    WHERE event_type = 'purchase' AND NOT is_non_positive_price
  ) AS revenue
  FROM fact_events
  GROUP BY brand
),
band_tot AS (
  SELECT sum(price) AS revenue
  FROM fact_events
  WHERE event_type = 'purchase' AND NOT is_non_positive_price
),
rfm_tot AS (
  SELECT count(*) AS buyers, sum(monetary) AS revenue
  FROM buyer_rfm
),
cart_tot AS (
  SELECT sum(cart_events) AS cart_events
  FROM cart_pairs
)
SELECT
  fact.fact_rows,
  session_tot.session_rows,
  fact.fact_sessions,
  fact.purchases,
  fact.buyers AS purchase_buyers,
  rfm_tot.buyers AS rfm_buyers,
  rfm_tot.buyers - fact.buyers AS buyer_gap,
  cart_tot.cart_events AS cart_pair_events,
  fact.cart_events,
  cart_tot.cart_events - fact.cart_events AS cart_event_gap,
  round(fact.revenue, 2) AS fact_revenue,
  abs(fact.revenue - session_tot.revenue) AS session_revenue_gap,
  abs(fact.revenue - (SELECT sum(revenue) FROM day_tot)) AS day_revenue_gap,
  abs(fact.revenue - category_tot.revenue) AS category_revenue_gap,
  abs(fact.revenue - (SELECT sum(revenue) FROM brand_tot)) AS brand_revenue_gap,
  abs(fact.revenue - band_tot.revenue) AS band_revenue_gap,
  abs(fact.revenue - rfm_tot.revenue) AS rfm_revenue_gap
FROM fact
CROSS JOIN session_tot
CROSS JOIN category_tot
CROSS JOIN band_tot
CROSS JOIN rfm_tot
CROSS JOIN cart_tot;
