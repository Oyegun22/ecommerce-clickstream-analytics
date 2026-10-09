# Project brief

Build a reproducible analysis of the REES46 October 2019 clickstream. The file is one month of storefront events. The work product is a medallion warehouse, a documented metric layer, and a small dashboard that can be hosted without the raw file.

## Question

Where does October revenue come from, which sessions buy without a cart, which carted products are abandoned, and which buyers account for the revenue inside this month?

## Data

`data/raw/2019-Oct.csv` has the columns `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, and `user_session`. `event_type` is `view`, `cart`, or `purchase`. The raw file is 5,668,612,855 bytes. It stays out of git. The only committed data is the dashboard extracts under `dashboard/data`.

## Metric definitions

- Revenue is the sum of purchase prices where `is_non_positive_price` is false.
- Category comes from `dim_product`. On this file, `product_id` has one category.
- Brand revenue uses `fact_events.brand`, the brand on the event. The product dimension stores the modal brand and is the wrong grain for brand revenue.
- A cart pair is one product carted in one session. It is abandoned when that session has no purchase of the same product. Abandoned value is the maximum cart price on the pair.
- RFM is fit on October buyers only. Recency is days from the last purchase date to 2019-10-31. Segment labels use the raw purchase-session count because frequency scores 1, 2, and 3 are all one session.
- Hours and dates are UTC. The file has no store time zone.

## Deliverables

Numbered scripts in `src/` land bronze, profile it, clean silver, build the gold star schema, run `sql/analysis.sql`, and export the dashboard. The narrative results are `docs/key_findings.md`, `notebooks/eda.ipynb`, and `dashboard/app.py`.

## Boundaries

The model covers 2019-10-01 through 2019-10-31. Recency is position inside that month. Sessions that contain more than one `user_id` keep a single dominant user on the session row, and the fact keeps each event's own user. Spark runs locally with a driver heap of at most 12g.

The copyright line in `LICENSE` uses the local author name recorded on the commits in this workspace. Replace that name before a public release.
