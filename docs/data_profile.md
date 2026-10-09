# Bronze data profile

Source: `data/bronze/events`, profiled 2026-10-09 12:28:13 UTC by `src/02_profile.py` on Spark 3.5.3. Median and p99 are Spark SQL `percentile` (exact, with interpolation). Session stats exclude null `user_session` values. User stats exclude null `user_id` values.

## Population

| Measure | Value |
| --- | --- |
| Events | 42,448,764 |
| First event_date | 2019-10-01 |
| Last event_date | 2019-10-31 |
| Distinct event_date | 31 |
| Distinct user_id | 3,022,290 |
| Distinct user_session | 9,244,421 |
| Distinct product_id | 166,794 |
| Distinct category_id | 624 |
| Distinct category_code | 126 |
| Distinct brand | 3,445 |

The month contains 42,448,764 events from 3,022,290 users, 9,244,421 sessions, and 166,794 products.

## Event type mix

| event_type | Events | Share |
| --- | --- | --- |
| view | 40,779,399 | 96.07% |
| cart | 926,516 | 2.18% |
| purchase | 742,849 | 1.75% |
| other | 0 | 0% |

Views are 96.07% of events, carts 2.18%, and purchases 1.75%; no other event_type is present.

## Null rates

| Column | Null rows | Null rate | Empty-string rows |
| --- | --- | --- | --- |
| event_time | 0 | 0% | n/a |
| event_type | 0 | 0% | 0 |
| product_id | 0 | 0% | n/a |
| category_id | 0 | 0% | n/a |
| category_code | 13,515,609 | 31.84% | 0 |
| brand | 6,113,008 | 14.40% | 0 |
| price | 0 | 0% | n/a |
| user_id | 0 | 0% | n/a |
| user_session | 2 | 0.00000471% | 0 |
| event_date | 0 | 0% | n/a |

Nulls occur only in category_code (13,515,609 rows, 31.84%), brand (6,113,008 rows, 14.40%), user_session (2 rows, 0.00000471%).

## Price

Median, mean, and p99 are computed on non-null prices.

| Measure | Value |
| --- | --- |
| Min | 0.00 |
| Median | 162.93 |
| Mean | 290.3237 |
| p99 | 1,741.34 |
| Max | 2,574.07 |
| Null price rows | 0 |
| Price < 0 | 0 |
| Price = 0 | 68,673 |
| Price > 0 | 42,380,091 |

The median price is 162.93 against a mean of 290.3237 and a p99 of 1,741.34, so the distribution is right-skewed, and 68,673 events are priced at exactly 0 and none are negative.

| event_type | Events with price <= 0 | Min price | Max price |
| --- | --- | --- | --- |
| cart | 150 | 0.00 | 0.00 |
| view | 68,523 | 0.00 | 0.00 |

The 68,673 non-positive prices are split across event types as cart 150, view 68,523.

## Exact duplicates

A duplicate group is a set of rows equal on `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, and `user_session`. Extra rows are the copies a `dropDuplicates` on those columns would remove (`sum(count - 1)` over groups with `count > 1`).

| Measure | Value |
| --- | --- |
| Duplicate groups | 18,099 |
| Rows in those groups | 48,319 |
| Extra rows | 30,220 |
| Distinct source rows | 42,418,544 |
| Extra rows as a share of events | 0.07% |

Exact matches on the nine source columns produce 18,099 repeated groups and 30,220 extra rows (0.07% of events).

## Category code shape

Depth is the number of pieces after splitting `category_code` on `.`. Missing codes are null, not depth 0. The example is the most common code at that depth.

| Depth | Rows | Distinct codes | Empty-token rows | Most common code |
| --- | --- | --- | --- | --- |
| missing | 13,515,609 | 0 | 0 | — |
| 2 | 17,356,435 | 41 | 0 | electronics.smartphone |
| 3 | 11,540,478 | 84 | 0 | electronics.video.tv |
| 4 | 36,242 | 1 | 0 | electronics.audio.music_tools.piano |

Non-null category codes are depth 2 (17,356,435 rows), depth 3 (11,540,478 rows), depth 4 (36,242 rows), and 0 rows contain an empty token between dots.

## Brand text

| Measure | Value |
| --- | --- |
| Null brand rows | 6,113,008 |
| Empty-string brand rows | 0 |
| Rows with surrounding whitespace | 0 |
| Rows already trimmed but not lowercase | 0 |
| Rows that lowercase+trim would change | 0 |
| Distinct brand strings | 3,445 |
| Distinct brand strings after lowercase+trim | 3,445 |

Every non-null brand is already trimmed and lowercase, so the distinct brand count stays 3,445.

## Sessions per user

A user's session count is the number of distinct non-null `user_session` values on that `user_id`. A count of 0 means every event for that user has a null session. Bucket edges are fixed cuts.

| Measure | Value |
| --- | --- |
| Users | 3,022,290 |
| Min sessions | 1 |
| Median sessions | 2 |
| Mean sessions | 3.0589 |
| p99 sessions | 21 |
| Max sessions | 7,400 |

| Sessions per user | Users | Share of users |
| --- | --- | --- |
| 0 | 0 | 0% |
| 1 | 1,475,463 | 48.82% |
| 2 | 565,518 | 18.71% |
| 3-5 | 591,116 | 19.56% |
| 6-10 | 251,193 | 8.31% |
| 11-20 | 105,182 | 3.48% |
| 21+ | 33,818 | 1.12% |

The median user has 2 distinct sessions, 48.82% of users have exactly one, and the busiest user has 7,400.

## Events per session

Null `user_session` rows are excluded from this table and counted below.

| Measure | Value |
| --- | --- |
| Sessions | 9,244,421 |
| Events in those sessions | 42,448,762 |
| Null user_session rows excluded | 2 |
| Min events | 1 |
| Median events | 2 |
| Mean events | 4.5918 |
| p99 events | 32 |
| Max events | 1,159 |
| Sessions with more than one user_id | 348 |
| Sessions with no user_id | 0 |

| Events per session | Sessions | Share of sessions |
| --- | --- | --- |
| 1 | 3,269,577 | 35.37% |
| 2-5 | 3,804,645 | 41.16% |
| 6-20 | 1,901,202 | 20.57% |
| 21-50 | 243,632 | 2.64% |
| 51-200 | 25,253 | 0.27% |
| 201+ | 112 | 0.0012% |

The median session has 2 events, the longest has 1,159, and 348 sessions are tied to more than one user_id.

## Sessions that contain a purchase

| Measure | Value |
| --- | --- |
| Sessions | 9,244,421 |
| Sessions with at least one purchase | 629,560 |
| Share of sessions | 6.81% |
| Purchase events | 742,849 |

629,560 sessions contain at least one purchase (6.81% of non-null sessions); this is co-occurrence inside the session, not an ordered funnel.
