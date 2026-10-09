# Silver cleaning log

Source: `data/bronze/events`. Output: `data/silver/events`, partitioned by `event_date`.
Run: 2026-10-09 20:31:52 UTC on Spark 3.5.3. Elapsed 93.3 seconds.
Removed is rows deleted by the rule. Affected is rows the rule rewrote or flagged.
For the two drop rules, affected equals removed. Counts are from this run.

## Rules

| Step | Rule | Rows in | Rows out | Removed | Affected |
| --- | --- | --- | --- | --- | --- |
| 1 | standardise_brand | 42,448,764 | 42,448,764 | 0 | 0 |
| 2 | drop_exact_duplicates | 42,448,764 | 42,418,544 | 30,220 | 30,220 |
| 3 | drop_null_user_keys | 42,418,544 | 42,418,542 | 2 | 2 |
| 4 | flag_non_positive_price | 42,418,542 | 42,418,542 | 0 | 68,670 |
| 5 | fill_unknown_brand | 42,418,542 | 42,418,542 | 0 | 6,112,126 |
| 6 | fill_unknown_category_code | 42,418,542 | 42,418,542 | 0 | 13,512,262 |
| 7 | add_category_levels | 42,418,542 | 42,418,542 | 0 | 30,884,455 |

Bronze rows 42,448,764. Silver rows 42,418,542. Removed 30,222.

### 1. standardise_brand

Brand is a grouping key. Lowercase and trim make case and whitespace variants one value. Null stays null so the fill rule can see it. The rule runs even when the affected count is 0, so a later month with mixed case takes the same path. It runs before the duplicate drop so case variants are compared as one key.

### 2. drop_exact_duplicates

A second row that matches on event_time, event_type, product_id, category_id, category_code, brand, price, user_id, and user_session is a copy of the same event, not a new action. dropDuplicates keeps one. The copies are identical, so which copy survives does not matter. event_date is left out of the key because it is derived from event_time.

### 3. drop_null_user_keys

user_id and user_session are the keys for the user and session grains. Filling a null session with unknown would merge unrelated events into one session. A null user_id or a null or blank user_session is dropped. Session ids that are present are not rewritten.

### 4. flag_non_positive_price

Flagged with is_non_positive_price and kept. This run has 0 negative prices and 68,670 zero prices (view 68,521, cart 149, purchase 0, other 0). Dropping those rows would remove views and carts from the funnel. Revenue queries can ignore the flag. The original price is unchanged.

### 5. fill_unknown_brand

A missing brand is unknown metadata, not a reason to drop the event. The fill is the literal unknown, after standardisation, so it is already lowercase. Rows whose brand was already unknown before the fill: 0.

### 6. fill_unknown_category_code

A missing category_code is unknown metadata, same as brand. The row stays. The fill is the literal unknown. Rows whose category_code was already unknown before the fill: 0.

### 7. add_category_levels

Level 1 is the first dot-separated piece. Level 2 is the second piece, or unknown when the code is shorter. Level 3 is the third piece when there are exactly three, the third piece onward joined with dots when there are four or more, and unknown when the code has fewer than three pieces. Joining the tail keeps a leaf such as piano on electronics.audio.music_tools.piano. Missing levels are the literal unknown, not null, so a group-by never sees a null level. Affected counts codes that are not already exactly three pieces, which is where a blind split would be wrong.

## Price <= 0 by event_type

Counted on the rows that survived the duplicate drop and the key drop, before the flag was written.

| event_type | Rows with price <= 0 |
| --- | --- |
| view | 68,521 |
| cart | 149 |
| purchase | 0 |
| other | 0 |
| price < 0 | 0 |
| price = 0 | 68,670 |
| null price | 0 |

These rows stay in silver. `is_non_positive_price` is true exactly when price is not null and price <= 0.

## Category depth before the fill

Depth is the number of pieces after splitting the original category_code on `.`. Missing includes null and blank. The fill turns missing into the literal unknown, which is one piece, so this table is the pre-fill shape.

| Depth | Rows | How level 3 is set |
| --- | --- | --- |
| missing | 13,512,262 | category_l1, category_l2, and category_l3 are unknown |
| 1 | 0 | category_l2 and category_l3 are unknown |
| 2 | 17,335,969 | category_l3 is unknown |
| 3 | 11,534,087 | category_l3 is the third piece |
| 4 or more | 36,224 | category_l3 is the tail joined with dots |

Maximum depth of a non-missing code: 4.

## Checks after write

| Check | Value |
| --- | --- |
| Silver rows | 42,418,542 |
| Null brand, category_code, category levels, user_id | 0 |
| Blank user_session | 0 |
| Flag mismatches | 0 |
| Flagged rows | 68,670 |
| Brand values still needing lower/trim | 0 |
| Depth-2 rows whose category_l3 is not unknown | 0 |
| Deep codes whose category_l3 is not the joined tail | 0 |
| `electronics.audio.music_tools.piano` rows | 36,224 |
| Those rows with levels electronics / audio / music_tools.piano | 36,224 |
| Distinct event_date | 31 |
| First event_date | 2019-10-01 |
| Last event_date | 2019-10-31 |
| Parquet files | 31 |
| Silver folder bytes | 1,478,063,662 |
| Extra duplicate rows after clean | 0 |

## Left unchanged

Phase 3 measured 348 sessions tied to more than one user_id (`docs/data_profile.md`). Silver does not pick a winning user_id. The profile does not say which id is the real one.
