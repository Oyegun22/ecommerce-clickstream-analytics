# Hiring-manager critique

Read as a portfolio review of this repository. The figures are the ones already published in `docs/key_findings.md` and `docs/analysis_results.md`.

## 1. RFM is a one-month ranking

Recency is days from the last purchase to 2019-10-31, so the maximum is 30. "Need attention" is 30,297 buyers whose last purchase fell 16 to 30 days before that date, with 29,633,223.38 of revenue. Champions are 27,453 buyers and 85,321,183.75 of revenue. Those labels sound like a live CRM program. Inside one October they are a sort of the same file.

Frequency scores 1, 2, and 3 are each 69,424 buyers with a purchase-session minimum and maximum of 1. The segment macro uses the raw session count, and the dashboard says so. A reader who only sees the word "champions" can still skip that sentence.

**Fix.** Keep the scores, and put "October 2019 only" in the segment chart title. Add a second month before any recommendation that treats the labels as a retention program. Until then, lead with the raw repeat split: 111,657 buyers with two or more purchase sessions and 159,793,398.94 of revenue.

## 2. The clock is UTC, and the business hours are still easy to quote

Event volume peaks at 16:00 UTC (3,053,226 events). Purchases peak at 08:00 UTC (55,195). Revenue peaks at 09:00 UTC (17,551,669.73). The file does not contain a store offset. The docs say that. A screenshot of the heatmap can still be read as local time.

**Fix.** Keep the axis labeled "Hour UTC" on the chart itself, which the app already does, and leave staffing recommendations out of the README until a timezone source exists. If REES46 publishes the shop region, convert the three peaks and show both clocks.

## 3. Unknown category is large enough to move a target

Unknown category is second at level 1: 173,411 purchases, 22,922,165.08, share 0.099691. Unknown brand is fourth: 58,210 purchases, 8,539,747.08, share 0.037140. Category is one tuple per `product_id`, so the missing code is a property of the product, and filling it from another event of the same product will not work.

**Fix.** Say that limitation in the category chart caption, next to the orange unknown bar. The next analysis step is an external catalog on `category_id`, or an explicit decision to keep the bucket in every target. Rankings of the known tree should carry the unknown share beside them.

## 4. Users and sessions are easy to over-count

348 sessions contain more than one `user_id`. The session row keeps the dominant user. The fact keeps the user on each event. Summing `dim_user.n_sessions` gives 9,244,770 against 9,244,421 sessions. The dashboard is exact for the full month (3,022,290 users) and for a single day. A multi-day slice shows the average of daily distinct counts, because those counts are not additive. The label says so. A KPI card can still be copied as "users in this range."

**Fix.** Show the user card for the full month and for one day. For any other range, replace the number with the sentence that an exact distinct count was not pre-aggregated. Publish the 348 multi-user sessions next to the session grain in the README data-model section, with the rule for the dominant user.

## 5. A reviewer cannot click the finished product yet

`make all` rebuilds bronze through the extracts on this machine, and the last run validated revenue at 229,933,212.63. The GitHub remote is not set. Commits are authored as `Apple <apple@local>`. The README still points at `https://YOUR_STREAMLIT_APP_URL`, and `docs/screenshots/` has no images. The notebook is outside `make all`. The two dashboard exports were 353,617 bytes and then 354,128 bytes, with the same validated totals, so the Parquet bytes are a weak checksum.

**Fix.** Set a real Git author, push the repository, deploy `dashboard/app.py` with `dashboard/requirements.txt` on Streamlit Community Cloud, and add the three screenshots named in the README. Add a small CI job that runs `src/05_run_analysis.py --self-test` and the dashboard filter checks. Keep the full Spark run as a manual `make all`. Pin the dashboard check to revenue, purchases, and users, and treat the byte count as a size cap under 50 MB.
