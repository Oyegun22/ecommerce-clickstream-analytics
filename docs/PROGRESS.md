# Progress log

Running notes for the REES46 October 2019 clickstream project. Every figure below comes from a command run in this workspace. Raw CSV under `data/raw/` is never modified.

## Phase 1 — Verify environment (2026-10-09)

### What was done

- Confirmed the project virtualenv is Python 3.11 and that PySpark 3.5.3 and DuckDB 1.5.6 import from it.
- Registered the existing Homebrew OpenJDK 17 so `/usr/libexec/java_home -v 17` can find it.
- Replaced the empty `src/00_check_env.py` with a re-runnable smoke test: a local Spark session counts `range(1_000_000)`, then DuckDB reports `version()`.
- Confirmed `data/raw/2019-Oct.csv` is present and recorded its size. The file was not opened beyond the header line.

### Commands

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
source .venv/bin/activate
java -version
python --version
python src/00_check_env.py
python -c "import os; print(os.path.getsize('data/raw/2019-Oct.csv'))"
```

JDK registration (user-level, no sudo), run once on this machine:

```bash
mkdir -p "$HOME/Library/Java/JavaVirtualMachines"
ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  "$HOME/Library/Java/JavaVirtualMachines/openjdk-17.jdk"
```

### Key numbers

| Check | Result |
| --- | --- |
| Java | OpenJDK 17.0.20.1 (2026-08-18), Homebrew build 17.0.20.1+0 |
| `JAVA_HOME` | `/opt/homebrew/Cellar/openjdk@17/17.0.20.1/libexec/openjdk.jdk/Contents/Home` |
| Python | 3.11.17 (`.venv/bin/python`) |
| PySpark package | 3.5.3 |
| Spark smoke test | `Spark version 3.5.3 rows: 1000000` |
| DuckDB | `DuckDB version v1.5.6` |
| Raw CSV path | `data/raw/2019-Oct.csv` |
| Raw CSV size | 5,668,612,855 bytes (5.6686 GB decimal, 5.2793 GiB) |
| CSV header | `event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session` |

Driver memory for the smoke test is `2g`. Later Spark jobs stay at or below `12g`.

### Problems and fixes

1. **`java_home` could not see a JDK.** `/usr/libexec/java_home -v 17` returned "Unable to locate a Java Runtime" even though Homebrew already had `openjdk@17` at `/opt/homebrew/opt/openjdk@17`. macOS only lists JDKs registered under a `JavaVirtualMachines` directory. Fixed with the user-level symlink above. After that, `java -version` prints 17.0.20.1.
2. **`src/00_check_env.py` was an empty file (0 bytes).** Wrote the smoke test described above. It does not read the raw CSV.
3. **`src/01_ingest_bronze.py` and `src/02_profile.py` are not in the tree.** The brief described them as already written. They will be created at the start of Phases 2 and 3.
4. **No git `user.name` or `user.email` is configured** on this machine (`~/.gitconfig` does not exist). The Phase 1 commit is authored from the local account so the history exists. Replace the author with your real name before pushing.
5. **Spark warnings, not failures.** Spark logged that `MacBook-Pro.local` resolves to `127.0.0.1` and that the native Hadoop library is missing. Both are expected in local mode on macOS. The count still returned 1,000,000.

### Ignored Spark leftovers

`.gitignore` already excluded `data/`, `.venv/`, and `spark-warehouse/`. Added `metastore_db/` and `derby.log` so a local Hive metastore from later Spark jobs cannot be committed. `dashboard/data/` is outside `data/` and remains committable for Phase 7.

## Phase 2 — Bronze ingestion (2026-10-09)

### What was done

- Wrote `src/01_ingest_bronze.py`. The file was not in the tree. The script reads `data/raw/2019-Oct.csv` with an explicit schema, parses `event_time` as UTC, adds `event_date`, and overwrites `data/bronze/events` as zstd Parquet partitioned by `event_date`.
- Smoke-tested on the first 20,000 data rows (copied to `/tmp`, not written under `data/`). That run parsed every timestamp, kept all 20,000 rows, and showed the JVM heap matching the requested `-Xmx`.
- Ran the full file. Driver heap was 12g (`maxMemory=12,884,901,888`) and the master was `local[6]`.
- Cross-checked the result with `wc -l` on the CSV and with a DuckDB count of the Parquet. Both agree with Spark. The raw file's size and mtime were unchanged.

### Commands

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
source .venv/bin/activate
python src/01_ingest_bronze.py --input /tmp/oct_head.csv --output /tmp/bronze-smoke --driver-memory 2g --master local[2]
python src/01_ingest_bronze.py
wc -l data/raw/2019-Oct.csv
```

DuckDB then counted `data/bronze/events/**/*.parquet` with `hive_partitioning=true`.

### Key numbers

| Check | Result |
| --- | --- |
| Rows written | 42,448,764 |
| CSV lines (`wc -l`) | 42,448,765 (header + 42,448,764 data rows) |
| Null `event_time` | 0 |
| Date partitions | 31 (`2019-10-01` through `2019-10-31`) |
| Parquet files | 73 |
| CSV size | 5,668,612,855 bytes |
| Bronze folder size | 737,707,885 bytes (Parquet + Hadoop `.crc` + `_SUCCESS`) |
| Parquet payload only | 731,988,497 bytes |
| Compression ratio | 7.6841× (CSV bytes / bronze folder bytes) |
| Payload ratio | 7.7441× (CSV bytes / Parquet file bytes) |
| Folder as % of CSV | 13.01% |
| Elapsed | 22.0 seconds |
| First event instant | epoch 1,569,888,000, which is 2019-10-01 00:00:00 UTC |

Event mix. Percentages are the counts divided by 42,448,764, rounded to 2 decimal places.

| event_type | Rows | Share |
| --- | --- | --- |
| view | 40,779,399 | 96.07% |
| cart | 926,516 | 2.18% |
| purchase | 742,849 | 1.75% |

These three counts sum to 42,448,764. The quietest UTC day is 2019-10-03 (1,127,303 rows). The busiest is 2019-10-13 (1,639,071 rows).

Daily row counts, from the DuckDB group-by, which matches the Spark log:

| event_date | Rows | event_date | Rows |
| --- | --- | --- | --- |
| 2019-10-01 | 1,244,245 | 2019-10-17 | 1,407,007 |
| 2019-10-02 | 1,191,328 | 2019-10-18 | 1,481,799 |
| 2019-10-03 | 1,127,303 | 2019-10-19 | 1,449,293 |
| 2019-10-04 | 1,417,190 | 2019-10-20 | 1,509,992 |
| 2019-10-05 | 1,330,339 | 2019-10-21 | 1,405,416 |
| 2019-10-06 | 1,318,379 | 2019-10-22 | 1,416,872 |
| 2019-10-07 | 1,200,531 | 2019-10-23 | 1,381,126 |
| 2019-10-08 | 1,370,633 | 2019-10-24 | 1,316,501 |
| 2019-10-09 | 1,347,543 | 2019-10-25 | 1,435,328 |
| 2019-10-10 | 1,284,077 | 2019-10-26 | 1,347,268 |
| 2019-10-11 | 1,495,343 | 2019-10-27 | 1,388,553 |
| 2019-10-12 | 1,479,896 | 2019-10-28 | 1,260,880 |
| 2019-10-13 | 1,639,071 | 2019-10-29 | 1,227,910 |
| 2019-10-14 | 1,454,702 | 2019-10-30 | 1,210,145 |
| 2019-10-15 | 1,542,225 | 2019-10-31 | 1,245,479 |
| 2019-10-16 | 1,522,390 |  |  |

The daily counts sum to 42,448,764.

### Decisions

- Bronze does not clean. Null brand, null `category_code`, and any non-positive price stay as they arrived. A checked sample row (`user_session=72d76fde-8bb3-4e00-8c23-a032dfed738c`) still has `category_id=2103807459595387724` as a bigint, `brand=shiseido`, `price=35.79`, and a null `category_code`.
- `category_id` is a bigint. That sample value survived the round trip, so it was not downcast to double.
- Compression ratio for the README is **7.6841×**, using the bronze folder size. Checksum files are 5,719,388 bytes of that folder. Excluding them, the Parquet payload is 7.7441× smaller than the CSV.
- No shuffle before the write. Spark read the CSV as 43 splits. A time-ordered file crosses 30 midnights, and 43 + 30 = 73, which is the number of Parquet files written. One file per day was not worth a full-month shuffle on a 12 GB heap.

### Problems

1. **`src/01_ingest_bronze.py` was missing.** Written in this phase.
2. **The job took 22.0 seconds, not the expected 5–15 minutes.** Explicit schema, no shuffle, a 12g heap that actually stuck, and the laptop SSD. The row count still matches `wc -l`, so the short runtime is not a partial load.
3. **Spark still warns** that `MacBook-Pro.local` resolves to loopback and that the native Hadoop library is absent. Same local-mode warnings as Phase 1. The load succeeded.
4. **DuckDB prints the first timestamp as `2019-10-01 01:00:00+01`** when the session timezone is the machine zone. The epoch is 1,569,888,000, equal to 2019-10-01 00:00:00 UTC. The instant is UTC. Display it in UTC.

## Phase 3 — Data profiling (2026-10-09)

### What was done

- Wrote `src/02_profile.py`. The file was not in the tree. It reads `data/bronze/events` only and overwrites `docs/data_profile.md`.
- Profiled null rates, the price distribution, the event mix, exact duplicates, category-code depth, brand text, sessions per user, events per session, and sessions that contain a purchase.
- Cross-checked the headline counts in DuckDB, including the duplicate count. The two engines agree.

### Commands

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
source .venv/bin/activate
python src/02_profile.py
```

The run that wrote the committed profile used an 8g driver and `local[6]`. It finished in 44.5 seconds. JVM `maxMemory` was 8,589,934,592 bytes.

### Key numbers

Full tables and the one-line reading of each table are in `docs/data_profile.md`. The figures below are from that run.

| Check | Result |
| --- | --- |
| Events | 42,448,764 |
| Users | 3,022,290 |
| Sessions | 9,244,421 |
| Products | 166,794 |
| view / cart / purchase | 40,779,399 / 926,516 / 742,849 |
| Null category_code | 13,515,609 (31.84%) |
| Null brand | 6,113,008 (14.40%) |
| Null user_id | 0 |
| Null user_session | 2 |
| Price min / median / mean / p99 / max | 0.00 / 162.93 / 290.3237 / 1,741.34 / 2,574.07 |
| Price < 0 | 0 |
| Price = 0 | 68,673 (view 68,523, cart 150, purchase 0) |
| Exact-duplicate groups | 18,099 |
| Extra duplicate rows | 30,220 (0.07% of events) |
| Distinct source rows | 42,418,544 |
| Category depth | missing 13,515,609; depth 2: 17,356,435; depth 3: 11,540,478; depth 4: 36,242 |
| Brands needing lowercase+trim | 0 (3,445 distinct either way) |
| Median / max sessions per user | 2 / 7,400 |
| Users with exactly one session | 1,475,463 (48.82%) |
| Median / p99 / max events per session | 2 / 32 / 1,159 |
| Sessions with a purchase | 629,560 (6.81% of sessions) |
| Sessions tied to more than one user_id | 348 |

DuckDB on the same Parquet returned the same row count, null counts, zero-price split, median 162.93, p99 1,741.34, max 2,574.07, user count, session count, and purchase-session count. `count(*) - count(DISTINCT row)` was 30,220 extra rows, the same number Spark reported. DuckDB's `avg(price)` was 290.32366068504956 and Spark's was 290.32366068488096. Both round to 290.3237. The profile publishes the Spark value.

### Decisions

- Duplicates use `GROUP BY` on the nine source columns `HAVING COUNT(*) > 1`. Only the three scalar totals are collected. `event_date` is not part of the key.
- Median and p99 are Spark's exact `percentile`, which interpolates between adjacent values. On this file the median and p99 landed on observed cent prices, and DuckDB `quantile_cont` returned the same two numbers.
- A null `user_session` is not treated as a session. Two such rows are excluded from the session tables and counted beside them.
- Driver memory is 8g, under the 12g cap. The table is about 700 MB of Parquet.

### Problems

1. **`src/02_profile.py` was missing.** Written in this phase.
2. **Spark warned `RowBasedKeyValueBatch` could not spill** during the duplicate aggregation. The stage still finished. The DuckDB distinct count matches, so the warning did not drop rows. The query was not rewritten.
3. **The first draft of the report rounded a 2-row null rate to 0.0000%.** The formatter now keeps enough digits. The committed profile shows 0.00000471% for those 2 rows.

## Phase 4 — Silver cleaning (2026-10-09)

### What was done

- Wrote `src/03_clean_silver.py`. One function per cleaning rule. The script overwrites `data/silver/events`, partitioned by `event_date`, and writes `docs/cleaning_log.md` from the counts it just measured.
- Ran a tiny self-test of the split, the price flag, the brand standardisation, and the duplicate order, then ran the full month.
- DuckDB read the silver Parquet back and matched the Spark checks.

### Commands

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
source .venv/bin/activate
python src/03_clean_silver.py --self-test --driver-memory 1g --master local[1]
python src/03_clean_silver.py
```

The full run used a 12g driver and `local[6]`. `maxMemory` was 12,884,901,888 bytes. The process logged `SILVER_ELAPSED seconds=98.1`. The cleaning log stamps 97.7 seconds because it is written before the Spark session stops.

### Key numbers

| Check | Result |
| --- | --- |
| Bronze rows | 42,448,764 |
| Brand values changed by lower/trim | 0 |
| Exact duplicate rows removed | 30,220 |
| Rows after the duplicate drop | 42,418,544 |
| Null user_id or user_session rows removed | 2 |
| Silver rows | 42,418,542 |
| Zero prices flagged and kept | 68,670 (view 68,521, cart 149, purchase 0) |
| Negative prices | 0 |
| Brands filled with `unknown` | 6,112,126 |
| Category codes filled with `unknown` | 13,512,262 |
| Rows whose category was not already three levels | 30,884,455 |
| Depth-4 code `electronics.audio.music_tools.piano` | 36,224, all stored as electronics / audio / music_tools.piano |
| Date partitions | 31 (2019-10-01 through 2019-10-31) |
| Parquet files | 31 |
| Silver folder size | 1,478,063,662 bytes |
| Extra duplicate rows after the fills | 0 |

Fill and flag counts are lower than the Phase 3 null and zero-price totals because they are taken after the duplicate drop and the two-row key drop. DuckDB on `data/silver/events` returned 42,418,542 rows, 68,670 flagged prices, 36,224 correct piano splits, 13,512,262 fully unknown category rows, and no null brand, category, level, user, or session.

### Decisions

- Price `<= 0` is flagged, not dropped. After the row drops, the 68,670 zeros are 68,521 views and 149 carts. None are purchases and none are negative. Deleting them would shrink the funnel. `is_non_positive_price` lets revenue queries skip them. The stored price is still 0.
- Null brand and null `category_code` are filled with `unknown`. No row is dropped for a missing attribute. Neither column already contained the literal `unknown` (0 rows), so the fill does not collide with a real value.
- Category level 3 keeps the tail of a code with four or more pieces. The only such code is `electronics.audio.music_tools.piano`, and all 36,224 surviving rows landed on `music_tools.piano`.
- Brand lower/trim removed nothing on this file. It still runs first, so a later month with mixed case is deduplicated after the case fold.
- A null `user_session` is dropped, not filled. Two rows were removed. Filling them with `unknown` would invent a session.

### Problems

1. **`src/03_clean_silver.py` was missing.** Written in this phase.
2. **Spark again warned that `RowBasedKeyValueBatch` could not spill** during `dropDuplicates`. The stage finished. The post-write duplicate check found 0 extra rows.
3. **The silver folder is larger than bronze.** Silver is 1,478,063,662 bytes and the bronze folder was 737,707,885 bytes. The clean copy adds three category levels and a flag, and it repeats the literal `unknown` on the missing categories. The row count still went down.

## Phase 5 — Gold star schema (2026-10-09)

### What was done

- Wrote `src/04_build_gold.py`. It reads `data/silver/events`, overwrites `fact_events`, `sessions`, `dim_user`, `dim_product`, and `dim_date` under `data/gold`, and writes `docs/data_model.md` from the counts that run measured.
- Ran the self-test (modal brand, dominant session user, revenue, weekend flags, and the Monday week boundary), then the full month.
- Validation read the written Parquet back and checked grains, additive facts, and orphan keys.

### Commands

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
source .venv/bin/activate
python src/04_build_gold.py --self-test --driver-memory 1g --master local[1]
python src/04_build_gold.py
```

The full run used a 12g driver and `local[6]`. `maxMemory` was 12,884,901,888 bytes. The model doc stamps 73.0 seconds because it is written before the Spark session stops. The process logged `GOLD_ELAPSED seconds=73.1`.

### Key numbers

| Check | Result |
| --- | --- |
| fact_events | 42,418,542 |
| Views / carts / purchases | 40,777,328 / 898,441 / 742,773 |
| Revenue | 229,933,212.63 |
| sessions | 9,244,421 |
| dim_user | 3,022,290 |
| dim_product | 166,794 |
| dim_date | 31 (2019-10-01 through 2019-10-31, 8 weekend dates) |
| Products with more than one category tuple | 0 |
| Products with more than one brand | 7,701, covering 1,724,111 events |
| Those events that carry the modal brand | 1,612,796 |
| Purchases on a multi-brand product | 20,096, revenue 6,400,447.26 |
| Sessions with more than one user_id | 348 |
| User-session links | 9,244,770 (sum of `dim_user.n_sessions` and of `sessions.n_users`) |
| Sessions crossing UTC midnight | 29,606 |
| Sessions with duration 0 | 3,271,152 |
| Duration range | 0 to 2,640,529 seconds |
| Orphan keys | 0 on every check |
| Parquet files | fact 31 (one per `event_date`); sessions 8; dim_user 4; dim_product 1; dim_date 1 |
| Folder bytes | fact 1,353,311,820; sessions 349,310,794; dim_user 49,150,966; dim_product 944,677; dim_date 2,115; total 1,752,720,372 |

Views, carts, and purchases sum to 42,418,542. Fact revenue and the sum of `sessions.revenue` are both 229,933,212.63. The collected double was 229933212.63000426. The residual is under a tenth of a cent, inside the 0.01 reconciliation tolerance, and both sides formatted to the same cent.

### Decisions

- Category lives only on `dim_product`. The job measured 0 products with more than one category tuple. A non-zero count exits before any gold write.
- `fact_events.brand` is the brand on that event. `dim_product.brand` is the modal brand, and a tie goes to the smaller brand string. 1,612,796 of the 1,724,111 events on multi-brand products carry that modal brand. Later brand metrics use the fact column.
- `sessions.user_id` is the user with the most events in the session. A tie goes to the smaller `user_id`. Counts and revenue include the whole session. The fact keeps each event's own `user_id`. The 9,244,770 user-session links are 349 above the session count because 348 sessions contain more than one user.
- `sessions` is a session-grain fact and is not partitioned by date. 29,606 sessions start and end on different UTC dates.
- Revenue is purchase `price` where `is_non_positive_price` is false. Views and carts are excluded. `n_purchases` still counts every purchase event, including a flagged non-positive price. Phase 4 found no such purchase, so the purchase count and the revenue-eligible purchase count are the same 742,773 on this file.
- `dim_date` uses `event_date` so a SQL join does not need the reserved word `date`. `day_of_week` is Monday = 1 through Sunday = 7. `week` is Spark `weekofyear`. The spine is the 31 dates present in silver. 2019-10-01 is Tuesday, week 40. Week 41 starts on Monday 2019-10-07.

### Problems

1. **`src/04_build_gold.py` was missing.** Written in this phase.
2. **The bronze purchase count was 742,849.** Gold purchases are 742,773. By event type, the Phase 4 row drops are 76 purchases, 28,075 carts, and 2,071 views. Those three drops sum to 30,222, which is the 30,220 duplicate extras plus the 2 null-session rows. The purchase grain is the surviving silver purchase.
3. **3,271,152 sessions have duration 0, and the longest session lasts 2,640,529 seconds (30.56 days).** A single instant is duration 0. A session may span the month. Both were left as measured.
4. **No spill warning on this run.** The hostname and missing native-Hadoop messages are the same local-mode warnings as earlier phases. Validation counts matched, so they were left as-is.
