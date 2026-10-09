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
