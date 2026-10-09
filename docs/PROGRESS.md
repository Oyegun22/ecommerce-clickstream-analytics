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
