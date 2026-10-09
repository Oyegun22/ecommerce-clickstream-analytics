# October 2019 clickstream pipeline.
# `clean` writes data/silver/events from bronze. It is the cleaning step.
# Paths are project-relative. Spark jobs refuse a driver heap above 12g.

JAVA_HOME := $(shell /usr/libexec/java_home -v 17)
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

PY := .venv/bin/python

.PHONY: setup ingest profile clean gold analyze dashboard all

setup:
	$(PY) -m pip install -r requirements.txt
	$(PY) src/00_check_env.py

ingest:
	$(PY) src/01_ingest_bronze.py

profile:
	$(PY) src/02_profile.py

clean:
	$(PY) src/03_clean_silver.py

gold:
	$(PY) src/04_build_gold.py

analyze:
	$(PY) src/05_run_analysis.py

dashboard:
	$(PY) src/06_export_dashboard_data.py

all: setup ingest profile clean gold analyze dashboard
