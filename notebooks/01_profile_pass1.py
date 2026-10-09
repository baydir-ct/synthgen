# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Pass 1 profiler (customer workspace)
# MAGIC
# MAGIC Reads the configured source tables **read-only** and writes one JSON profile per table plus
# MAGIC `relationships.json` to `<output.root>/review/`. Nothing leaves the workspace from here: the
# MAGIC privacy guard (notebook 02) and a human reviewer come next.
# MAGIC
# MAGIC This notebook deliberately never displays data. It prints only file paths and counts.

# COMMAND ----------

# MAGIC %pip install /Workspace/Shared/synthgen/synthgen-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.text("config_path", "/Workspace/Shared/synthgen/config/dataset.yaml", "Config YAML")  # noqa: F821

# COMMAND ----------

import logging

from synthgen.config import SynthgenConfig
from synthgen.profiler.pass1 import run_pass1

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
cfg = SynthgenConfig.load(dbutils.widgets.get("config_path"))  # noqa: F821
result = run_pass1(spark, cfg)  # noqa: F821

# COMMAND ----------

# Summary only: table names, row counts, how many columns were flagged sensitive, and file paths.
for profile in result.profiles:
    sensitive = sum(c.sensitive for c in profile.columns)
    print(f"{profile.source_table}: {profile.row_count} rows, {len(profile.columns)} columns, {sensitive} sensitive")
print(f"{len(result.relationships.foreign_keys)} foreign keys, {len(result.relationships.primary_keys)} primary keys")
for path in result.written:
    print(path)
