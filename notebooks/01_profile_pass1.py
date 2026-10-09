# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Profile tables (run in the customer workspace)
# MAGIC
# MAGIC Reads the tables you list **read-only** and writes one JSON summary per table, plus
# MAGIC `relationships.json`, into `<output_folder>/review/`. The summaries hold statistics only:
# MAGIC no rows, no names, emails or other personal values.
# MAGIC
# MAGIC **How to use:** fill in the boxes at the top of the notebook, then *Run all*.
# MAGIC This notebook never displays table data; it prints only file names and counts.

# COMMAND ----------

# MAGIC %pip install /Workspace/Shared/synthgen/synthgen-0.1.0-py3-none-any.whl

# COMMAND ----------

dbutils.widgets.text("tables", "", "1. Tables (catalog.schema.table or catalog.schema.*)")  # noqa: F821
dbutils.widgets.text("output_folder", "", "2. Output folder (e.g. /Volumes/...)")  # noqa: F821
dbutils.widgets.text("sensitive_columns", "", "3. Extra sensitive columns (optional)")  # noqa: F821
dbutils.widgets.text("config_path", "", "4. Config YAML (optional, overrides 1-3)")  # noqa: F821

# COMMAND ----------

import logging

from synthgen.notebook_helpers import config_from_widgets
from synthgen.profiler.pass1 import run_pass1

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
cfg = config_from_widgets(
    dbutils.widgets.get("tables"),  # noqa: F821
    dbutils.widgets.get("output_folder"),  # noqa: F821
    dbutils.widgets.get("sensitive_columns"),  # noqa: F821
    dbutils.widgets.get("config_path"),  # noqa: F821
)
result = run_pass1(spark, cfg)  # noqa: F821

# COMMAND ----------

# Summary only: table names, row counts, how many columns were treated as sensitive, and file paths.
for profile in result.profiles:
    sensitive = [c.name for c in profile.columns if c.sensitive]
    print(f"{profile.source_table}: {profile.row_count} rows, {len(profile.columns)} columns, "
          f"sensitive: {', '.join(sensitive) or 'none'}")
print(f"\n{len(result.relationships.foreign_keys)} table links found")
print("\nFiles to review:")
for path in result.written:
    print(" ", path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Package for sending (after review)
# MAGIC Read the JSON files in `review/` first. If you edit or delete any of them, run this cell again.
# MAGIC It creates `synthgen_profiles.zip` next to the `review/` folder: that zip is the only thing to send.

# COMMAND ----------

import shutil
from pathlib import Path

review = Path(cfg.output.root) / "review"
archive = shutil.make_archive(str(Path(cfg.output.root) / "synthgen_profiles"), "zip", review)
print(archive)
