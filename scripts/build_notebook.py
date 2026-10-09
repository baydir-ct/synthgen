"""Build notebooks/01_profile_tables.ipynb: one self-contained notebook for the customer workspace.

The notebook embeds the synthgen package source (the "engine" cell), so nothing has to be uploaded
or installed besides pydantic from PyPI. Run this script after changing any code under synthgen/;
a test fails if the committed notebook is out of date.

Usage: python scripts/build_notebook.py
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "notebooks" / "01_profile_tables.ipynb"

# Dependency order: each module only imports modules listed before it.
MODULES = [
    ("synthgen", "synthgen/__init__.py", True),
    ("synthgen.config", "synthgen/config.py", False),
    ("synthgen.profile_schema", "synthgen/profile_schema.py", False),
    ("synthgen.io", "synthgen/io.py", False),
    ("synthgen.notebook_helpers", "synthgen/notebook_helpers.py", False),
    ("synthgen.profiler", "synthgen/profiler/__init__.py", True),
    ("synthgen.profiler._safe", "synthgen/profiler/_safe.py", False),
    ("synthgen.profiler.detect", "synthgen/profiler/detect.py", False),
    ("synthgen.profiler.stats", "synthgen/profiler/stats.py", False),
    ("synthgen.profiler.metadata", "synthgen/profiler/metadata.py", False),
    ("synthgen.profiler.rules", "synthgen/profiler/rules.py", False),
    ("synthgen.profiler.relationships", "synthgen/profiler/relationships.py", False),
    ("synthgen.profiler.pass1", "synthgen/profiler/pass1.py", False),
]

INTRO = """\
# Profile tables for a synthetic Genie demo

This notebook reads the tables you list and writes **statistics only**: column types, share of empty values,
typical ranges, most common categories, and how tables link to each other. It **never exports rows** or
personal values (names, emails, phones, addresses, IDs...). It only reads your tables: it creates, changes
or deletes nothing outside the output folder you choose.

**How to use it**
1. Attach the notebook to a cluster (Databricks Runtime 15.4 LTS or newer, or serverless).
2. Edit **cell 2 (Configuration)**: the tables and an output folder you can write to.
3. Click **Run all**. It takes a few minutes per table.
4. Read the files in `<output folder>/review/` (see the review notes further down).
5. Run the **last cell** to zip the reviewed files, download the zip and send only that file.

You need read access to the tables, write access to the output folder, and the cluster must be able to
install `pydantic` from PyPI (cell 1)."""

INSTALL = '%pip install -q "pydantic>=2.7"'
RESTART = "dbutils.library.restartPython()  # loads the freshly installed package"

CONFIG = '''\
# ============================== 2. CONFIGURATION: edit, then run =================================

# Tables to profile: full names, or catalog.schema.* for every table of a schema.
TABLES = [
    "main.sales.*",
]

# Folder where the results are written. A Unity Catalog volume you can write to, e.g.
#   "/Volumes/<catalog>/<schema>/<volume>/synthgen"
# or your own workspace folder, e.g. "/Workspace/Users/<your.email>/synthgen_output"
OUTPUT_FOLDER = "/Volumes/main/sandbox/files/synthgen"

# Optional: columns whose values must never leave, as catalog.schema.table.column (* allowed).
# Personal data (names, emails, phones, addresses, IDs, IBANs, birth dates, IPs...) and columns with
# a Unity Catalog PII tag are already detected automatically. Add anything else confidential here.
EXTRA_SENSITIVE_COLUMNS = [
    # "main.sales.customers.internal_notes",
]

# Optional: advanced settings. Leave empty for the defaults. Examples:
#   {"privacy": {"k": 20}}                          hide any value shared by fewer than 20 rows (default 10)
#   {"profiling": {"sample_fraction": 0.1}}         compute statistics on a 10% sample (very large tables)
ADVANCED_SETTINGS = {}'''

ENGINE_HEADER = '''\
# ============================== 3. ENGINE: run, do not edit ======================================
# The profiling code (synthgen package, embedded so nothing has to be uploaded). It is plain Python:
# feel free to read it. It only defines functions; nothing is read or written until cell 4.

import sys
import types


def _load(name, source, package=False):
    module = types.ModuleType(name)
    module.__file__ = "<synthgen notebook: " + name + ">"
    if package:
        module.__path__ = []
    sys.modules[name] = module
    exec(compile(source, module.__file__, "exec"), module.__dict__)

'''

RUN = '''\
# ============================== 4. RUN: profile the tables =======================================
import logging

from synthgen.notebook_helpers import config_from_notebook
from synthgen.profiler.pass1 import run_pass1

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", force=True)
for noisy in ("py4j", "pyspark"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

cfg = config_from_notebook(TABLES, OUTPUT_FOLDER, EXTRA_SENSITIVE_COLUMNS, ADVANCED_SETTINGS)
result = run_pass1(spark, cfg)

# Summary only: names, counts and file paths. This notebook never displays table data.
for profile in result.profiles:
    sensitive = [c.name for c in profile.columns if c.sensitive]
    print(f"{profile.source_table}: {profile.row_count} rows, {len(profile.columns)} columns, "
          f"values hidden for: {', '.join(sensitive) or 'none'}")
print(f"\\n{len(result.relationships.foreign_keys)} links between tables found")
print("\\nFiles to review:")
for path in result.written:
    print("  ", path)'''

REVIEW = """\
## 5. Review the files before sending

Open the `review/` folder inside your output folder: one `.json` file per table plus `relationships.json`.

- **Sensitive columns** (`"sensitive": true`) show only length and a format such as `"AAAA.AAAA@AAAA.AAA"`
  (letters become `A`, digits `9`). No values.
- **Categorical columns** list their most common values with shares, e.g. `"value": "France", "share": 0.74`.
  Values used by fewer than 10 rows are never listed. **Main thing to check:** are you comfortable sending
  these category names (countries, product categories, statuses, channels...)?
- **Comments**: table and column descriptions from Unity Catalog are copied as they are. Check they contain
  no example personal data or confidential text.
- **Numbers and dates** show percentiles and shares by month, weekday and hour. Extreme values are cut off.

If something must not leave, **do not edit the JSON**: add the column to `EXTRA_SENSITIVE_COLUMNS`
(or remove the table from `TABLES`) in cell 2 and run the notebook again."""

PACKAGE = '''\
# ============================== 6. PACKAGE: run after your review ================================
# Zips exactly the files in review/ into synthgen_profiles.zip next to it. Send only that zip.
# Download it from Catalog Explorer (volume) or by right-clicking the file (workspace folder).
import shutil
from pathlib import Path

archive = shutil.make_archive(str(Path(cfg.output.root) / "synthgen_profiles"), "zip", Path(cfg.output.root) / "review")
print(archive)'''


def _engine() -> str:
    parts = [ENGINE_HEADER]
    for name, rel, package in MODULES:
        source = (ROOT / rel).read_text(encoding="utf-8")
        if "'''" in source or source.rstrip().endswith("\\"):
            raise ValueError(f"{rel} cannot be embedded in a raw triple-quoted string")
        flag = ", package=True" if package else ""
        parts.append(f"_load(\"{name}\", r'''\n{source}'''{flag})\n")
    parts.append('print("synthgen engine loaded:", sys.modules["synthgen"].__version__)')
    return "\n".join(parts)


def _cell(kind: str, text: str) -> dict:
    lines = text.splitlines(keepends=True)
    cell = {"cell_type": kind, "metadata": {}, "source": lines}
    if kind == "code":
        cell.update(execution_count=None, outputs=[])
    return cell


def build() -> dict:
    notebook = {
        "cells": [
            _cell("markdown", INTRO),
            _cell("markdown", "## 1. Install the one dependency (pydantic)"),
            _cell("code", INSTALL),
            _cell("code", RESTART),
            _cell("code", CONFIG),
            _cell("code", _engine()),
            _cell("code", RUN),
            _cell("markdown", REVIEW),
            _cell("code", PACKAGE),
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    for i, cell in enumerate(notebook["cells"]):
        cell["id"] = f"synthgen-{i:02d}"
    return notebook


def render() -> str:
    return json.dumps(build(), indent=1, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render(), encoding="utf-8")
    print(OUT.relative_to(ROOT))
