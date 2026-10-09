"""Build the two self-contained notebooks:
- notebooks/01_profile_tables.ipynb   (customer workspace: profile tables, export statistics)
- notebooks/02_generate_tables.ipynb  (our workspace: generate synthetic tables from the statistics)

Each notebook embeds the synthgen source it needs (the "engine" cell), so nothing has to be uploaded
or installed besides pydantic from PyPI. Run this script after changing any code under synthgen/;
a test fails if a committed notebook is out of date.

Usage: python scripts/build_notebook.py
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "notebooks"

# Dependency order: each module only imports modules listed before it.
PROFILE_MODULES = [
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

GENERATE_MODULES = [
    ("synthgen", "synthgen/__init__.py", True),
    ("synthgen.config", "synthgen/config.py", False),
    ("synthgen.profile_schema", "synthgen/profile_schema.py", False),
    ("synthgen.io", "synthgen/io.py", False),
    ("synthgen.profiler", "synthgen/profiler/__init__.py", True),
    ("synthgen.profiler.detect", "synthgen/profiler/detect.py", False),
    ("synthgen.generator", "synthgen/generator/__init__.py", True),
    ("synthgen.generator.vocab", "synthgen/generator/vocab.py", False),
    ("synthgen.generator.plan", "synthgen/generator/plan.py", False),
    ("synthgen.generator.columns", "synthgen/generator/columns.py", False),
    ("synthgen.generator.rules", "synthgen/generator/rules.py", False),
    ("synthgen.generator.generate", "synthgen/generator/generate.py", False),
    ("synthgen.generator.write", "synthgen/generator/write.py", False),
    ("synthgen.generator.check", "synthgen/generator/check.py", False),
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
# The synthgen code, embedded so nothing has to be uploaded. It is plain Python: feel free to read it.
# It only defines functions; nothing is read or written in this cell.

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


GEN_INTRO = """\
# Generate synthetic tables for a Genie demo

Reads the statistics zip produced by `01_profile_tables` in the customer workspace and writes fake tables
with the same names, columns, types and comments into a catalog and schema of your choice. The tables
join like the originals (declared primary and foreign keys), and columns follow the original
distributions. Nothing here touches customer data: the zip contains statistics only.

**How to use it**
1. Upload `synthgen_profiles.zip` to a volume or workspace folder.
2. Edit **cell 2 (Configuration)**: where the zip is, and the target catalog and schema.
3. Click **Run all**. Cell 4 prints the plan (tables, sizes, links) before anything is written.
4. Check the summary and sample rows at the end, then point a Genie space at the new schema."""

GEN_CONFIG = '''\
# ============================== 2. CONFIGURATION: edit, then run =================================

# The zip received from the customer (or the folder with the unzipped .json files).
PROFILES = "/Volumes/main/genie_demo/files/synthgen_profiles.zip"

# Where to create the fake tables. The schema is created if it does not exist.
TARGET_CATALOG = "main"
TARGET_SCHEMA = "genie_demo"

# Size: each table gets SCALE x its real row count, at least MIN_ROWS (or its real size if smaller).
# Child tables (orders, order lines...) follow from their parents, keeping the real proportions.
SCALE = 0.01
MIN_ROWS = 100
MAX_ROWS = 1_000_000  # hard cap per table (rows are generated on the driver)

SEED = 42  # same seed + same zip = same data

# Declare primary and foreign keys on the new tables so Genie knows how they join (Unity Catalog).
DECLARE_KEYS = True

# Safety: tables this notebook did not create are never replaced unless you set this to True.
REPLACE_EXISTING = False

TABLE_FORMAT = "delta"'''

GEN_PLAN = '''\
# ============================== 4. PLAN: what will be generated (nothing written yet) ============
import logging

from synthgen.generator.plan import GeneratorSettings, build_plan, describe_plan, load_profiles

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", force=True)
for noisy in ("py4j", "pyspark"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

settings = GeneratorSettings(scale=SCALE, min_rows=MIN_ROWS, max_rows=MAX_ROWS, seed=SEED)
profiles, relationships = load_profiles(PROFILES)
plans = build_plan(profiles, relationships, settings)
print(describe_plan(plans, settings))'''

GEN_RUN = '''\
# ============================== 5. GENERATE and write the tables =================================
from synthgen.generator.generate import generate
from synthgen.generator.write import write_tables

data = generate(plans, settings)
report = write_tables(spark, plans, data, TARGET_CATALOG, TARGET_SCHEMA, settings, table_format=TABLE_FORMAT,
                      declare_keys=DECLARE_KEYS, replace_existing=REPLACE_EXISTING)
del data
for table, rows in report.tables.items():
    print(f"{table}: {rows} rows")
for warning in report.warnings:
    print("warning:", warning)'''

GEN_CHECK = '''\
# ============================== 6. CHECK: row counts and links between tables ====================
from synthgen.generator.check import check_tables

for line in check_tables(spark, plans, TARGET_CATALOG, TARGET_SCHEMA):
    print(line)'''

GEN_LOOK = '''\
# ============================== 7. LOOK at the data (fake rows, safe to display) =================
for table in report.tables:
    print(table)
    display(spark.table(table).limit(20))'''


def _engine(modules: list[tuple[str, str, bool]]) -> str:
    parts = [ENGINE_HEADER]
    for name, rel, package in modules:
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


def _notebook(cells: list[dict], prefix: str) -> dict:
    for i, cell in enumerate(cells):
        cell["id"] = f"{prefix}-{i:02d}"
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _install_cells() -> list[dict]:
    return [
        _cell("markdown", "## 1. Install the one dependency (pydantic)"),
        _cell("code", INSTALL),
        _cell("code", RESTART),
    ]


def build_profile() -> dict:
    return _notebook([
        _cell("markdown", INTRO),
        *_install_cells(),
        _cell("code", CONFIG),
        _cell("code", _engine(PROFILE_MODULES)),
        _cell("code", RUN),
        _cell("markdown", REVIEW),
        _cell("code", PACKAGE),
    ], "synthgen")


def build_generate() -> dict:
    return _notebook([
        _cell("markdown", GEN_INTRO),
        *_install_cells(),
        _cell("code", GEN_CONFIG),
        _cell("code", _engine(GENERATE_MODULES)),
        _cell("code", GEN_PLAN),
        _cell("code", GEN_RUN),
        _cell("code", GEN_CHECK),
        _cell("code", GEN_LOOK),
    ], "synthgen-gen")


NOTEBOOKS = {
    "01_profile_tables.ipynb": build_profile,
    "02_generate_tables.ipynb": build_generate,
}


def render(name: str) -> str:
    return json.dumps(NOTEBOOKS[name](), indent=1, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    NOTEBOOK_DIR.mkdir(parents=True, exist_ok=True)
    for name in NOTEBOOKS:
        (NOTEBOOK_DIR / name).write_text(render(name), encoding="utf-8")
        print(f"notebooks/{name}")
