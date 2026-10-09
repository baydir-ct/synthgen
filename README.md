# synthgen

Profiles Unity Catalog tables in a customer workspace into privacy-filtered statistics, and (later
milestones) regenerates statistically similar synthetic tables in another workspace for a Genie demo.
The full spec is [PLAN.md](PLAN.md).

**Status:** milestone 1 (profiler) and a first generator (milestone 3, simple version) are done.
Milestones 2 (automatic privacy guard), 4 (validator) and 5 (question-driven joint statistics) are
skipped for now: a person reviews the JSON by eye, and the fake tables are checked by eye and in Genie.

## The two notebooks

| Notebook | Where it runs | What it does |
| --- | --- | --- |
| [`01_profile_tables.ipynb`](notebooks/01_profile_tables.ipynb) | customer workspace | Reads the listed tables, writes statistics-only JSON, zips them after review |
| [`02_generate_tables.ipynb`](notebooks/02_generate_tables.ipynb) | our workspace | Reads the zip, writes fake tables with the same names, columns, comments and keys |

Both are self-contained (the package source is embedded) and are configured in one Python cell.
Their only install is `pydantic`, from their first cell.

**Known limits of the simple generator:** columns are generated independently within a table (e.g.
a cancelled order can still have a ship date; premium customers do not spend more), except for the
detected rules (date ordering, `a = b * c`, `a = b + c`), links between tables, and emails that match
the generated names. Category labels below k are replaced by invented ones ("Brand A", "Brand B"...).
Rows are generated on the driver, capped by `MAX_ROWS` per table (default 1M).

## Layout

```
synthgen/
  config.py               # every threshold, with defaults; loaded from YAML
  profile_schema.py       # Pydantic models for profile files, schema_version 1.0
  io.py                   # ProfileStore: writes only inside <root>/review, validates on write and read
  profiler/
    pass1.py              # orchestration and per-column statistics
    detect.py             # pure heuristics: PII names, identifiers, roles, semantic types, FK name scores
    relationships.py      # PK candidates, FK inference, containment and children-per-parent stats
    rules.py              # date ordering and numeric identities
    metadata.py           # Spark catalog / Unity Catalog information_schema metadata
    stats.py              # k-suppression and conversion helpers
    _safe.py              # the only place Spark results are collected
  generator/
    plan.py               # table order, sizes, which FK drives each child table's row count
    columns.py            # samplers per column type (inverse CDF, patterns, word lists)
    generate.py           # builds all tables in memory, parents first
    rules.py              # date ordering, numeric identities, emails matching names
    write.py              # CREATE TABLE with comments, insert, PK/FK constraints
    check.py              # row counts and join rates after writing
    vocab.py              # built-in names, streets, words (no Faker)
config/example_dataset.yaml  # every setting spelled out with its default
schemas/                  # JSON Schema of the profile files, for reviewers
notebooks/                   # the two self-contained notebooks (generated, do not edit by hand)
docs/customer_runbook.md     # instructions for the person running it
scripts/build_notebook.py    # rebuilds both notebooks from the package source
scripts/profile_fixture.py   # run pass 1 on the test fixture locally
examples/fixture_profiles/   # sample output from the fixture
tests/
```

## Local setup

Needs Python 3.10+ and Java 17 or 21 (Spark does not support newer JDKs). On macOS the tests pick a
supported JDK automatically when `JAVA_HOME` is unset.

```bash
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e ".[dev]"
```

```bash
.venv/bin/python -m pytest -q
```

```bash
.venv/bin/python scripts/profile_fixture.py
```

## On Databricks

Send [`notebooks/01_profile_tables.ipynb`](notebooks/01_profile_tables.ipynb) and the
[runbook](docs/customer_runbook.md). The notebook embeds the package source, so nothing else is
uploaded; its first cell installs `pydantic` from PyPI. The person edits one configuration cell and
clicks Run all. After changing code under `synthgen/`, rebuild it with
`python scripts/build_notebook.py` (a test fails if it is out of date).

The code uses only the DataFrame API (no RDDs, no `sparkContext`), so it runs on Spark Connect
(serverless and shared access mode) as well as classic clusters, on Spark 3.5+ (DBR 15.4+).

## How the hard constraints are enforced in pass 1

| Constraint | Enforcement |
| --- | --- |
| No row-level output | All results reach the driver through `profiler/_safe.py`, which caps the row count. A test parses the profiler source and fails on `collect`, `show`, `display`, `print`, `toPandas`, writes, temp views or DDL anywhere else. Rare labels are filtered inside Spark, so they never reach the driver. |
| k-suppression | Labels, patterns and time buckets below k are dropped in Spark. Any other count between 1 and k-1 (nulls, zeros, booleans, orphans, rule violations) becomes `null` and is listed in `suppressed`. k below 5 is rejected by the config. |
| Sensitive columns | Flagged by UC tag, name heuristic, config, or value patterns (email/phone/IP/IBAN). They export only type, null rate, length, format patterns and semantic type. |
| Free text and identifiers | Strings above the cardinality thresholds, identifier-named columns and key/FK columns export no values. |
| Clipped extremes | Exported min/max are the 1st/99th percentiles; the config cannot loosen this. Distributions need at least `min_count_for_distribution` non-null rows, checked inside Spark. |
| Review before export | Profiles are written only to `<output.root>/review/`. |
| Read-only source | The profiler only issues `SELECT`-type DataFrame operations; tests snapshot the warehouse and catalog before and after a run. |
| Last check | `enforce_export_policy` refuses to write a profile where a no-values column carries value statistics. |
