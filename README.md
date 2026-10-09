# synthgen

Profiles Unity Catalog tables in a customer workspace into privacy-filtered statistics, and (later
milestones) regenerates statistically similar synthetic tables in another workspace for a Genie demo.
The full spec is [PLAN.md](PLAN.md).

**Status: milestone 1 (profile schema and pass-1 profiler).**

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
config/example_dataset.yaml  # every setting spelled out with its default
schemas/                  # JSON Schema of the profile files, for reviewers
notebooks/01_profile_tables.ipynb  # self-contained customer notebook (generated, do not edit by hand)
docs/customer_runbook.md     # instructions for the person running it
scripts/build_notebook.py    # rebuilds the notebook from the package source
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
