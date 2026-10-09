from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from synthgen.config import SynthgenConfig
from synthgen.profiler.pass1 import Pass1Result, run_pass1
from tests.fixtures.retail import RetailFixture, build_fake_information_schema, build_retail

FIXTURES = Path(__file__).parent / "fixtures"


def _ensure_java() -> None:
    """Local Spark setup: workers use this interpreter; Spark 3.5/4.0 need Java 17 or 21.
    On macOS, pick a supported JDK automatically if JAVA_HOME is unset."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if os.environ.get("JAVA_HOME") or not shutil.which("/usr/libexec/java_home"):
        return
    for version in ("21", "17"):
        found = subprocess.run(["/usr/libexec/java_home", "-v", version], capture_output=True, text=True)
        if found.returncode == 0:
            os.environ["JAVA_HOME"] = found.stdout.strip()
            return


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    _ensure_java()
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("synthgen-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def warehouse_dir(spark) -> Path:
    return Path(spark.conf.get("spark.sql.warehouse.dir").removeprefix("file:"))


@pytest.fixture(scope="session")
def retail(spark) -> RetailFixture:
    fx = build_retail(spark)
    build_fake_information_schema(spark)
    return fx


def load_config(output_root: Path, **overrides) -> SynthgenConfig:
    raw = SynthgenConfig.load(FIXTURES / "retail_config.yaml").model_dump()
    raw["output"]["root"] = str(output_root)
    for key, value in overrides.items():
        raw[key] = value
    return SynthgenConfig.model_validate(raw)


def snapshot(folder: Path) -> dict[str, tuple[int, int]]:
    return {
        str(p.relative_to(folder)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in sorted(folder.rglob("*"))
        if p.is_file()
    }


@dataclass
class Pass1Run:
    config: SynthgenConfig
    result: Pass1Result
    output_root: Path
    warehouse_before: dict
    warehouse_after: dict
    catalog_before: list[str]
    catalog_after: list[str]


def _catalog_listing(spark) -> list[str]:
    out = []
    for db in spark.catalog.listDatabases():
        out += [f"{db.name}.{t.name}:{t.tableType}" for t in spark.catalog.listTables(db.name)]
    return sorted(out)


@pytest.fixture(scope="session")
def pass1_run(spark, retail, warehouse_dir, tmp_path_factory) -> Pass1Run:
    """Pass 1 on the retail fixture with the (fake) Unity Catalog metadata provider, run once."""
    output_root = tmp_path_factory.mktemp("synthgen_out")
    cfg = load_config(output_root)
    wh_before, cat_before = snapshot(warehouse_dir), _catalog_listing(spark)
    result = run_pass1(spark, cfg)
    return Pass1Run(cfg, result, output_root, wh_before, snapshot(warehouse_dir), cat_before, _catalog_listing(spark))


def profile_of(run: Pass1Run, table: str):
    return next(p for p in run.result.profiles if p.source_table.endswith("." + table))


def column_of(run: Pass1Run, table: str, column: str):
    return next(c for c in profile_of(run, table).columns if c.name == column)
