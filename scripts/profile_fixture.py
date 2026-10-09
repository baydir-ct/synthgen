"""Build the retail test fixture in a throwaway local Spark warehouse and run pass 1 on it.

Usage: python scripts/profile_fixture.py [output_root]   (default: examples/fixture_profiles)
Writes <output_root>/review/*.profile.json and relationships.json so the format can be reviewed.
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests.conftest import _ensure_java, load_config  # noqa: E402
from tests.fixtures.retail import build_fake_information_schema, build_retail  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    _ensure_java()
    from pyspark.sql import SparkSession

    from synthgen.profiler.pass1 import run_pass1

    output_root = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "examples" / "fixture_profiles"
    with tempfile.TemporaryDirectory() as warehouse:
        spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        build_retail(spark)
        build_fake_information_schema(spark)
        result = run_pass1(spark, load_config(output_root))
        spark.stop()
    for path in result.written:
        print(path.relative_to(ROOT) if path.is_relative_to(ROOT) else path)


if __name__ == "__main__":
    main()
