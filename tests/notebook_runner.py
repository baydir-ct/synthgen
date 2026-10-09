"""Execute one of the generated notebooks outside Databricks, on a local Spark session.

Run in a fresh interpreter (the notebook registers its own `synthgen` modules):
    python tests/notebook_runner.py <notebook.ipynb> '<json overrides for the config cell>' [--retail]
--retail builds the retail fixture (and fake information_schema) first.
The installed synthgen package is blocked, so this proves the notebook is self-contained.
"""

from __future__ import annotations

import importlib.abc
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class _BlockInstalledSynthgen(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "synthgen" or name.startswith("synthgen."):
            raise ImportError(f"{name} must come from the notebook, not the installed package")
        return None


def main(notebook: Path, overrides: dict, retail: bool) -> None:
    sys.meta_path.insert(0, _BlockInstalledSynthgen())
    from tests.conftest_spark import local_spark  # no synthgen imports
    from tests.fixtures.retail import build_fake_information_schema, build_retail

    cells = [c for c in json.loads(notebook.read_text())["cells"] if c["cell_type"] == "code"]
    with tempfile.TemporaryDirectory() as warehouse:
        spark = local_spark(warehouse)
        if retail:
            build_retail(spark)
            build_fake_information_schema(spark)
        ns: dict = {"spark": spark, "__name__": "__main__", "display": lambda df: None}
        for cell in cells:
            source = "".join(cell["source"])
            if source.lstrip().startswith("%pip") or "dbutils.library" in source:
                continue  # Databricks-only cells
            exec(compile(source, "<notebook cell>", "exec"), ns)
            if "CONFIGURATION: edit" in source:
                ns.update(overrides)
        assert sys.modules["synthgen"].__file__.startswith("<synthgen notebook")
        spark.stop()


if __name__ == "__main__":
    main(Path(sys.argv[1]), json.loads(sys.argv[2]), "--retail" in sys.argv[3:])
