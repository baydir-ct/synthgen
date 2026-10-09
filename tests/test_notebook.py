"""The customer notebook: up to date with the package, self-contained, same output as the package."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "01_profile_tables.ipynb"
GENERATE_NOTEBOOK = ROOT / "notebooks" / "02_generate_tables.ipynb"


def _builder():
    spec = importlib.util.spec_from_file_location("build_notebook", ROOT / "scripts" / "build_notebook.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_notebooks_are_current():
    builder = _builder()
    for name in builder.NOTEBOOKS:
        committed = (ROOT / "notebooks" / name).read_text(encoding="utf-8")
        assert committed == builder.render(name), f"{name} is stale: run python scripts/build_notebook.py"


def test_notebook_structure():
    cells = json.loads(NOTEBOOK.read_text())["cells"]
    code = ["".join(c["source"]) for c in cells if c["cell_type"] == "code"]
    assert code[0].startswith("%pip install")
    assert "TABLES = [" in code[2] and "OUTPUT_FOLDER =" in code[2]
    assert all("display(" not in c and ".show(" not in c for c in code[2:])


def _run_notebook(notebook: Path, overrides: dict, *flags: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "tests" / "notebook_runner.py"), str(notebook), json.dumps(overrides), *flags],
        capture_output=True, text=True, timeout=900,
    )


def _without_timestamps(text: str) -> dict:
    data = json.loads(text)
    data.pop("generated_at")
    return data


def test_notebook_runs_self_contained_and_matches_package(pass1_run, tmp_path):
    out = tmp_path / "notebook_out"
    overrides = {"TABLES": ["spark_catalog.retail.*"], "OUTPUT_FOLDER": str(out),
                 "EXTRA_SENSITIVE_COLUMNS": ["*.customers.contact_handle"]}
    run = _run_notebook(NOTEBOOK, overrides, "--retail")
    assert run.returncode == 0, run.stderr[-3000:]
    assert "synthgen engine loaded" in run.stdout and "links between tables found" in run.stdout

    package_files = sorted((pass1_run.output_root / "review").iterdir())
    notebook_files = sorted((out / "review").iterdir())
    assert [p.name for p in notebook_files] == [p.name for p in package_files]
    for nb, pkg in zip(notebook_files, package_files):
        assert _without_timestamps(nb.read_text()) == _without_timestamps(pkg.read_text()), nb.name

    with zipfile.ZipFile(out / "synthgen_profiles.zip") as archive:
        assert sorted(archive.namelist()) == [p.name for p in package_files]


def test_generate_notebook_runs_self_contained(pass1_run, tmp_path):
    import shutil

    archive = shutil.make_archive(str(tmp_path / "synthgen_profiles"), "zip", pass1_run.output_root / "review")
    overrides = {"PROFILES": archive, "TARGET_CATALOG": "spark_catalog", "TARGET_SCHEMA": "genie_demo",
                 "SCALE": 0.2, "MIN_ROWS": 50, "DECLARE_KEYS": False, "TABLE_FORMAT": "parquet"}
    run = _run_notebook(GENERATE_NOTEBOOK, overrides)
    assert run.returncode == 0, run.stderr[-3000:]
    out = run.stdout
    for table in ("customers", "products", "orders", "order_lines"):
        assert f"spark_catalog.genie_demo.{table}:" in out
    assert "order_lines.order_id -> orders.order_id: 100.0% of rows join" in out
    assert "orders.customer_id -> customers.customer_id: 100.0% of rows join" in out
