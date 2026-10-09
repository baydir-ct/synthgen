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


def _builder():
    spec = importlib.util.spec_from_file_location("build_notebook", ROOT / "scripts" / "build_notebook.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_notebook_is_current():
    assert NOTEBOOK.read_text(encoding="utf-8") == _builder().render(), "run: python scripts/build_notebook.py"


def test_notebook_structure():
    cells = json.loads(NOTEBOOK.read_text())["cells"]
    code = ["".join(c["source"]) for c in cells if c["cell_type"] == "code"]
    assert code[0].startswith("%pip install")
    assert "TABLES = [" in code[2] and "OUTPUT_FOLDER =" in code[2]
    assert all("display(" not in c and ".show(" not in c for c in code[2:])


def _without_timestamps(text: str) -> dict:
    data = json.loads(text)
    data.pop("generated_at")
    return data


def test_notebook_runs_self_contained_and_matches_package(pass1_run, tmp_path):
    out = tmp_path / "notebook_out"
    run = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "notebook_runner.py"), str(NOTEBOOK), str(out)],
        capture_output=True, text=True, timeout=900,
    )
    assert run.returncode == 0, run.stderr[-3000:]
    assert "synthgen engine loaded" in run.stdout and "links between tables found" in run.stdout

    package_files = sorted((pass1_run.output_root / "review").iterdir())
    notebook_files = sorted((out / "review").iterdir())
    assert [p.name for p in notebook_files] == [p.name for p in package_files]
    for nb, pkg in zip(notebook_files, package_files):
        assert _without_timestamps(nb.read_text()) == _without_timestamps(pkg.read_text()), nb.name

    with zipfile.ZipFile(out / "synthgen_profiles.zip") as archive:
        assert sorted(archive.namelist()) == [p.name for p in package_files]
