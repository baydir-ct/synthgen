from __future__ import annotations

import pytest
from pydantic import ValidationError

from synthgen.notebook_helpers import config_from_notebook


def test_notebook_values_build_a_config():
    cfg = config_from_notebook([" main.sales.*", "main.crm.customers", ""], "/Volumes/main/x/files/out",
                               ["main.crm.customers.notes"])
    assert cfg.source.table_patterns == ["main.sales.*", "main.crm.customers"]
    assert cfg.output.root == "/Volumes/main/x/files/out"
    assert cfg.privacy.sensitive_columns == ["main.crm.customers.notes"]
    assert cfg.privacy.k == 10


def test_advanced_settings_override_defaults():
    cfg = config_from_notebook("main.sales.*", "/out", advanced_settings={
        "privacy": {"k": 20}, "profiling": {"sample_fraction": 0.1}})
    assert cfg.privacy.k == 20 and cfg.profiling.sample_fraction == 0.1
    assert cfg.privacy.top_k_values == 50  # untouched defaults survive the merge
    with pytest.raises(ValidationError):
        config_from_notebook("main.sales.*", "/out", advanced_settings={"privacy": {"k": 3}})


@pytest.mark.parametrize("tables, folder, message", [
    ([], "/out", "TABLES is empty"),
    (["sales.orders"], "/out", "must be catalog.schema.table"),
    (["main.sales.*"], " ", "OUTPUT_FOLDER is empty"),
])
def test_mistakes_are_explained(tables, folder, message):
    with pytest.raises(ValueError, match=message):
        config_from_notebook(tables, folder)
