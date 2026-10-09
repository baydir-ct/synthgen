from __future__ import annotations

import pytest

from synthgen.notebook_helpers import config_from_widgets


def test_widgets_build_a_config():
    cfg = config_from_widgets(" main.sales.*,\nmain.crm.customers ", "/Volumes/main/x/files/out",
                              "main.crm.customers.notes")
    assert cfg.source.table_patterns == ["main.sales.*", "main.crm.customers"]
    assert cfg.output.root == "/Volumes/main/x/files/out"
    assert cfg.privacy.sensitive_columns == ["main.crm.customers.notes"]
    assert cfg.privacy.k == 10


@pytest.mark.parametrize("tables, folder, message", [
    ("", "/out", "fill in 'tables'"),
    ("sales.orders", "/out", "must be catalog.schema.table"),
    ("main.sales.*", " ", "fill in 'output_folder'"),
])
def test_widgets_explain_mistakes(tables, folder, message):
    with pytest.raises(ValueError, match=message):
        config_from_widgets(tables, folder)


def test_config_file_wins(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text("dataset: d\nsource: {tables: [a.b.c]}\noutput: {root: /x}\nprivacy: {k: 20}\n")
    assert config_from_widgets("", "", "", str(path)).privacy.k == 20
