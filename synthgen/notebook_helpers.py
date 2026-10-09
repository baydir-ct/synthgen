"""Glue for the Databricks notebooks: turn widget values into a config."""

from __future__ import annotations

from synthgen.config import SynthgenConfig


def split_list(value: str) -> list[str]:
    return [item.strip() for item in value.replace("\n", ",").split(",") if item.strip()]


def config_from_widgets(
    tables: str, output_folder: str, sensitive_columns: str = "", config_path: str = ""
) -> SynthgenConfig:
    """A YAML config file wins when given; otherwise defaults plus the notebook's fill-in boxes.

    tables: comma-separated catalog.schema.table names or patterns such as main.sales.*
    sensitive_columns: comma-separated catalog.schema.table.column names (globs allowed)
    """
    if config_path.strip():
        return SynthgenConfig.load(config_path.strip())
    patterns = split_list(tables)
    if not patterns:
        raise ValueError("fill in 'tables', e.g. main.sales.* or main.sales.orders, main.sales.customers")
    for p in patterns:
        if p.count(".") != 2:
            raise ValueError(f"'{p}' must be catalog.schema.table (use * for all tables of a schema)")
    if not output_folder.strip():
        raise ValueError("fill in 'output_folder', e.g. /Volumes/main/sandbox/files/synthgen")
    return SynthgenConfig.model_validate(
        {
            "dataset": "genie_demo",
            "source": {"table_patterns": patterns},
            "output": {"root": output_folder.strip()},
            "privacy": {"sensitive_columns": split_list(sensitive_columns)},
        }
    )
