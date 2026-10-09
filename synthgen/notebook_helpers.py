"""Glue for the Databricks notebooks: turn the configuration cell into a config object."""

from __future__ import annotations

from typing import Any

from synthgen.config import SynthgenConfig


def split_list(value: str | list[str] | tuple[str, ...]) -> list[str]:
    items = value.replace("\n", ",").split(",") if isinstance(value, str) else list(value)
    return [item.strip() for item in items if item and item.strip()]


def _merge(base: dict, extra: dict) -> dict:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge(base[key], value)
        else:
            base[key] = value
    return base


def config_from_notebook(
    tables: str | list[str],
    output_folder: str,
    extra_sensitive_columns: str | list[str] = (),
    advanced_settings: dict[str, Any] | None = None,
) -> SynthgenConfig:
    """Defaults, plus the few values a person fills in at the top of the notebook.

    tables: catalog.schema.table names or patterns such as main.sales.*
    extra_sensitive_columns: catalog.schema.table.column names (globs allowed)
    advanced_settings: optional overrides with the same structure as config/example_dataset.yaml,
        e.g. {"privacy": {"k": 20}, "profiling": {"sample_fraction": 0.1}}
    """
    patterns = split_list(tables)
    if not patterns:
        raise ValueError('TABLES is empty: list at least one table, e.g. ["main.sales.*"]')
    for p in patterns:
        if p.count(".") != 2:
            raise ValueError(f"'{p}' in TABLES must be catalog.schema.table (use * for all tables of a schema)")
    if not output_folder or not output_folder.strip():
        raise ValueError('OUTPUT_FOLDER is empty, e.g. "/Volumes/main/sandbox/files/synthgen"')
    raw: dict[str, Any] = {
        "dataset": "genie_demo",
        "source": {"table_patterns": patterns},
        "output": {"root": output_folder.strip()},
        "privacy": {"sensitive_columns": split_list(extra_sensitive_columns)},
    }
    return SynthgenConfig.model_validate(_merge(raw, advanced_settings or {}))
