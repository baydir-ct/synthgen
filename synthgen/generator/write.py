"""Write generated tables to the target catalog: same names, column types, comments; keys declared."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from synthgen.generator.columns import to_spark_value
from synthgen.generator.generate import GeneratedTables
from synthgen.generator.plan import GeneratorSettings, TablePlan

log = logging.getLogger(__name__)

SOURCE_PROPERTY = "synthgen.source_table"
CHUNK_ROWS = 200_000


class TargetTableError(RuntimeError):
    pass


@dataclass
class WriteReport:
    tables: dict[str, int] = field(default_factory=dict)  # target table -> rows
    warnings: list[str] = field(default_factory=list)


def _q(*parts: str) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def _lit(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def target_names(plans: list[TablePlan], catalog: str, schema: str) -> dict[str, tuple[str, str, str]]:
    """Source table -> (catalog, schema, name). Same short name in two source schemas gets a prefix."""
    names = [p.name for p in plans]
    out = {}
    for p in plans:
        name = p.name if names.count(p.name) == 1 else f"{p.table.split('.')[1]}_{p.name}"
        out[p.table] = (catalog, schema, name)
    return out


def _created_by_synthgen(spark: SparkSession, table: str) -> bool:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return any(r[0] == SOURCE_PROPERTY for r in rows)


def write_tables(
    spark: SparkSession,
    plans: list[TablePlan],
    data: GeneratedTables,
    catalog: str,
    schema: str,
    settings: GeneratorSettings,
    *,
    table_format: str = "delta",
    declare_keys: bool = True,
    replace_existing: bool = False,
) -> WriteReport:
    report = WriteReport()
    names = target_names(plans, catalog, schema)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_q(catalog, schema)}")
    except Exception as exc:  # noqa: BLE001 - schema may exist without CREATE permission
        report.warnings.append(f"could not create schema {catalog}.{schema}: {exc}".splitlines()[0])

    # Refuse to drop tables that this tool did not create, before touching anything.
    for plan in plans:
        target = _q(*names[plan.table])
        if spark.catalog.tableExists(".".join(names[plan.table])) and not replace_existing \
                and not _created_by_synthgen(spark, target):
            raise TargetTableError(f"{'.'.join(names[plan.table])} exists and was not created by synthgen; "
                                   "pick another schema or set REPLACE_EXISTING = True")

    for plan in plans:
        target = _q(*names[plan.table])
        key = set(plan.primary_key) if declare_keys else set()
        cols = ",\n  ".join(
            f"{_q(c.name)} {c.type}{' NOT NULL' if c.name in key else ''}"
            + (f" COMMENT {_lit(c.comment)}" if c.comment else "")
            for c in plan.profile.columns
        )
        props = {
            SOURCE_PROPERTY: plan.table,
            "synthgen.profile_version": plan.profile.schema_version,
            "synthgen.seed": str(settings.seed),
            "synthgen.scale": f"{settings.scale:g}",
        }
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        spark.sql(
            f"CREATE TABLE {target} (\n  {cols}\n) USING {table_format}"
            + (f"\nCOMMENT {_lit(plan.profile.comment)}" if plan.profile.comment else "")
            + "\nTBLPROPERTIES (" + ", ".join(f"{_lit(k)} = {_lit(v)}" for k, v in props.items()) + ")"
        )
        schema_struct = spark.table(target).schema
        types = [c.type for c in plan.profile.columns]
        values = data[plan.table]
        n = len(values[plan.profile.columns[0].name]) if plan.profile.columns else 0
        column_lists = [values[c.name] for c in plan.profile.columns]
        for start in range(0, n, CHUNK_ROWS):
            rows = [
                tuple(to_spark_value(col[i], t) for col, t in zip(column_lists, types))
                for i in range(start, min(n, start + CHUNK_ROWS))
            ]
            spark.createDataFrame(rows, schema=schema_struct).write.insertInto(target)
        report.tables[".".join(names[plan.table])] = n
        log.info("wrote %s: %d rows", ".".join(names[plan.table]), n)

    if declare_keys:
        for plan in plans:
            target = _q(*names[plan.table])
            name = names[plan.table][2]
            statements = []
            if plan.primary_key:
                statements.append(f"ALTER TABLE {target} ADD CONSTRAINT {_q(name + '_pk')} "
                                  f"PRIMARY KEY ({', '.join(_q(c) for c in plan.primary_key)})")
            for column, fk in plan.foreign_keys.items():
                parent = names.get(fk.parent_table)
                if parent:
                    statements.append(
                        f"ALTER TABLE {target} ADD CONSTRAINT {_q(f'{name}_{column}_fk')} FOREIGN KEY ({_q(column)}) "
                        f"REFERENCES {_q(*parent)} ({_q(fk.parent_columns[0])})"
                    )
            for sql in statements:
                try:
                    spark.sql(sql)
                except Exception as exc:  # noqa: BLE001 - constraints are informational; keep going
                    report.warnings.append(f"{sql.split(' ADD ')[1][:80]}: {str(exc).splitlines()[0][:160]}")
    return report
