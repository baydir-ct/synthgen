"""Quick checks on the written synthetic tables: row counts and how well the links between tables join."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from synthgen.generator.plan import TablePlan
from synthgen.generator.write import target_names


def check_tables(spark: SparkSession, plans: list[TablePlan], catalog: str, schema: str) -> list[str]:
    names = {src: ".".join(parts) for src, parts in target_names(plans, catalog, schema).items()}
    lines = []
    for plan in plans:
        lines.append(f"{names[plan.table]}: {spark.table(names[plan.table]).count()} rows")
    for plan in plans:
        for column, fk in plan.foreign_keys.items():
            if fk.parent_table not in names:
                continue
            child = spark.table(names[plan.table]).select(F.col(f"`{column}`").alias("k")).where(F.col("k").isNotNull())
            parent = spark.table(names[fk.parent_table]).select(F.col(f"`{fk.parent_columns[0]}`").alias("k"))
            total = child.count()
            joined = total - child.join(parent, "k", "left_anti").count()
            share = 100.0 * joined / total if total else 100.0
            expected = 100.0 * (1 - (fk.orphan_rate or 0.0))
            lines.append(f"{plan.name}.{column} -> {fk.parent_table.split('.')[-1]}.{fk.parent_columns[0]}: "
                         f"{share:.1f}% of rows join (source: {expected:.1f}%)")
    return lines
