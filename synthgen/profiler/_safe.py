"""The only place profiler code may bring Spark results to the driver.

Shipped profiler code must not call collect/show/take/toPandas/display anywhere else; a test scans
the source to enforce it. Every DataFrame passed here must be an aggregate (or catalog metadata) that
was built so that no individual row value can survive: callers filter buckets below k inside Spark.
The row cap is a backstop against a caller accidentally passing a row-level DataFrame.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, Row


class RowLevelOutputError(RuntimeError):
    pass


def collect_aggregate(df: DataFrame, *, max_rows: int, what: str) -> list[Row]:
    rows = df.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise RowLevelOutputError(f"{what}: expected at most {max_rows} aggregate rows, got more")
    return rows


def collect_single(df: DataFrame, *, what: str) -> dict[str, Any]:
    rows = collect_aggregate(df, max_rows=1, what=what)
    if not rows:
        raise RowLevelOutputError(f"{what}: aggregate returned no row")
    return rows[0].asDict(recursive=True)


def collect_metadata(df: DataFrame, *, max_rows: int, what: str) -> list[Row]:
    """Catalog / information_schema queries only. Never pass a DataFrame built on a source table."""
    return collect_aggregate(df, max_rows=max_rows, what=what)
