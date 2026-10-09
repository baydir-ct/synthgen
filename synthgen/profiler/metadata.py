"""Table metadata: names, types, comments, partitions, tags and declared keys.

Two providers:
- `SparkCatalogMetadata` uses only the Spark catalog API (works anywhere, no tags or constraints).
- `UnityCatalogMetadata` adds tags and declared PK/FK constraints from `<catalog>.information_schema`.

Both only read catalog metadata, never table rows.
"""

from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from synthgen.config import SourceConfig, SynthgenConfig
from synthgen.profile_schema import DeclaredForeignKey
from synthgen.profiler._safe import collect_metadata

log = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z0-9_\-]+$")
_MAX_META_ROWS = 100_000


def quote(*parts: str) -> str:
    for p in parts:
        if not _IDENT.match(p):
            raise ValueError(f"unsupported identifier {p!r}")
    return ".".join(f"`{p}`" for p in parts)


def split_name(full_name: str) -> tuple[str, str, str]:
    parts = full_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"expected catalog.schema.table, got {full_name!r}")
    return parts[0], parts[1], parts[2]


@dataclass
class ColumnMeta:
    name: str
    type: str
    nullable: bool
    comment: str | None
    is_partition: bool
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class TableMeta:
    full_name: str
    comment: str | None
    columns: list[ColumnMeta]
    tags: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] | None = None
    foreign_keys: list[DeclaredForeignKey] = field(default_factory=list)

    @property
    def partition_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.is_partition]


class SparkCatalogMetadata:
    name = "spark"

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # --- table discovery ------------------------------------------------------------------

    def _schemas(self, catalog: str, pattern: str) -> list[str]:
        if not any(ch in pattern for ch in "*?["):
            return [pattern]
        rows = collect_metadata(self.spark.sql(f"SHOW SCHEMAS IN {quote(catalog)}"), max_rows=_MAX_META_ROWS, what="schemas")
        return sorted(r[0] for r in rows if fnmatch.fnmatch(r[0], pattern))

    def _tables(self, catalog: str, schema: str, include_views: bool) -> list[str]:
        out = []
        for t in self.spark.catalog.listTables(f"{catalog}.{schema}"):
            if t.isTemporary or (t.tableType == "VIEW" and not include_views):
                continue
            out.append(f"{catalog}.{schema}.{t.name}")
        return out

    def resolve_tables(self, source: SourceConfig) -> list[str]:
        found: list[str] = list(source.tables)
        for pattern in source.table_patterns:
            catalog, schema_pat, table_pat = split_name(pattern)
            for schema in self._schemas(catalog, schema_pat):
                for full in self._tables(catalog, schema, source.include_views):
                    if fnmatch.fnmatch(full, f"{catalog}.{schema}.{table_pat}"):
                        found.append(full)
        excluded = [t for t in found if any(fnmatch.fnmatch(t, p) for p in source.exclude_patterns)]
        return sorted(set(found) - set(excluded))

    # --- per table ------------------------------------------------------------------------

    def table_meta(self, full_name: str) -> TableMeta:
        table = self.spark.catalog.getTable(full_name)
        columns = [
            ColumnMeta(
                name=c.name,
                type=c.dataType,
                nullable=bool(c.nullable),
                comment=c.description or None,
                is_partition=bool(c.isPartition),
            )
            for c in self.spark.catalog.listColumns(full_name)
        ]
        meta = TableMeta(full_name=full_name, comment=table.description or None, columns=columns)
        self._enrich(meta)
        return meta

    def _enrich(self, meta: TableMeta) -> None:
        """Hook for providers that know more (tags, constraints)."""


class UnityCatalogMetadata(SparkCatalogMetadata):
    name = "unity_catalog"

    def _query(self, sql: str, args: dict[str, str], what: str):
        return collect_metadata(self.spark.sql(sql, args=args), max_rows=_MAX_META_ROWS, what=what)

    def _enrich(self, meta: TableMeta) -> None:
        cat, sch, tbl = split_name(meta.full_name)
        info = quote(cat, "information_schema")
        args = {"cat": cat, "sch": sch, "tbl": tbl}

        for r in self._query(
            f"SELECT tag_name, tag_value FROM {info}.table_tags "
            "WHERE catalog_name = :cat AND schema_name = :sch AND table_name = :tbl",
            args,
            "table tags",
        ):
            meta.tags[r["tag_name"]] = r["tag_value"] or ""

        by_col = {c.name: c for c in meta.columns}
        for r in self._query(
            f"SELECT column_name, tag_name, tag_value FROM {info}.column_tags "
            "WHERE catalog_name = :cat AND schema_name = :sch AND table_name = :tbl",
            args,
            "column tags",
        ):
            if r["column_name"] in by_col:
                by_col[r["column_name"]].tags[r["tag_name"]] = r["tag_value"] or ""

        key_rows = self._query(
            f"SELECT tc.constraint_name, tc.constraint_type, k.column_name, k.ordinal_position, "
            f"k.position_in_unique_constraint "
            f"FROM {info}.table_constraints tc JOIN {info}.key_column_usage k "
            "ON k.constraint_catalog = tc.constraint_catalog AND k.constraint_schema = tc.constraint_schema "
            "AND k.constraint_name = tc.constraint_name "
            "WHERE tc.table_catalog = :cat AND tc.table_schema = :sch AND tc.table_name = :tbl "
            "AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')",
            args,
            "key constraints",
        )
        pk = sorted((r for r in key_rows if r["constraint_type"] == "PRIMARY KEY"), key=lambda r: r["ordinal_position"])
        if pk:
            meta.primary_key = [r["column_name"] for r in pk]

        fk_names = sorted({r["constraint_name"] for r in key_rows if r["constraint_type"] == "FOREIGN KEY"})
        if not fk_names:
            return
        parent_rows = self._query(
            f"SELECT rc.constraint_name, p.table_catalog, p.table_schema, p.table_name, p.column_name, "
            f"p.ordinal_position "
            f"FROM {info}.referential_constraints rc JOIN {info}.key_column_usage p "
            "ON p.constraint_catalog = rc.unique_constraint_catalog "
            "AND p.constraint_schema = rc.unique_constraint_schema "
            "AND p.constraint_name = rc.unique_constraint_name "
            "WHERE rc.constraint_catalog = :cat AND rc.constraint_schema = :sch",
            {"cat": cat, "sch": sch},
            "referential constraints",
        )
        for name in fk_names:
            child = sorted(
                (r for r in key_rows if r["constraint_name"] == name and r["constraint_type"] == "FOREIGN KEY"),
                key=lambda r: r["ordinal_position"],
            )
            parents = {r["ordinal_position"]: r for r in parent_rows if r["constraint_name"] == name}
            if not parents:
                log.warning("FK %s on %s: parent key not visible in %s.information_schema", name, meta.full_name, cat)
                continue
            first = next(iter(parents.values()))
            parent_cols = [parents[r["position_in_unique_constraint"] or r["ordinal_position"]]["column_name"] for r in child]
            meta.foreign_keys.append(
                DeclaredForeignKey(
                    name=name,
                    columns=[r["column_name"] for r in child],
                    parent_table=f"{first['table_catalog']}.{first['table_schema']}.{first['table_name']}",
                    parent_columns=parent_cols,
                )
            )


def make_metadata_provider(spark: SparkSession, cfg: SynthgenConfig, catalogs: set[str] | None = None) -> SparkCatalogMetadata:
    if cfg.metadata_provider == "spark":
        return SparkCatalogMetadata(spark)
    if cfg.metadata_provider == "unity_catalog":
        return UnityCatalogMetadata(spark)
    catalogs = catalogs or {t.split(".")[0] for t in cfg.source.tables + cfg.source.table_patterns}
    has_info = all(spark.catalog.tableExists(f"{c}.information_schema.table_tags") for c in catalogs)
    provider = UnityCatalogMetadata(spark) if has_info else SparkCatalogMetadata(spark)
    log.info("metadata provider: %s", provider.name)
    return provider
