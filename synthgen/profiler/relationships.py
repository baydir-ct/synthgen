"""Primary key candidates and foreign keys with containment, orphan rate and children-per-parent stats."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from itertools import combinations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from synthgen import __version__
from synthgen.config import SynthgenConfig
from synthgen.profile_schema import ChildrenPerParent, DeclaredForeignKey, ForeignKey, PrimaryKey, Relationships
from synthgen.profiler._safe import collect_single
from synthgen.profiler.detect import fk_name_score, singular
from synthgen.profiler.stats import col, qkey, rnd, share_or_suppress, sum_if

log = logging.getLogger(__name__)


@dataclass
class KeyColumn:
    name: str
    family: str
    sensitive: bool
    id_named: bool


@dataclass
class KeyInfo:
    """What relationship detection needs to know about one profiled table."""

    table: str
    df: DataFrame  # full table, never sampled
    row_count: int
    columns: dict[str, KeyColumn]
    declared_pk: list[str] | None = None
    declared_fks: list[DeclaredForeignKey] = field(default_factory=list)
    unique_singles: list[str] = field(default_factory=list)
    unique_pairs: list[tuple[str, str]] = field(default_factory=list)


# --- primary keys -----------------------------------------------------------------------------


def find_unique_keys(
    df: DataFrame,
    row_count: int,
    singles: list[str],
    pair_pool: list[str],
    cfg: SynthgenConfig,
    *,
    table: str,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Exact uniqueness checks on the full table. Pairs are tested only if no single column is unique."""
    if row_count == 0:
        return [], []
    found: list[str] = []
    if singles:
        exprs = []
        for i, name in enumerate(singles):
            exprs += [F.countDistinct(col(name)).alias(f"d{i}"), F.count(col(name)).alias(f"n{i}")]
        agg = collect_single(df.agg(*exprs), what=f"{table} key uniqueness")
        found = [n for i, n in enumerate(singles) if agg[f"d{i}"] == agg[f"n{i}"] == row_count]
    if found:
        return found, []
    pool = pair_pool[: cfg.relationships.max_pk_pair_columns]
    pairs = list(combinations(pool, 2))
    if not pairs:
        return [], []
    exprs = []
    for i, (a, b) in enumerate(pairs):
        exprs += [
            F.countDistinct(col(a), col(b)).alias(f"d{i}"),
            sum_if(col(a).isNotNull() & col(b).isNotNull()).alias(f"n{i}"),
        ]
    agg = collect_single(df.agg(*exprs), what=f"{table} key pair uniqueness")
    return [], [p for i, p in enumerate(pairs) if agg[f"d{i}"] == agg[f"n{i}"] == row_count]


def _single_pk_rank(table: str, column: KeyColumn) -> tuple[int, str]:
    name = column.name.lower()
    stem = singular(table.split(".")[-1].lower())
    if name in (f"{stem}_id", "id"):
        return (0, name)
    if column.id_named:
        return (1, name)
    return (2, name)


def choose_primary_key(info: KeyInfo, cfg: SynthgenConfig) -> PrimaryKey | None:
    forced = cfg.relationships.forced_pks.get(info.table)
    uniques = [[c] for c in sorted(info.unique_singles, key=lambda c: _single_pk_rank(info.table, info.columns[c]))]
    uniques += [list(p) for p in info.unique_pairs]
    if info.declared_pk:
        return PrimaryKey(table=info.table, columns=info.declared_pk, source="declared",
                          alternatives=[u for u in uniques if u != info.declared_pk])
    if forced:
        return PrimaryKey(table=info.table, columns=forced, source="config",
                          alternatives=[u for u in uniques if u != forced])
    if uniques:
        return PrimaryKey(table=info.table, columns=uniques[0], source="inferred", alternatives=uniques[1:])
    return None


# --- foreign keys -----------------------------------------------------------------------------


def fk_stats(child: DataFrame, child_col: str, parent: DataFrame, parent_col: str, cfg: SynthgenConfig, *, what: str) -> dict:
    """Aggregate-only statistics for a single-column FK. Returns raw counts plus quantiles."""
    child_counts = (
        child.select(col(child_col).alias("k")).where(F.col("k").isNotNull()).groupBy("k").agg(F.count(F.lit(1)).alias("n"))
    )
    parent_keys = parent.select(col(parent_col).alias("k")).where(F.col("k").isNotNull()).distinct()

    totals = collect_single(
        child_counts.agg(F.count(F.lit(1)).alias("values"), F.sum("n").alias("rows")), what=f"{what} child totals"
    )
    orphans = collect_single(
        child_counts.join(parent_keys, "k", "left_anti").agg(
            F.count(F.lit(1)).alias("values"), F.sum("n").alias("rows")
        ),
        what=f"{what} orphans",
    )
    qs = cfg.relationships.children_quantiles
    per_parent = (
        parent_keys.join(child_counts, "k", "left").select(F.coalesce(F.col("n"), F.lit(0)).alias("n"))
    )
    cpp = collect_single(
        per_parent.agg(
            F.count(F.lit(1)).alias("parents"),
            sum_if(F.col("n") == 0).alias("zero"),
            F.when(
                F.count(F.lit(1)) >= cfg.privacy.min_count_for_distribution,
                F.percentile_approx(F.col("n").cast("double"), qs, cfg.profiling.percentile_accuracy),
            ).alias("q"),
        ),
        what=f"{what} children per parent",
    )
    return {
        "child_values": totals["values"] or 0,
        "child_rows": totals["rows"] or 0,
        "orphan_values": orphans["values"] or 0,
        "orphan_rows": orphans["rows"] or 0,
        "parents": cpp["parents"] or 0,
        "zero_parents": cpp["zero"] or 0,
        "quantiles": {qkey(q): rnd(v) for q, v in zip(qs, cpp["q"])} if cpp["q"] else {},
    }


def _fk_from_stats(base: dict, stats: dict, cfg: SynthgenConfig) -> ForeignKey:
    k = cfg.privacy.k
    suppressed: list[str] = []
    orphan_rate = share_or_suppress("orphan_rate", stats["orphan_rows"], stats["child_rows"], k, suppressed)
    orphan_values = share_or_suppress("orphan_value_share", stats["orphan_values"], stats["child_values"], k, suppressed)
    zero_share = share_or_suppress(
        "children_per_parent.zero_children_share", stats["zero_parents"], stats["parents"], k, suppressed
    )
    return ForeignKey(
        **base,
        containment=None if orphan_rate is None else round(1 - orphan_rate, 6),
        orphan_rate=orphan_rate,
        orphan_value_share=orphan_values,
        children_per_parent=ChildrenPerParent(quantiles=stats["quantiles"], zero_children_share=zero_share),
        suppressed=suppressed,
    )


def _containment(stats: dict) -> float:
    """Row-based: share of non-null child rows whose value exists in the parent key."""
    return 1 - stats["orphan_rows"] / stats["child_rows"] if stats["child_rows"] else 0.0


def _split_column(full: str) -> tuple[str, str]:
    table, _, column = full.rpartition(".")
    return table, column


def infer_relationships(tables: dict[str, KeyInfo], cfg: SynthgenConfig, generated_at: datetime) -> Relationships:
    pks = {name: pk for name, info in tables.items() if (pk := choose_primary_key(info, cfg))}
    fks: list[ForeignKey] = []
    claimed: set[tuple[str, str]] = set()  # (child_table, child_column) already linked

    def explicit(child_table: str, child_cols: list[str], parent_table: str, parent_cols: list[str], source: str) -> None:
        base = dict(child_table=child_table, child_columns=child_cols, parent_table=parent_table,
                    parent_columns=parent_cols, source=source)
        claimed.update((child_table, c) for c in child_cols)
        if len(child_cols) != 1:
            fks.append(ForeignKey(**base, warnings=["composite foreign key: statistics not computed in pass 1"]))
            return
        if parent_table not in tables:
            fks.append(ForeignKey(**base, warnings=["parent table not profiled: statistics not computed"]))
            return
        stats = fk_stats(tables[child_table].df, child_cols[0], tables[parent_table].df, parent_cols[0], cfg,
                         what=f"{child_table}.{child_cols[0]}")
        fks.append(_fk_from_stats(base, stats, cfg))

    for name, info in tables.items():
        for d in info.declared_fks:
            explicit(name, d.columns, d.parent_table, d.parent_columns, "declared")
    for forced in cfg.relationships.forced_fks:
        (ct, cc), (pt, pc) = _split_column(forced.child), _split_column(forced.parent)
        if (ct, cc) not in claimed and ct in tables:
            explicit(ct, [cc], pt, [pc], "config")

    # Inference: single-column parent keys only.
    parents = [(t, pk.columns[0]) for t, pk in pks.items() if len(pk.columns) == 1]
    for child_table, info in tables.items():
        child_pk = pks.get(child_table)
        for column in info.columns.values():
            if column.sensitive or (child_table, column.name) in claimed:
                continue
            best: tuple[float, float, str, str, dict] | None = None
            for parent_table, parent_col in parents:
                if parent_table == child_table and child_pk and child_pk.columns == [column.name]:
                    continue  # a table's own single-column key
                pcol = tables[parent_table].columns[parent_col]
                if pcol.family != column.family:
                    continue
                score = fk_name_score(column.name, parent_table, parent_col)
                if score < cfg.relationships.name_similarity_threshold:
                    continue
                stats = fk_stats(info.df, column.name, tables[parent_table].df, parent_col, cfg,
                                 what=f"{child_table}.{column.name}")
                containment = _containment(stats)
                log.info("FK candidate %s.%s -> %s.%s score=%.2f", child_table, column.name, parent_table, parent_col, score)
                if containment >= cfg.relationships.fk_min_containment and (
                    best is None or (containment, score) > (best[0], best[1])
                ):
                    best = (containment, score, parent_table, parent_col, stats)
            if best:
                _, score, parent_table, parent_col, stats = best
                base = dict(child_table=child_table, child_columns=[column.name], parent_table=parent_table,
                            parent_columns=[parent_col], source="inferred", name_score=round(score, 3))
                fks.append(_fk_from_stats(base, stats, cfg))
                claimed.add((child_table, column.name))

    return Relationships(
        generated_at=generated_at,
        profiler_version=__version__,
        k_threshold=cfg.privacy.k,
        tables=sorted(tables),
        primary_keys=[pks[t] for t in sorted(pks)],
        foreign_keys=sorted(fks, key=lambda f: (f.child_table, f.child_columns)),
    )
