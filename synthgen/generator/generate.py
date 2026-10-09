"""Generate all tables in memory as columns of Python values (no Spark needed until writing)."""

from __future__ import annotations

import logging

from synthgen.generator import columns as C
from synthgen.generator.plan import GeneratorSettings, TablePlan
from synthgen.generator.rules import apply_rules
from synthgen.profile_schema import ColumnProfile, ForeignKey, TableProfile

log = logging.getLogger(__name__)

GeneratedTables = dict[str, dict[str, list]]  # table -> column -> values (one per row)


def _children_counts(fk: ForeignKey, n_parents: int, rng) -> list[int]:
    points = C.quantile_points(fk.children_per_parent.quantiles)
    return [max(0, int(round(C.inverse_cdf(points, u)))) for u in C.stratified_uniforms(n_parents, rng)]


def _dangling(parent_col: ColumnProfile, parent_keys: list, parent_table: str, m: int) -> list:
    """Keys that look like parent keys but do not exist in the parent (orphans)."""
    if m <= 0:
        return []
    if parent_keys and isinstance(parent_keys[0], int):
        top = max(parent_keys)
        return [top + 1 + i for i in range(m)]
    return C.key_values(parent_col, m, parent_table.split(".")[-1], offset=len(parent_keys) + 1000)


def _parent_column(profiles: dict[str, TableProfile], fk: ForeignKey) -> ColumnProfile:
    return next(c for c in profiles[fk.parent_table].columns if c.name == fk.parent_columns[0])


def _keys(plan: TablePlan, data: GeneratedTables, profiles: dict[str, TableProfile], s: GeneratorSettings) -> dict[str, list]:
    cols: dict[str, list] = {}
    fk = plan.driving_fk
    if fk:
        rng = C.rng_for(s.seed, plan.table, "rows")
        parent_keys = data[fk.parent_table][fk.parent_columns[0]]
        child, seq = [], []
        for key, count in zip(parent_keys, _children_counts(fk, len(parent_keys), rng)):
            child += [key] * count
            seq += range(1, count + 1)
        orphans = round((fk.orphan_rate or 0.0) * len(child))
        child += _dangling(_parent_column(profiles, fk), parent_keys, fk.parent_table, orphans)
        seq += [1] * orphans
        if len(child) > s.max_rows:
            log.warning("%s: %d rows capped to max_rows=%d", plan.name, len(child), s.max_rows)
            child, seq = child[: s.max_rows], seq[: s.max_rows]
        cols[fk.child_columns[0]] = child
        others = [c for c in plan.primary_key if c != fk.child_columns[0]]
        if others and fk.child_columns[0] in plan.primary_key:
            cols[others[0]] = seq  # e.g. order_lines.line_no: 1..k within each order
    n = len(next(iter(cols.values()))) if cols else plan.target_rows
    for key in plan.primary_key:
        if key not in cols:
            cols[key] = C.key_values(plan.column(key), n, plan.name)
    cols["__n__"] = [n]
    return cols


def _fk_values(fk: ForeignKey, data: GeneratedTables, profiles: dict[str, TableProfile], n: int, rng) -> list:
    parent_keys = data[fk.parent_table][fk.parent_columns[0]]
    # Popularity per parent follows the observed children-per-parent distribution; parents drawn
    # with weight 0 get no children, which reproduces the share of parents without children.
    weights = [float(w) for w in _children_counts(fk, len(parent_keys), rng)] if fk.children_per_parent and \
        fk.children_per_parent.quantiles else [1.0] * len(parent_keys)
    if sum(weights) <= 0:
        weights = [1.0] * len(parent_keys)
    picker = C.Weighted(parent_keys, weights)
    orphan_rate = fk.orphan_rate or 0.0
    orphan_pool = _dangling(_parent_column(profiles, fk), parent_keys, fk.parent_table, max(1, round(orphan_rate * n))) if orphan_rate else []
    return [rng.choice(orphan_pool) if orphan_pool and rng.random() < orphan_rate else picker.pick(rng) for _ in range(n)]


def _column_values(
    plan: TablePlan, col: ColumnProfile, data: GeneratedTables, profiles: dict[str, TableProfile], n: int, seed: int
) -> list:
    rng = C.rng_for(seed, plan.table, col.name)
    fk = plan.foreign_keys.get(col.name)
    if fk and fk.parent_table in data:
        values = _fk_values(fk, data, profiles, n, rng)
    elif col.numeric:
        values = C.numeric_values(col, n, rng)
    elif col.categorical:
        values = C.categorical_values(col, n, rng)
    elif col.date:
        values = C.date_values(col, n, rng)
    elif col.boolean:
        values = C.boolean_values(col, n, rng)
    elif col.text:
        values = C.text_values(col, n, rng)
    else:
        return [None] * n  # unsupported type or no statistics
    nulls = C.null_mask(n, col.null_rate, C.rng_for(seed, plan.table, col.name, "nulls"))
    return [None if is_null else v for v, is_null in zip(values, nulls)]


def generate(plans: list[TablePlan], settings: GeneratorSettings) -> GeneratedTables:
    data: GeneratedTables = {}
    profiles = {p.table: p.profile for p in plans}
    for plan in plans:  # phase 1: row counts and keys, parents before children
        data[plan.table] = _keys(plan, data, profiles, settings)
    for plan in plans:  # phase 2: every other column (FKs can now reference any table's keys)
        table = data[plan.table]
        n = table.pop("__n__")[0]
        for col in plan.profile.columns:
            if col.name not in table:
                table[col.name] = _column_values(plan, col, data, profiles, n, settings.seed)
        apply_rules(plan.profile, table, settings.seed)
        data[plan.table] = {c.name: table[c.name] for c in plan.profile.columns}
        log.info("generated %s: %d rows", plan.name, n)
    return data
