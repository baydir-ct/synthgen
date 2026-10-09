"""Cheap within-table rules, detected from the data: date ordering and numeric identities.

Sensitive and identifier columns never take part in rules.
"""

from __future__ import annotations

from itertools import combinations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from synthgen.config import SynthgenConfig
from synthgen.profile_schema import DateOrderRule, NumericIdentityRule
from synthgen.profiler._safe import collect_single
from synthgen.profiler.stats import col, share_or_suppress, sum_if

# --- date ordering (evaluated inside the table's main aggregate) ----------------------------


def date_order_exprs(columns: list[tuple[int, str, str]]) -> list[Column]:
    """columns: (index, name, family) of non-sensitive date/timestamp columns."""
    exprs = []
    for (i, a, fa), (j, b, fb) in combinations(columns, 2):
        as_date = "date" in (fa, fb)
        ca = col(a).cast("date") if as_date else col(a)
        cb = col(b).cast("date") if as_date else col(b)
        both = ca.isNotNull() & cb.isNotNull()
        exprs += [
            sum_if(both).alias(f"do_{i}_{j}_n"),
            sum_if(both & (ca <= cb)).alias(f"do_{i}_{j}_le"),
            sum_if(both & (ca >= cb)).alias(f"do_{i}_{j}_ge"),
        ]
    return exprs


def date_order_rules(agg: dict, columns: list[tuple[int, str, str]], cfg: SynthgenConfig) -> list[DateOrderRule]:
    k = cfg.privacy.k
    rules = []
    for (i, a, _), (j, b, _) in combinations(columns, 2):
        n, le, ge = agg[f"do_{i}_{j}_n"], agg[f"do_{i}_{j}_le"], agg[f"do_{i}_{j}_ge"]
        if not n:
            continue
        for before, after, ok in ((a, b, le), (b, a, ge)):
            if ok / n >= cfg.rules.date_order_min_rate:
                suppressed: list[str] = []
                violation = share_or_suppress("match_rate", n - ok, n, k, suppressed)
                rate = None if violation is None else round(1 - violation, 6)
                rules.append(DateOrderRule(before=before, after=after, match_rate=rate, suppressed=suppressed))
                break
    return rules


# --- numeric identities (separate aggregate over a sample) ----------------------------------


def numeric_identity_rules(
    df: DataFrame, row_count: int, columns: list[str], cfg: SynthgenConfig, *, table: str
) -> list[NumericIdentityRule]:
    """Test a = b * c and a = b + c for every triple of the given numeric columns, on a sample."""
    rc = cfg.rules
    columns = columns[: rc.max_identity_columns]
    if len(columns) < 3 or row_count == 0:
        return []
    fraction = min(1.0, rc.identity_sample_rows / row_count)
    sample = df if fraction >= 1.0 else df.sample(fraction=fraction, seed=cfg.profiling.sample_seed)

    candidates: list[tuple[str, str, str]] = []
    exprs: list[Column] = []
    for a in columns:
        for b, c in combinations([x for x in columns if x != a], 2):
            xa, xb, xc = (col(x).cast("double") for x in (a, b, c))
            both = xa.isNotNull() & xb.isNotNull() & xc.isNotNull()
            tol = F.greatest(F.lit(rc.identity_abs_tolerance), F.abs(xa) * rc.identity_rel_tolerance)
            idx = len(candidates)
            exprs.append(sum_if(both).alias(f"n_{idx}"))
            for op, expr in (("product", xb * xc), ("sum", xb + xc)):
                exprs.append(sum_if(both & (F.abs(xa - expr) <= tol)).alias(f"{op}_{idx}"))
            candidates.append((a, b, c))
    agg = collect_single(sample.agg(*exprs), what=f"{table} numeric identities")

    rules = []
    for idx, (a, b, c) in enumerate(candidates):
        n = agg[f"n_{idx}"]
        if not n:
            continue
        for op in ("product", "sum"):
            ok = agg[f"{op}_{idx}"]
            if ok / n >= rc.identity_min_rate:
                suppressed: list[str] = []
                violation = share_or_suppress("match_rate", n - ok, n, cfg.privacy.k, suppressed)
                rate = None if violation is None else round(1 - violation, 6)
                rules.append(
                    NumericIdentityRule(
                        target=a, operation=op, operands=[b, c], match_rate=rate,
                        sample_fraction=round(fraction, 6), suppressed=suppressed,
                    )
                )
    return rules
