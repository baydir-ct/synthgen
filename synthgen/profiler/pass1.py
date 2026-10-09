"""Pass 1: generic profiler. Knows nothing about business questions.

Per table, Spark runs:
1. one aggregate over the (optionally sampled) table for all column statistics and date-order rules;
2. one frequency pass (explode + group by) for category labels, format patterns and date/hour buckets,
   with k-suppression and top-N applied inside Spark so rare values never reach the driver;
3. small aggregates for exact key uniqueness and numeric identities.
Then relationships are inferred across tables and roles are finalised before anything is written.

Privacy rules applied here (and re-checked by the privacy guard in milestone 2):
- only aggregates are collected, through `_safe`;
- every count below k is suppressed (rate set to null, field listed in `suppressed`);
- sensitive, identifier and high-cardinality columns export no values;
- min/max are clipped quantiles, mean/std are computed on values capped at them;
  distributions need `min_count_for_distribution` non-null rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from synthgen import __version__
from synthgen.config import SynthgenConfig
from synthgen.io import ProfileStore
from synthgen.profile_schema import (
    BooleanStats,
    CategoricalStats,
    ColumnProfile,
    DateStats,
    LengthStats,
    NumericStats,
    PatternShare,
    Relationships,
    TableProfile,
    TextStats,
    ValueShare,
)
from synthgen.profiler import detect
from synthgen.profiler._safe import collect_aggregate, collect_single
from synthgen.profiler.metadata import ColumnMeta, SparkCatalogMetadata, TableMeta, make_metadata_provider
from synthgen.profiler.relationships import KeyColumn, KeyInfo, find_unique_keys, infer_relationships
from synthgen.profiler.rules import date_order_exprs, date_order_rules, numeric_identity_rules
from synthgen.profiler.stats import (
    col,
    days_to_iso,
    micros_to_iso,
    qkey,
    rnd,
    share_or_suppress,
    sum_if,
)

log = logging.getLogger(__name__)

DOW_NAMES = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
DOW_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


class ExportPolicyError(RuntimeError):
    """Raised when a profile would export something the hard constraints forbid."""


@dataclass
class _Col:
    meta: ColumnMeta
    idx: int
    family: str
    id_named: bool
    static: detect.Sensitivity
    sensitive: bool = False
    reasons: list[str] = field(default_factory=list)
    kind: str = "none"  # numeric | categorical | text | date | boolean | none
    role: str = "other"

    @property
    def name(self) -> str:
        return self.meta.name

    @property
    def stringish(self) -> bool:
        """Gets length (and, for strings, value-regex) analysis."""
        return self.family != "other" and (self.family == "string" or self.static.sensitive or self.id_named)

    @property
    def value_stats_allowed(self) -> bool:
        return not (self.static.sensitive or self.id_named)


@dataclass
class TableResult:
    profile: TableProfile
    keys: KeyInfo
    cols: dict[str, _Col]


@dataclass
class Pass1Result:
    profiles: list[TableProfile]
    relationships: Relationships
    written: list[Path]


# --- aggregate expressions --------------------------------------------------------------------


def _gate(c: Column, cfg: SynthgenConfig, agg: Column) -> Column:
    """Evaluate a distribution aggregate only when enough non-null rows exist (checked inside Spark)."""
    return F.when(F.count(c) >= cfg.privacy.min_count_for_distribution, agg)


def _quantile_list(cfg: SynthgenConfig) -> list[float]:
    return sorted(set(cfg.profiling.quantiles) | {cfg.privacy.clip_lower, cfg.privacy.clip_upper})


def _column_exprs(c: _Col, cfg: SynthgenConfig) -> list[Column]:
    p, acc = f"c{c.idx}_", cfg.profiling.percentile_accuracy
    x = col(c.name)
    exprs = [F.count(x).alias(p + "nn")]
    if c.family != "other":
        exprs.append(F.approx_count_distinct(x, cfg.profiling.approx_distinct_rsd).alias(p + "nd"))

    if c.stringish:
        s = x.cast("string")
        exprs += [
            _gate(x, cfg, F.percentile_approx(F.length(s), [cfg.privacy.clip_lower, cfg.privacy.clip_upper], acc)).alias(p + "len_q"),
            _gate(x, cfg, F.avg(F.length(s))).alias(p + "len_mean"),
        ]
        if c.family == "string":
            exprs += [sum_if(s.rlike(rx)).alias(f"{p}rx_{sem}") for sem, rx in detect.VALUE_REGEXES.items()]
            exprs.append(sum_if(s.rlike(r"\s")).alias(p + "space"))

    if not c.value_stats_allowed:
        return exprs

    if detect.is_numeric_family(c.family):
        v = x.cast("double")
        exprs += [
            _gate(x, cfg, F.percentile_approx(v, _quantile_list(cfg), acc)).alias(p + "q"),
            sum_if(v == 0).alias(p + "zero"),
            sum_if(v < 0).alias(p + "neg"),
        ]
        if c.family != "integer":
            exprs += [
                sum_if(v != F.floor(v)).alias(p + "nonint"),
                F.max(F.length(F.regexp_extract(x.cast("string"), r"\.(\d*?)0*$", 1))).alias(p + "dec"),
            ]
    elif c.family in ("date", "timestamp"):
        d = F.unix_date(x) if c.family == "date" else F.unix_micros(x.cast("timestamp"))
        exprs.append(_gate(x, cfg, F.percentile_approx(d, _quantile_list(cfg), acc)).alias(p + "q"))
        if c.family == "timestamp":
            ts = x.cast("timestamp")
            exprs.append(sum_if(ts != F.date_trunc("day", ts)).alias(p + "nonmidnight"))
    elif c.family == "boolean":
        exprs += [sum_if(x == F.lit(True)).alias(p + "t"), sum_if(x == F.lit(False)).alias(p + "f")]
    return exprs


def _clipped_moments(df: DataFrame, cols: list[_Col], agg: dict, cfg: SynthgenConfig, table: str) -> None:
    """Mean and std of numeric columns with values capped at the clip quantiles, so a few extreme
    values cannot inflate them. Needs the quantiles first, hence a second aggregate. Updates agg."""
    lo_i, hi_i = (_quantile_list(cfg).index(q) for q in (cfg.privacy.clip_lower, cfg.privacy.clip_upper))
    exprs = []
    for c in cols:
        q = agg.get(f"c{c.idx}_q")
        if c.kind != "numeric" or not q:
            continue
        v = F.least(F.greatest(col(c.name).cast("double"), F.lit(q[lo_i])), F.lit(q[hi_i]))
        exprs += [F.avg(v).alias(f"c{c.idx}_mean"), F.stddev_samp(v).alias(f"c{c.idx}_std")]
    if exprs:
        agg.update(collect_single(df.agg(*exprs), what=f"{table} clipped moments"))


def _pattern(s: Column, cfg: SynthgenConfig) -> Column:
    masked = F.regexp_replace(F.regexp_replace(s, r"\p{L}", "A"), r"\p{Nd}", "9")
    return F.when(F.length(s) <= cfg.profiling.pattern_max_length, masked)


def _frequency_items(cols: list[_Col], cfg: SynthgenConfig) -> tuple[list[Column], dict[str, int]]:
    items: list[Column] = []

    def item(c: _Col, kind: str, key: Column) -> None:
        items.append(F.struct(F.lit(c.idx).alias("c"), F.lit(kind).alias("k"), key.cast("string").alias("v")))

    for c in cols:
        x = col(c.name)
        if c.kind == "categorical":
            item(c, "v", x)
        elif c.kind == "text":
            item(c, "p", _pattern(x.cast("string"), cfg))
        elif c.kind == "date":
            item(c, "ym", F.date_format(x, "yyyy-MM"))
            item(c, "dow", F.dayofweek(x))
            if c.family == "timestamp":
                item(c, "h", F.lpad(F.hour(x).cast("string"), 2, "0"))
    limits = {"v": cfg.privacy.top_k_values, "p": cfg.profiling.pattern_top_n,
              "ym": cfg.profiling.max_year_months, "dow": 7, "h": 24}
    return items, limits


def _frequencies(df: DataFrame, cols: list[_Col], cfg: SynthgenConfig, table: str):
    """Returns (kept, buckets): kept[(idx, kind)] = [(key, count)] with count >= k, top-N by count;
    buckets[(idx, kind)] = number of distinct keys seen. Rare keys never leave Spark."""
    items, limits = _frequency_items(cols, cfg)
    if not items:
        return {}, {}
    k = cfg.privacy.k
    grouped = (
        df.select(F.explode(F.array(*items)).alias("e"))
        .select("e.c", "e.k", "e.v")
        .where(F.col("v").isNotNull())
        .groupBy("c", "k", "v")
        .agg(F.count(F.lit(1)).alias("n"))
    )
    limit = F.lit(0)
    for kind, n in limits.items():
        limit = F.when(F.col("k") == kind, F.lit(n)).otherwise(limit)
    rank = F.row_number().over(Window.partitionBy("c", "k").orderBy(F.col("n").desc(), F.col("v")))
    top = grouped.where(F.col("n") >= k).withColumn("r", rank).where(F.col("r") <= limit).select("c", "k", "v", "n")

    max_rows = sum(limits[r] for r in limits) * len(cols)
    kept: dict[tuple[int, str], list[tuple[str, int]]] = {}
    for r in collect_aggregate(top, max_rows=max_rows, what=f"{table} frequencies"):
        if r["n"] < k:  # defence in depth: Spark already filtered
            raise ExportPolicyError(f"{table}: bucket below k reached the driver")
        kept.setdefault((r["c"], r["k"]), []).append((r["v"], r["n"]))
    buckets = {
        (r["c"], r["k"]): r["b"]
        for r in collect_aggregate(
            grouped.groupBy("c", "k").agg(F.count(F.lit(1)).alias("b")), max_rows=len(items), what=f"{table} bucket counts"
        )
    }
    for v in kept.values():
        v.sort(key=lambda kv: (-kv[1], kv[0]))
    return kept, buckets


# --- per-column decisions and stats -----------------------------------------------------------


def _decide(c: _Col, agg: dict, cfg: SynthgenConfig) -> None:
    p = f"c{c.idx}_"
    nn = agg[p + "nn"]
    reasons = list(c.static.reasons)
    if c.family == "string" and nn:
        shares = {sem: agg[f"{p}rx_{sem}"] / nn for sem in detect.VALUE_REGEXES}
        reasons += [r for r in detect.value_pii_reasons(shares, c.id_named, cfg) if r not in reasons]
    c.reasons, c.sensitive = reasons, bool(reasons)

    if c.family == "other":
        c.kind, c.role = "none", "other"
    elif c.sensitive or c.id_named:
        c.kind = "text"
        c.role = {"date": "date", "timestamp": "date", "boolean": "boolean"}.get(
            c.family, "numeric" if detect.is_numeric_family(c.family) and not c.id_named else "text"
        )
    elif c.family == "boolean":
        c.kind = c.role = "boolean"
    elif detect.is_numeric_family(c.family):
        c.kind = c.role = "numeric"
    elif c.family in ("date", "timestamp"):
        c.kind = c.role = "date"
    else:
        nd = min(agg.get(p + "nd") or 0, nn)
        categorical = detect.string_is_categorical(nd, nd / nn if nn else None, cfg)
        c.kind = c.role = "categorical" if categorical else "text"


def _length(agg: dict, p: str) -> LengthStats:
    q = agg.get(p + "len_q")
    return LengthStats(
        min_clipped=rnd(q[0]) if q else None,
        mean=rnd(agg.get(p + "len_mean")),
        max_clipped=rnd(q[1]) if q else None,
    )


def _shares(pairs: list[tuple[str, int]], total: int) -> tuple[list[tuple[str, float]], float]:
    shares = [(key, rnd(n / total)) for key, n in pairs]
    kept = sum(n for _, n in pairs)
    return shares, max(0.0, rnd(1 - kept / total)) if total else 0.0


def _numeric_stats(c: _Col, agg: dict, nn: int, cfg: SynthgenConfig, suppressed: list[str]) -> NumericStats:
    p, k = f"c{c.idx}_", cfg.privacy.k
    qs, q = _quantile_list(cfg), agg[f"c{c.idx}_q"]
    quantiles = {qkey(a): rnd(v) for a, v in zip(qs, q)} if q else {}
    nonint = agg.get(p + "nonint", 0)
    is_integer = c.family == "integer" or nonint == 0
    cap = detect.decimal_scale(c.meta.type)
    cap = cfg.profiling.max_decimals if cap is None else min(cap, cfg.profiling.max_decimals)
    return NumericStats(
        min_clipped=quantiles.get(qkey(cfg.privacy.clip_lower)),
        max_clipped=quantiles.get(qkey(cfg.privacy.clip_upper)),
        mean=rnd(agg.get(p + "mean")),
        std=rnd(agg.get(p + "std")),
        quantiles={key: v for key, v in quantiles.items() if float(key) in cfg.profiling.quantiles},
        zero_share=share_or_suppress("numeric.zero_share", agg[p + "zero"], nn, k, suppressed),
        negative_share=share_or_suppress("numeric.negative_share", agg[p + "neg"], nn, k, suppressed),
        is_integer=is_integer,
        decimals=0 if is_integer else min(int(agg.get(p + "dec") or 0), cap),
    )


def _date_stats(c: _Col, agg: dict, nn: int, kept, cfg: SynthgenConfig, session_tz: str) -> DateStats:
    p = f"c{c.idx}_"
    ntz = c.meta.type.lower() == "timestamp_ntz"
    if c.family == "date":
        conv = days_to_iso
    else:
        def conv(v):
            return micros_to_iso(v, ntz=ntz, session_tz=session_tz)
    qs, q = _quantile_list(cfg), agg[p + "q"]
    quantiles = {qkey(a): conv(v) for a, v in zip(qs, q)} if q else {}
    ym, ym_other = _shares(sorted(kept.get((c.idx, "ym"), [])), nn)
    dow = {DOW_NAMES[int(key)]: s for key, s in _shares(kept.get((c.idx, "dow"), []), nn)[0]}
    stats = DateStats(
        min_clipped=quantiles.get(qkey(cfg.privacy.clip_lower)),
        max_clipped=quantiles.get(qkey(cfg.privacy.clip_upper)),
        quantiles={key: v for key, v in quantiles.items() if float(key) in cfg.profiling.quantiles},
        year_month_shares=dict(ym),
        other_year_month_share=ym_other,
        day_of_week_shares={d: dow[d] for d in DOW_ORDER if d in dow},
    )
    if c.family == "timestamp":
        stats.hour_shares = dict(sorted(_shares(kept.get((c.idx, "h"), []), nn)[0]))
        stats.always_midnight = agg[p + "nonmidnight"] == 0
        stats.timezone = None if ntz else session_tz
    return stats


def _column_profile(c: _Col, agg: dict, n: int, kept, buckets, cfg: SynthgenConfig, session_tz: str) -> ColumnProfile:
    p, k = f"c{c.idx}_", cfg.privacy.k
    nn = agg[p + "nn"]
    nd = min(agg.get(p + "nd") or 0, nn)
    suppressed: list[str] = []
    prof = ColumnProfile(
        name=c.name,
        position=c.idx,
        type=c.meta.type,
        nullable=c.meta.nullable,
        comment=c.meta.comment,
        tags=c.meta.tags,
        is_partition=c.meta.is_partition,
        role=c.role,
        sensitive=c.sensitive,
        sensitive_reasons=c.reasons,
        null_rate=share_or_suppress("null_rate", n - nn, n, k, suppressed),
        distinct_approx=nd,
        distinct_ratio=rnd(nd / nn) if nn else None,
    )
    if c.kind == "numeric":
        prof.numeric = _numeric_stats(c, agg, nn, cfg, suppressed)
    elif c.kind == "date":
        prof.date = _date_stats(c, agg, nn, kept, cfg, session_tz)
    elif c.kind == "boolean":
        prof.boolean = BooleanStats(
            true_share=share_or_suppress("boolean.true_share", agg[p + "t"], n, k, suppressed),
            false_share=share_or_suppress("boolean.false_share", agg[p + "f"], n, k, suppressed),
        )
    elif c.kind == "categorical":
        pairs = kept.get((c.idx, "v"), [])
        values, other = _shares(pairs, nn)
        prof.categorical = CategoricalStats(
            top_values=[ValueShare(value=v, share=s) for v, s in values],
            other_share=other,
            labels_not_exported=max(0, buckets.get((c.idx, "v"), 0) - len(pairs)),
            length=_length(agg, p),
        )
    elif c.kind == "text":
        patterns, other = _shares(kept.get((c.idx, "p"), []), nn)
        shares = {sem: agg[f"{p}rx_{sem}"] / nn for sem in detect.VALUE_REGEXES} if c.family == "string" and nn else {}
        value_hint = next((r.split(":")[1] for r in c.reasons if r.startswith("value_pattern:")), None)
        length = _length(agg, p)
        prof.text = TextStats(
            length=length,
            patterns=[PatternShare(pattern=v, share=s) for v, s in patterns],
            other_pattern_share=other if nn else 0.0,
            semantic_type=detect.guess_semantic_type(
                name_hint=c.static.semantic_hint or value_hint,
                regex_shares=shares,
                id_named=c.id_named,
                mean_length=length.mean,
                space_share=agg.get(p + "space", 0) / nn if nn else 0.0,
                cfg=cfg,
            ),
        )
    prof.suppressed = suppressed
    return prof


# --- export policy ------------------------------------------------------------------------------

_VALUE_FIELDS = ("numeric", "categorical", "date", "boolean")


def enforce_export_policy(profile: TableProfile, id_named: set[str]) -> None:
    """Last check before writing: columns that must not export values carry no value statistics."""
    for c in profile.columns:
        no_values = c.sensitive or c.name in id_named or c.role in ("key", "foreign_key", "text", "other")
        if no_values and any(getattr(c, f) is not None for f in _VALUE_FIELDS):
            raise ExportPolicyError(f"{profile.source_table}.{c.name}: value statistics on a no-values column")
        if c.role == "other" and c.text is not None:
            raise ExportPolicyError(f"{profile.source_table}.{c.name}: unsupported type exported text stats")


# --- table ------------------------------------------------------------------------------------


def profile_table(spark: SparkSession, meta: TableMeta, cfg: SynthgenConfig, generated_at: datetime) -> TableResult:
    name = meta.full_name
    log.info("profiling %s", name)
    full = spark.table(name)
    row_count = full.count()
    fraction = cfg.sample_fraction_for(name)
    df = full if fraction >= 1.0 else full.sample(fraction=fraction, seed=cfg.profiling.sample_seed)
    session_tz = spark.conf.get("spark.sql.session.timeZone")

    cols = [
        _Col(
            meta=m,
            idx=i,
            family=detect.type_family(m.type),
            id_named=detect.is_id_name(m.name, cfg),
            static=detect.static_sensitivity(name, m.name, m.tags, cfg),
        )
        for i, m in enumerate(meta.columns)
    ]
    date_cols = [(c.idx, c.name, c.family) for c in cols if c.family in ("date", "timestamp") and c.value_stats_allowed]

    exprs = [F.count(F.lit(1)).alias("_n")]
    for c in cols:
        exprs += _column_exprs(c, cfg)
    exprs += date_order_exprs(date_cols)
    agg = collect_single(df.agg(*exprs), what=f"{name} column statistics")
    n = agg["_n"]

    for c in cols:
        _decide(c, agg, cfg)
    _clipped_moments(df, cols, agg, cfg, name)
    kept, buckets = _frequencies(df, cols, cfg, name)

    warnings: list[str] = []
    if fraction < 1.0:
        warnings.append(f"column statistics computed on a {fraction:g} sample; row_count is exact")
    for c in cols:
        if c.family == "other":
            warnings.append(f"column {c.name} has unsupported type {c.meta.type}: only type and null rate exported")

    rules = date_order_rules(agg, date_cols, cfg)
    numeric_cols = [c.name for c in cols if c.kind == "numeric"]
    rules += numeric_identity_rules(full, row_count, numeric_cols, cfg, table=name)

    profile = TableProfile(
        generated_at=generated_at,
        profiler_version=__version__,
        source_table=name,
        row_count=row_count,
        sample_fraction=fraction,
        k_threshold=cfg.privacy.k,
        comment=meta.comment,
        tags=meta.tags,
        primary_key=meta.primary_key,
        foreign_keys=meta.foreign_keys,
        partition_columns=meta.partition_columns,
        columns=[_column_profile(c, agg, n, kept, buckets, cfg, session_tz) for c in cols],
        rules=rules,
        warnings=warnings,
    )

    # Key candidates: exact uniqueness is checked on the full table, prefiltered by approx distinct.
    def key_ok(c: _Col) -> bool:
        nn = agg[f"c{c.idx}_nn"]
        return not c.sensitive and c.family in ("integer", "string") and n > 0 and nn == n

    singles = [
        c.name for c in cols
        if key_ok(c) and (agg.get(f"c{c.idx}_nd") or 0) >= cfg.relationships.pk_prefilter_ratio * n
    ]
    pair_pool = [c.name for c in cols if key_ok(c) and c.id_named]
    unique_singles, unique_pairs = find_unique_keys(full, row_count, singles, pair_pool, cfg, table=name)

    keys = KeyInfo(
        table=name,
        df=full,
        row_count=row_count,
        columns={c.name: KeyColumn(c.name, c.family, c.sensitive, c.id_named) for c in cols},
        declared_pk=meta.primary_key,
        declared_fks=list(meta.foreign_keys),
        unique_singles=unique_singles,
        unique_pairs=unique_pairs,
    )
    return TableResult(profile=profile, keys=keys, cols={c.name: c for c in cols})


def _apply_key_roles(results: dict[str, TableResult], rel: Relationships) -> None:
    """Key and FK columns are identifiers: set their role and drop any value statistics."""

    def mark(table: str, column: str, role: str) -> None:
        res = results.get(table)
        if not res:
            return
        for c in res.profile.columns:
            if c.name != column:
                continue
            if any(getattr(c, f) is not None for f in _VALUE_FIELDS):
                for f in _VALUE_FIELDS:
                    setattr(c, f, None)
                c.suppressed = [s for s in c.suppressed if s == "null_rate"]
                res.profile.warnings.append(f"value statistics of key column {column} not exported")
            c.role = role

    for pk in rel.primary_keys:
        for column in pk.columns:
            mark(pk.table, column, "key")
    for fk in rel.foreign_keys:
        for column in fk.child_columns:
            mark(fk.child_table, column, "foreign_key")


def run_pass1(
    spark: SparkSession,
    cfg: SynthgenConfig,
    *,
    metadata: SparkCatalogMetadata | None = None,
    write: bool = True,
) -> Pass1Result:
    provider = metadata or make_metadata_provider(spark, cfg)
    tables = provider.resolve_tables(cfg.source)
    if not tables:
        raise ValueError("no source tables matched the configuration")
    generated_at = datetime.now(timezone.utc).replace(microsecond=0)

    results = {t: profile_table(spark, provider.table_meta(t), cfg, generated_at) for t in tables}
    rel = infer_relationships({t: r.keys for t, r in results.items()}, cfg, generated_at)
    _apply_key_roles(results, rel)

    profiles = [results[t].profile for t in tables]
    for t in tables:
        enforce_export_policy(results[t].profile, {c.name for c in results[t].cols.values() if c.id_named})

    written: list[Path] = []
    if write:
        store = ProfileStore.review(cfg.output.root)
        written = [store.write_table_profile(p) for p in profiles] + [store.write_relationships(rel)]
    return Pass1Result(profiles=profiles, relationships=rel, written=written)
