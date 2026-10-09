"""Hard constraints from PLAN.md, checked on the real pass-1 output of the retail fixture."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from synthgen.profiler._safe import RowLevelOutputError, collect_aggregate
from synthgen.profiler.pass1 import ExportPolicyError, enforce_export_policy
from tests.conftest import column_of, profile_of
from tests.fixtures.retail import PLANTED_QUANTITY, PLANTED_SUBTOTAL
from tests.leak_check import find_leaks

PACKAGE = Path(__file__).resolve().parents[1] / "synthgen"


def _output_files(run) -> list[Path]:
    return sorted(p for p in run.output_root.rglob("*") if p.is_file())


# --- rule 1, 3, 4: no row-level data, no values from sensitive or high-cardinality columns -----


def test_no_raw_sensitive_values_anywhere_in_output(pass1_run, retail):
    files = _output_files(pass1_run)
    assert len(files) == 5
    total_values = sum(len(v) for v in retail.sensitive_values.values())
    assert total_values > 20000, "fixture should plant a meaningful number of sensitive values"
    leaks = find_leaks(files, retail.sensitive_values)
    assert not leaks, f"{len(leaks)} raw values leaked, e.g. {leaks[:5]}"


def test_leak_check_catches_a_planted_leak(tmp_path, retail):
    """Negative control: the leak test must fail when a raw value is present."""
    email = retail.sensitive_values["customers.email"][0]
    postcode = retail.sensitive_values["customers.postcode"][0]
    bad = tmp_path / "bad.profile.json"
    bad.write_text(json.dumps({"columns": [{"top_values": [{"value": email.upper()}, {"value": postcode}]}]}))
    leaks = find_leaks([bad], retail.sensitive_values)
    assert any("customers.email" in leak for leak in leaks)
    assert any("customers.postcode" in leak for leak in leaks)


def test_rare_labels_below_k_are_not_exported(pass1_run, retail):
    leaks = find_leaks(_output_files(pass1_run), retail.rare_labels)
    assert not leaks
    country = column_of(pass1_run, "customers", "country").categorical
    assert country.other_share > 0 and country.labels_not_exported == 1


def test_columns_without_value_export_carry_no_value_stats(pass1_run):
    checked = 0
    for profile in pass1_run.result.profiles:
        for c in profile.columns:
            if c.sensitive or c.role in ("key", "foreign_key", "text", "other"):
                checked += 1
                assert (c.numeric, c.categorical, c.date, c.boolean) == (None, None, None, None), c.name
    assert checked >= 20


def test_every_exported_bucket_meets_k(pass1_run):
    """Each exported label, pattern or time bucket stands for at least k rows."""
    k = pass1_run.config.privacy.k
    for profile in pass1_run.result.profiles:
        n = profile.row_count * profile.sample_fraction
        for c in profile.columns:
            nonnull = n * (1 - (c.null_rate or 0))
            shares = []
            if c.categorical:
                shares += [v.share for v in c.categorical.top_values]
            if c.text:
                shares += [p.share for p in c.text.patterns]
            if c.date:
                shares += list(c.date.year_month_shares.values()) + list(c.date.day_of_week_shares.values())
                shares += list((c.date.hour_shares or {}).values())
            for share in shares:
                assert share * nonnull >= k - 0.5, (profile.source_table, c.name, share)


def test_small_counts_are_suppressed_not_rounded(pass1_run):
    # One planted quantity breaks line_amount = quantity * unit_price on fewer than k rows.
    rule = next(r for r in profile_of(pass1_run, "order_lines").rules if r.kind == "numeric_identity")
    assert rule.match_rate is None and rule.suppressed == ["match_rate"]


# --- rule 5: extremes are clipped ----------------------------------------------------------


def test_min_max_are_clipped_quantiles(pass1_run):
    lo, hi = pass1_run.config.privacy.clip_lower, pass1_run.config.privacy.clip_upper
    for profile in pass1_run.result.profiles:
        for c in profile.columns:
            if c.numeric and c.numeric.quantiles:
                assert c.numeric.min_clipped == c.numeric.quantiles[f"{lo:g}"]
                assert c.numeric.max_clipped == c.numeric.quantiles[f"{hi:g}"]
            if c.date and c.date.quantiles:
                assert c.date.min_clipped == c.date.quantiles[f"{lo:g}"]
                assert c.date.max_clipped == c.date.quantiles[f"{hi:g}"]


def test_mean_and_std_ignore_extremes(pass1_run):
    """Mean and std use values capped at the clip quantiles: they must fit inside those bounds."""
    checked = 0
    for profile in pass1_run.result.profiles:
        for c in profile.columns:
            n = c.numeric
            if n and n.mean is not None:
                checked += 1
                assert n.min_clipped <= n.mean <= n.max_clipped, c.name
                assert n.std <= (n.max_clipped - n.min_clipped) / 2 + 1e-9, c.name  # max std of a bounded variable
    assert checked >= 8
    # Raw std of orders.total is ~1087 because of the planted 98765.43 order; capped it is far smaller.
    assert column_of(pass1_run, "orders", "total").numeric.std < 600


def test_planted_extremes_do_not_appear(pass1_run):
    subtotal = column_of(pass1_run, "orders", "subtotal").numeric
    assert subtotal.max_clipped < 10000
    assert float(PLANTED_SUBTOTAL) not in subtotal.quantiles.values()
    quantity = column_of(pass1_run, "order_lines", "quantity").numeric
    assert quantity.max_clipped < PLANTED_QUANTITY
    text = "".join(p.read_text() for p in _output_files(pass1_run))
    assert str(PLANTED_SUBTOTAL) not in text and "98765" not in text


# --- rule 6 and 7: output confined to review/, source untouched -----------------------------


def test_output_only_in_review_folder(pass1_run):
    for path in _output_files(pass1_run):
        assert path.parent == pass1_run.output_root / "review", path


def test_source_tables_are_read_only(pass1_run):
    assert pass1_run.warehouse_after == pass1_run.warehouse_before
    assert pass1_run.catalog_after == pass1_run.catalog_before


# --- defence in depth in code ----------------------------------------------------------------

FORBIDDEN_CALLS = {
    "collect", "show", "take", "head", "first", "tail", "toPandas", "toLocalIterator", "foreach",
    "foreachPartition", "saveAsTable", "insertInto", "createOrReplaceTempView", "createTempView",
    "createGlobalTempView", "createOrReplaceGlobalTempView", "cache", "persist", "checkpoint",
    "localCheckpoint", "toJSON",
}
FORBIDDEN_ATTRIBUTES = {"write", "writeTo", "writeStream", "rdd", "sparkContext"}
FORBIDDEN_NAMES = {"display", "print", "displayHTML"}
FORBIDDEN_SQL = re.compile(r"^\s*(CREATE|INSERT|ALTER|DROP|MERGE|UPDATE|DELETE|TRUNCATE|REPLACE|COPY|OPTIMIZE|VACUUM)\b", re.I)


def _profiler_sources() -> list[Path]:
    return [p for p in sorted((PACKAGE / "profiler").glob("*.py")) if p.name != "_safe.py"]


def test_profiler_code_has_no_row_level_or_write_calls():
    violations = []
    for path in _profiler_sources():
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            where = f"{path.name}:{getattr(node, 'lineno', '?')}"
            if isinstance(node, ast.Call):
                fn = node.func
                if isinstance(fn, ast.Attribute) and fn.attr in FORBIDDEN_CALLS:
                    violations.append(f"{where} .{fn.attr}()")
                if isinstance(fn, ast.Name) and fn.id in FORBIDDEN_NAMES:
                    violations.append(f"{where} {fn.id}()")
            if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ATTRIBUTES:
                violations.append(f"{where} .{node.attr}")
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and FORBIDDEN_SQL.match(node.value):
                violations.append(f"{where} SQL {node.value[:30]!r}")
    assert not violations, violations
    assert len(_profiler_sources()) >= 6


def test_safe_collect_rejects_row_level_dataframes(spark, retail):
    rows = spark.table(retail.table("customers")).select("email")
    with pytest.raises(RowLevelOutputError):
        collect_aggregate(rows, max_rows=100, what="test")


def test_export_policy_blocks_values_on_sensitive_columns(pass1_run):
    from synthgen.profile_schema import CategoricalStats, ValueShare

    profile = profile_of(pass1_run, "customers").model_copy(deep=True)
    email = next(c for c in profile.columns if c.name == "email")
    email.categorical = CategoricalStats(top_values=[ValueShare(value="x", share=0.5)], other_share=0.5,
                                         labels_not_exported=0)
    with pytest.raises(ExportPolicyError):
        enforce_export_policy(profile, set())
