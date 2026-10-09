"""Generator: plan, determinism, written schema, joins, and how closely the fake data matches the profile.

The quality checks re-profile the synthetic tables with pass 1 and compare against the original profile,
which is the milestone-3 acceptance: same schema and comments, foreign keys join, marginals match.
"""

from __future__ import annotations

import random
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pytest

from synthgen.config import SynthgenConfig
from synthgen.generator import columns as C
from synthgen.generator.check import check_tables
from synthgen.generator.generate import generate
from synthgen.generator.plan import GeneratorSettings, build_plan, describe_plan, load_profiles
from synthgen.generator.write import TargetTableError, write_tables
from synthgen.profile_schema import Relationships, TableProfile
from synthgen.profiler.metadata import SparkCatalogMetadata
from synthgen.profiler.pass1 import run_pass1

SETTINGS = GeneratorSettings(scale=0.5, min_rows=50, seed=7)


@dataclass
class GenRun:
    profiles: list
    relationships: Relationships
    plans: list
    data: dict
    original: dict[str, TableProfile]
    synthetic: dict[str, TableProfile]
    synthetic_rel: Relationships
    check: list[str]


def _short(table: str) -> str:
    return table.split(".")[-1]


@pytest.fixture(scope="session")
def gen_run(spark, pass1_run) -> GenRun:
    profiles, rel = load_profiles(pass1_run.output_root)
    plans = build_plan(profiles, rel, SETTINGS)
    data = generate(plans, SETTINGS)
    write_tables(spark, plans, data, "spark_catalog", "synth", SETTINGS, table_format="parquet", declare_keys=False)
    cfg = SynthgenConfig.model_validate({
        "dataset": "synthetic", "source": {"table_patterns": ["spark_catalog.synth.*"]},
        "output": {"root": "/unused"}, "metadata_provider": "spark",
    })
    result = run_pass1(spark, cfg, metadata=SparkCatalogMetadata(spark), write=False)
    return GenRun(
        profiles=profiles,
        relationships=rel,
        plans=plans,
        data=data,
        original={_short(p.source_table): p for p in profiles},
        synthetic={_short(p.source_table): p for p in result.profiles},
        synthetic_rel=result.relationships,
        check=check_tables(spark, plans, "spark_catalog", "synth"),
    )


# --- plan ---------------------------------------------------------------------------------------


def test_plan_orders_parents_first_and_picks_driving_keys(gen_run):
    order = [p.name for p in gen_run.plans]
    assert order.index("customers") < order.index("orders") < order.index("order_lines")
    driving = {p.name: (p.driving_fk.child_columns[0] if p.driving_fk else None) for p in gen_run.plans}
    assert driving == {"customers": None, "products": None, "orders": "customer_id", "order_lines": "order_id"}
    text = describe_plan(gen_run.plans, SETTINGS)
    assert "order_lines: rows from orders.order_id" in text and "~1000 rows (source 2000)" in text


def test_load_profiles_from_zip_and_folder(pass1_run, tmp_path):
    archive = shutil.make_archive(str(tmp_path / "p"), "zip", pass1_run.output_root / "review")
    from_zip, rel_zip = load_profiles(archive)
    from_dir, rel_dir = load_profiles(pass1_run.output_root)
    assert [p.source_table for p in from_zip] == [p.source_table for p in from_dir]
    assert rel_zip == rel_dir
    with pytest.raises(FileNotFoundError):
        load_profiles(tmp_path)


def test_generation_is_deterministic(gen_run):
    again = generate(build_plan(gen_run.profiles, gen_run.relationships, SETTINGS), SETTINGS)
    assert again == gen_run.data
    other_seed = GeneratorSettings(scale=SETTINGS.scale, min_rows=SETTINGS.min_rows, seed=8)
    assert generate(build_plan(gen_run.profiles, gen_run.relationships, other_seed), other_seed) != gen_run.data


# --- written tables -------------------------------------------------------------------------------


def test_schema_comments_and_properties(spark, gen_run):
    for name, original in gen_run.original.items():
        table = f"spark_catalog.synth.{name}"
        written = {c.name: (c.dataType, c.description) for c in spark.catalog.listColumns(table)}
        assert list(written) == [c.name for c in original.columns]
        assert written == {c.name: (c.type, c.comment) for c in original.columns}
        assert spark.catalog.getTable(table).description == original.comment
        props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        assert props["synthgen.source_table"] == original.source_table and props["synthgen.seed"] == "7"


def test_keys_are_unique_and_links_join(gen_run):
    pks = {_short(pk.table): pk.columns for pk in gen_run.synthetic_rel.primary_keys}
    assert pks == {"customers": ["customer_id"], "products": ["product_id"], "orders": ["order_id"],
                   "order_lines": ["order_id", "line_no"]}
    fks = {(_short(f.child_table), f.child_columns[0]): f for f in gen_run.synthetic_rel.foreign_keys}
    assert {k: _short(f.parent_table) for k, f in fks.items()} == {
        ("customers", "referred_by_customer_id"): "customers", ("orders", "customer_id"): "customers",
        ("order_lines", "order_id"): "orders", ("order_lines", "product_id"): "products",
    }
    assert fks[("orders", "customer_id")].orphan_rate == 0.0
    assert fks[("order_lines", "order_id")].orphan_rate == 0.0
    assert 0.005 < fks[("order_lines", "product_id")].orphan_rate < 0.03  # the source has ~1.5% orphans
    assert any(line.startswith("orders.customer_id -> customers.customer_id: 100.0%") for line in gen_run.check)


def test_children_per_parent_follow_the_source(gen_run, pass1_run):
    def cpp(rel, child):
        fk = next(f for f in rel.foreign_keys if f.child_table.endswith(child[0]) and f.child_columns == [child[1]])
        return fk.children_per_parent
    for child in [(".orders", "customer_id"), (".order_lines", "order_id")]:
        real, fake = cpp(pass1_run.result.relationships, child), cpp(gen_run.synthetic_rel, child)
        assert abs(real.quantiles["0.5"] - fake.quantiles["0.5"]) <= 1
        assert abs(real.quantiles["0.95"] - fake.quantiles["0.95"]) <= 0.25 * real.quantiles["0.95"] + 1
        assert abs(real.zero_children_share - fake.zero_children_share) <= 0.05


def test_refuses_to_replace_tables_it_did_not_create(spark, gen_run):
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.guarded")
    spark.range(3).write.mode("overwrite").saveAsTable("spark_catalog.guarded.orders")
    with pytest.raises(TargetTableError, match="not created by synthgen"):
        write_tables(spark, gen_run.plans, gen_run.data, "spark_catalog", "guarded", SETTINGS,
                     table_format="parquet", declare_keys=False)
    assert spark.table("spark_catalog.guarded.orders").count() == 3


# --- distributions: synthetic profile vs original profile ----------------------------------------


def _tvd(a: dict[str, float], b: dict[str, float]) -> float:
    keys = set(a) | set(b)
    return 0.5 * sum(abs(a.get(k, 0.0) - b.get(k, 0.0)) for k in keys)


def _labels(cat) -> dict[str, float]:
    return {v.value: v.share for v in cat.top_values}


def test_marginal_distributions_match(gen_run):
    checked = {"null": 0, "categorical": 0, "numeric": 0, "boolean": 0, "date": 0}
    for name, original in gen_run.original.items():
        fake_cols = {c.name: c for c in gen_run.synthetic[name].columns}
        for real in original.columns:
            fake = fake_cols[real.name]
            where = f"{name}.{real.name}"
            if real.null_rate is not None and fake.null_rate is not None:
                assert abs(real.null_rate - fake.null_rate) <= 0.03, where
                checked["null"] += 1
            if real.categorical:
                # Straight from the generated values: re-profiling small fake tables would hide labels below k.
                values = [v for v in gen_run.data[original.source_table][real.name] if v is not None]
                expected = _labels(real.categorical) | {"<other>": real.categorical.other_share}
                observed = {k: values.count(k) / len(values) for k in _labels(real.categorical)}
                observed["<other>"] = 1 - sum(observed.values())
                assert _tvd(expected, observed) <= 0.1, where
                checked["categorical"] += 1
            if real.numeric and fake.numeric and real.numeric.quantiles:
                for q in ("0.25", "0.5", "0.75"):
                    r, f = real.numeric.quantiles[q], fake.numeric.quantiles[q]
                    assert abs(r - f) <= 0.15 * abs(r) + 1.0, (where, q, r, f)
                checked["numeric"] += 1
            if real.boolean and fake.boolean:
                assert abs((real.boolean.true_share or 0) - (fake.boolean.true_share or 0)) <= 0.05, where
                checked["boolean"] += 1
            if real.date and fake.date:
                rd, fd = real.date.day_of_week_shares, fake.date.day_of_week_shares
                n = len(gen_run.data[original.source_table][real.name])
                tolerance = max(0.05, 1.5 / n ** 0.5)  # ~3 standard errors for small fake tables
                assert max(abs(rd.get(d, 0) - fd.get(d, 0)) for d in rd) <= tolerance, where
                checked["date"] += 1
    assert checked == {"null": checked["null"], "categorical": 10, "numeric": 9, "boolean": 2, "date": 4}
    assert checked["null"] >= 30


def test_seasonality_and_hours_survive(gen_run):
    real = gen_run.original["orders"].columns
    fake = {c.name: c for c in gen_run.synthetic["orders"].columns}
    r = next(c for c in real if c.name == "order_ts").date
    f = fake["order_ts"].date
    assert f.year_month_shares["2023-12"] > f.year_month_shares["2023-03"]
    assert _tvd(r.hour_shares, f.hour_shares) <= 0.1
    assert f.timezone == "UTC" and f.always_midnight is False


def test_rules_hold_in_synthetic_data(gen_run):
    rules = {name: {(r.kind, getattr(r, "target", None) or r.before) for r in p.rules}
             for name, p in gen_run.synthetic.items()}
    assert ("date_order", "order_ts") in rules["orders"]
    assert ("numeric_identity", "total") in rules["orders"]
    assert ("numeric_identity", "line_amount") in rules["order_lines"]
    gaps = [(s - o.date()).days for o, s in zip(gen_run.data["spark_catalog.retail.orders"]["order_ts"],
                                                gen_run.data["spark_catalog.retail.orders"]["ship_date"])
            if o and s]
    assert sorted(gaps)[len(gaps) // 2] <= 7  # ships within days, not months


def test_fake_text_looks_like_the_source(gen_run):
    customers = gen_run.data["spark_catalog.retail.customers"]
    assert all("@example" in e or e.endswith("@mail.example") for e in customers["email"] if e)
    first, last, email = customers["first_name"][0], customers["last_name"][0], customers["email"][0]
    assert email.startswith(f"{first}.{last}".lower().replace(" ", ""))  # email matches the row's name
    assert all(len(p) == len("+99 9 99 99 99 99") for p in customers["phone"] if p)
    products = gen_run.data["spark_catalog.retail.products"]["product_id"]
    assert products[:2] == ["P-00001", "P-00002"]
    sem = {c.name: c.text.semantic_type for c in gen_run.synthetic["customers"].columns if c.text}
    assert sem["email"] == "email" and sem["phone"] == "phone" and sem["first_name"] == "person_name"
    fees = set(gen_run.data["spark_catalog.retail.orders"]["shipping_fee"])
    assert fees <= {0.0, 4.99, 9.99}  # low-cardinality numbers keep their observed values


# --- samplers ---------------------------------------------------------------------------------------


def test_inverse_cdf_and_step():
    points = [(0.01, 0.0), (0.5, 10.0), (0.99, 20.0)]
    assert C.inverse_cdf(points, 0.0) == 0.0 and C.inverse_cdf(points, 1.0) == 20.0
    assert C.inverse_cdf(points, 0.255) == pytest.approx(5.0)
    assert C.step_inverse_cdf(points, 0.3) == 10.0


def test_patterns():
    rng = random.Random(1)
    assert C.counter_pattern("A-99999", 42, "products") == "P-00042"
    assert C.counter_pattern("AAA-9999", 7, "ab") == "ABA-0007"
    filled = C.fill_pattern("+99 9 AA", rng)
    assert filled[0] == "+" and filled[1:3].isdigit() and filled[-2:].isalpha()
    assert C.wordy_pattern("AAAAA AAAA AAA-999", random.Random(2))[-4:-3] == "-"


@pytest.mark.parametrize("value, spark_type, expected", [
    (12.345, "decimal(10,2)", Decimal("12.35")),
    (1e12, "decimal(5,2)", Decimal("999.99")),
    ("P-00042", "int", 42),
    (3.6, "bigint", 4),
    ("2024-01-05", "date", date(2024, 1, 5)),
    (date(2024, 1, 5), "timestamp", datetime(2024, 1, 5)),
    (date(2024, 1, 5), "string", "2024-01-05"),
    (["x"], "array<string>", None),
    (None, "int", None),
])
def test_to_spark_value(value, spark_type, expected):
    assert C.to_spark_value(value, spark_type) == expected
