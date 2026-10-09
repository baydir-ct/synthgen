"""Pass 1 on the retail fixture: structure, roles, statistics, rules and relationships."""

from __future__ import annotations

import pytest

from synthgen.io import ProfileStore
from synthgen.profiler.metadata import SparkCatalogMetadata
from synthgen.profiler.pass1 import run_pass1
from tests.conftest import column_of, load_config, profile_of

T = "spark_catalog.retail."


def test_writes_one_valid_profile_per_table_plus_relationships(pass1_run, retail):
    store = ProfileStore.review(pass1_run.output_root)
    names = sorted(p.name for p in store.folder.iterdir())
    assert names == [
        "relationships.json",
        f"{T}customers.profile.json",
        f"{T}order_lines.profile.json",
        f"{T}orders.profile.json",
        f"{T}products.profile.json",
    ]
    for profile in store.table_profiles():  # validates against the schema on read
        table = profile.source_table.split(".")[-1]
        assert profile.row_count == retail.row_counts[table]
        assert profile.schema_version == "1.0" and profile.k_threshold == 10 and profile.sample_fraction == 1.0
    assert store.read_relationships().tables == sorted(T + t for t in retail.row_counts)


def test_table_metadata(pass1_run):
    customers, orders = profile_of(pass1_run, "customers"), profile_of(pass1_run, "orders")
    assert customers.comment == "One row per registered customer"
    assert customers.tags == {"domain": "crm"}
    assert customers.primary_key == ["customer_id"]
    assert orders.partition_columns == ["channel"]
    assert [fk.columns for fk in orders.foreign_keys] == [["customer_id"]]
    assert orders.foreign_keys[0].parent_table == T + "customers"
    email = column_of(pass1_run, "customers", "email")
    assert email.comment == "Customer email address" and email.type == "string"
    assert column_of(pass1_run, "orders", "channel").tags == {"business": "kpi"}
    assert [c.position for c in customers.columns] == list(range(len(customers.columns)))


EXPECTED_ROLES = {
    "customers": {
        "customer_id": "key", "first_name": "text", "email": "text", "birth_date": "date", "city": "categorical",
        "country": "categorical", "signup_date": "date", "is_newsletter": "boolean",
        "referred_by_customer_id": "foreign_key",
    },
    "products": {"product_id": "key", "product_name": "text", "category": "categorical", "brand": "categorical",
                 "unit_price": "numeric", "launch_date": "date", "is_active": "boolean"},
    "orders": {"order_id": "key", "customer_id": "foreign_key", "order_ts": "date", "status": "categorical",
               "subtotal": "numeric", "shipping_address": "text", "channel": "categorical"},
    "order_lines": {"order_id": "foreign_key", "line_no": "key", "product_id": "foreign_key",
                    "quantity": "numeric", "gift_message": "text"},
}


@pytest.mark.parametrize("table", EXPECTED_ROLES)
def test_roles(pass1_run, table):
    roles = {c.name: c.role for c in profile_of(pass1_run, table).columns}
    assert {k: roles[k] for k in EXPECTED_ROLES[table]} == EXPECTED_ROLES[table]


def test_sensitivity_sources(pass1_run):
    def reasons(table, column):
        c = column_of(pass1_run, table, column)
        assert c.sensitive
        return c.sensitive_reasons

    assert reasons("customers", "tax_ref") == ["uc_tag:pii=true"]
    assert reasons("customers", "contact_handle") == ["config"]
    assert "name:email" in reasons("customers", "email") and "value_pattern:email" in reasons("customers", "email")
    assert "value_pattern:phone" in reasons("customers", "phone")
    assert "name:ip_address" in reasons("orders", "ip_address")
    assert "value_pattern:phone" not in reasons("orders", "ip_address")
    for table, column in [("products", "product_name"), ("order_lines", "gift_message"), ("customers", "city")]:
        assert not column_of(pass1_run, table, column).sensitive


def test_semantic_types(pass1_run):
    sem = {
        (t, c): column_of(pass1_run, t, c).text.semantic_type
        for t, c in [("customers", "email"), ("customers", "phone"), ("customers", "first_name"),
                     ("customers", "postcode"), ("customers", "birth_date"), ("orders", "ip_address"),
                     ("order_lines", "gift_message"), ("products", "product_id")]
    }
    assert sem == {
        ("customers", "email"): "email", ("customers", "phone"): "phone",
        ("customers", "first_name"): "person_name", ("customers", "postcode"): "postcode",
        ("customers", "birth_date"): "birth_date", ("orders", "ip_address"): "ip_address",
        ("order_lines", "gift_message"): "free_text", ("products", "product_id"): "identifier",
    }


def test_text_patterns(pass1_run):
    phone = column_of(pass1_run, "customers", "phone").text
    assert [(p.pattern, p.share) for p in phone.patterns] == [("+99 9 99 99 99 99", 1.0)]
    assert column_of(pass1_run, "products", "product_id").text.patterns[0].pattern == "A-99999"
    assert phone.length.min_clipped == phone.length.max_clipped == 17


def test_categorical_stats(pass1_run):
    status = column_of(pass1_run, "orders", "status").categorical
    labels = [v.value for v in status.top_values]
    assert labels == ["delivered", "shipped", "returned", "cancelled"]
    assert 0.75 < status.top_values[0].share < 0.85
    assert sum(v.share for v in status.top_values) + status.other_share == pytest.approx(1.0, abs=1e-5)
    brand = column_of(pass1_run, "products", "brand").categorical
    assert brand.labels_not_exported >= 15  # ~6 products per brand: most brands fall below k
    discount = column_of(pass1_run, "orders", "discount_code")
    assert 0.8 < discount.null_rate < 0.9


def test_numeric_stats(pass1_run):
    total = column_of(pass1_run, "orders", "total").numeric
    assert total.decimals == 2 and not total.is_integer
    assert list(total.quantiles) == ["0.01", "0.05", "0.1", "0.25", "0.5", "0.75", "0.9", "0.95", "0.99"]
    assert total.min_clipped <= total.quantiles["0.5"] <= total.max_clipped
    fee = column_of(pass1_run, "orders", "shipping_fee").numeric
    assert 0.9 < fee.zero_share < 1.0 and fee.negative_share == 0.0
    qty = column_of(pass1_run, "order_lines", "quantity").numeric
    assert qty.is_integer and qty.decimals == 0 and qty.quantiles["0.5"] == 1.0
    discount = column_of(pass1_run, "order_lines", "discount_pct").numeric
    assert discount.is_integer  # decimal(5,2) column whose values are all whole


def test_date_and_timestamp_stats(pass1_run):
    ts = column_of(pass1_run, "orders", "order_ts").date
    assert ts.timezone == "UTC" and ts.always_midnight is False
    assert ts.min_clipped.startswith("2023-") and ts.max_clipped.startswith("2025-")
    assert list(ts.day_of_week_shares) == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    assert ts.day_of_week_shares["Sun"] > ts.day_of_week_shares["Wed"]  # planted weekend effect
    assert set(ts.hour_shares) <= {f"{h:02d}" for h in range(24)} and ts.hour_shares["19"] > ts.hour_shares["04"]
    months = ts.year_month_shares
    assert list(months) == sorted(months) and months["2023-12"] > months["2023-03"]  # planted seasonality
    launch = column_of(pass1_run, "products", "launch_date").date
    assert launch.year_month_shares == {} and launch.other_year_month_share == 1.0  # 120 rows over 7 years
    assert launch.hour_shares is None


def test_boolean_stats(pass1_run):
    newsletter = column_of(pass1_run, "customers", "is_newsletter").boolean
    assert newsletter.true_share + newsletter.false_share + column_of(
        pass1_run, "customers", "is_newsletter"
    ).null_rate == pytest.approx(1.0)


def test_rules(pass1_run):
    orders = {r.kind: r for r in profile_of(pass1_run, "orders").rules}
    date_rule = orders["date_order"]
    assert (date_rule.before, date_rule.after) == ("order_ts", "ship_date")
    assert 0.97 < date_rule.match_rate < 1.0
    identity = orders["numeric_identity"]
    assert (identity.target, identity.operation, identity.operands) == ("total", "sum", ["subtotal", "shipping_fee"])
    assert identity.match_rate == 1.0
    lines = [r for r in profile_of(pass1_run, "order_lines").rules if r.kind == "numeric_identity"]
    assert [(r.target, r.operation, r.operands) for r in lines] == [("line_amount", "product", ["quantity", "unit_price"])]
    assert profile_of(pass1_run, "customers").rules == []  # birth_date is sensitive: no rules on it


def test_primary_keys(pass1_run):
    pks = {pk.table.split(".")[-1]: (pk.columns, pk.source) for pk in pass1_run.result.relationships.primary_keys}
    assert pks == {
        "customers": (["customer_id"], "declared"),
        "products": (["product_id"], "inferred"),
        "orders": (["order_id"], "inferred"),
        "order_lines": (["order_id", "line_no"], "inferred"),
    }


def test_foreign_keys(pass1_run):
    fks = {
        (fk.child_table.split(".")[-1], fk.child_columns[0]): fk for fk in pass1_run.result.relationships.foreign_keys
    }
    assert {key: (fk.parent_table.split(".")[-1], fk.source) for key, fk in fks.items()} == {
        ("orders", "customer_id"): ("customers", "declared"),
        ("order_lines", "order_id"): ("orders", "inferred"),
        ("order_lines", "product_id"): ("products", "inferred"),
        ("customers", "referred_by_customer_id"): ("customers", "inferred"),
    }
    products = fks[("order_lines", "product_id")]
    assert 0.005 < products.orphan_rate < 0.03 and products.containment == pytest.approx(1 - products.orphan_rate)
    assert products.orphan_value_share > 0.3  # each orphan id is unique, so many distinct orphans
    orders = fks[("orders", "customer_id")]
    assert orders.orphan_rate == 0.0 and 0.05 < orders.children_per_parent.zero_children_share < 0.25
    cpp = fks[("order_lines", "order_id")].children_per_parent
    assert cpp.quantiles["0.5"] == 2.0 and cpp.zero_children_share == 0.0


# --- variants: other providers and settings, on the same fixture --------------------------------


def test_spark_only_provider_infers_the_declared_fk(spark, retail, tmp_path):
    cfg = load_config(tmp_path, metadata_provider="spark")
    result = run_pass1(spark, cfg, write=False)
    customers = next(p for p in result.profiles if p.source_table.endswith(".customers"))
    assert customers.tags == {} and customers.primary_key is None
    tax_ref = next(c for c in customers.columns if c.name == "tax_ref")
    assert not tax_ref.sensitive and tax_ref.role == "text" and tax_ref.categorical is None  # high-cardinality
    fk = next(f for f in result.relationships.foreign_keys if f.child_table.endswith(".orders"))
    assert (fk.child_columns, fk.parent_columns, fk.source) == (["customer_id"], ["customer_id"], "inferred")
    assert not list(tmp_path.iterdir())  # write=False wrote nothing


def test_sampling_is_recorded(spark, retail, tmp_path):
    raw = load_config(tmp_path).model_dump()
    raw["source"] = {"tables": [retail.table("orders")]}
    raw["profiling"]["sample_fractions"] = {retail.table("orders"): 0.5}
    from synthgen.config import SynthgenConfig

    result = run_pass1(spark, SynthgenConfig.model_validate(raw), metadata=SparkCatalogMetadata(spark), write=False)
    orders = result.profiles[0]
    assert orders.sample_fraction == 0.5 and orders.row_count == retail.row_counts["orders"]
    assert any("0.5 sample" in w for w in orders.warnings)


def test_forced_keys_and_containment_threshold(spark, retail, tmp_path):
    """A config-forced FK is kept even below the containment threshold; a name match alone is not enough."""
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.adhoc")
    spark.createDataFrame([(i, f"S{i}") for i in range(1, 61)], "store_id int, store_code string") \
        .write.mode("overwrite").saveAsTable("spark_catalog.adhoc.stores")
    # visits.store_id mostly points to stores that do not exist (containment ~0.2).
    spark.createDataFrame([(i, (i % 300) + 1) for i in range(3000)], "visit_id int, store_id int") \
        .write.mode("overwrite").saveAsTable("spark_catalog.adhoc.visits")

    raw = load_config(tmp_path).model_dump()
    raw["source"] = {"table_patterns": ["spark_catalog.adhoc.*"]}
    from synthgen.config import SynthgenConfig

    cfg = SynthgenConfig.model_validate(raw)
    rel = run_pass1(spark, cfg, metadata=SparkCatalogMetadata(spark), write=False).relationships
    assert rel.foreign_keys == []

    raw["relationships"]["forced_fks"] = [{"child": "spark_catalog.adhoc.visits.store_id",
                                           "parent": "spark_catalog.adhoc.stores.store_id"}]
    raw["relationships"]["forced_pks"] = {"spark_catalog.adhoc.stores": ["store_code"]}
    rel = run_pass1(spark, SynthgenConfig.model_validate(raw), metadata=SparkCatalogMetadata(spark), write=False).relationships
    (fk,) = rel.foreign_keys
    assert fk.source == "config" and fk.containment == pytest.approx(0.2, abs=0.01)
    stores_pk = next(pk for pk in rel.primary_keys if pk.table.endswith(".stores"))
    assert stores_pk.columns == ["store_code"] and stores_pk.source == "config"
    assert ["store_id"] in stores_pk.alternatives
