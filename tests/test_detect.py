from __future__ import annotations

import re

import pytest

from synthgen.config import SynthgenConfig
from synthgen.profiler import detect


@pytest.fixture(scope="module")
def cfg() -> SynthgenConfig:
    return SynthgenConfig.model_validate(
        {"dataset": "t", "source": {"tables": ["c.s.t"]}, "output": {"root": "/tmp/x"},
         "privacy": {"sensitive_columns": ["c.s.t.house_blend"]}}
    )


@pytest.mark.parametrize(
    "column, expected",
    [
        ("first_name", "person_name"), ("customer_name", "person_name"), ("name", "person_name"),
        ("email_address", "email"), ("e_mail", "email"), ("mobile_phone", "phone"), ("ip_address", "ip_address"),
        ("billing_address", "address"), ("postcode", "postcode"), ("zip_code", "postcode"),
        ("iban", "iban"), ("card_number", "iban"), ("nif", "national_id"), ("customer_dni", "national_id"),
        ("ssn", "national_id"), ("tax_id", "national_id"), ("birth_date", "birth_date"), ("dob", "birth_date"),
        ("latitude", "geo"), ("api_key", "secret"), ("health_status", "special_category"),
    ],
)
def test_pii_names(cfg, column, expected):
    assert detect.pii_name_matches(column, cfg)[0] == expected


@pytest.mark.parametrize(
    "column",
    ["product_name", "shipping_fee", "description", "tip_amount", "zipper_color", "brand", "status", "domain",
     "membership", "ship_date", "pipeline_stage", "skip_flag", "line_no"],
)
def test_not_pii_names(cfg, column):
    assert detect.pii_name_matches(column, cfg) == []


def test_sensitivity_sources(cfg):
    assert detect.static_sensitivity("c.s.t", "house_blend", {}, cfg).reasons == ["config"]
    tagged = detect.static_sensitivity("c.s.t", "ref", {"class.email_address": ""}, cfg)
    assert tagged.sensitive and tagged.reasons == ["uc_tag:class.email_address"]
    assert detect.static_sensitivity("c.s.t", "ref", {"pii": "true"}, cfg).reasons == ["uc_tag:pii=true"]
    assert not detect.static_sensitivity("c.s.t", "ref", {"owner": "sales"}, cfg).sensitive


@pytest.mark.parametrize("column, expected", [
    ("id", True), ("customer_id", True), ("order_key", True), ("line_no", True), ("sku", True),
    ("idle_time", False), ("discount_code", False), ("paid", False), ("valid", False),
])
def test_id_names(cfg, column, expected):
    assert detect.is_id_name(column, cfg) is expected


@pytest.mark.parametrize("spark_type, family", [
    ("int", "integer"), ("bigint", "integer"), ("decimal(10,0)", "integer"), ("decimal(12,2)", "decimal"),
    ("double", "float"), ("string", "string"), ("varchar(20)", "string"), ("date", "date"),
    ("timestamp", "timestamp"), ("timestamp_ntz", "timestamp"), ("boolean", "boolean"),
    ("array<string>", "other"), ("map<string,int>", "other"), ("binary", "other"),
])
def test_type_family(spark_type, family):
    assert detect.type_family(spark_type) == family


@pytest.mark.parametrize("child, parent_table, parent_col, minimum", [
    ("customer_id", "cat.s.customers", "customer_id", 1.0),
    ("customer_id", "cat.s.customers", "id", 0.95),
    ("referred_by_customer_id", "cat.s.customers", "customer_id", 0.85),
    ("category_id", "cat.s.categories", "id", 0.95),
    ("cust_id", "cat.s.customers", "customer_id", 0.0),
])
def test_fk_name_score_matches(child, parent_table, parent_col, minimum):
    assert detect.fk_name_score(child, parent_table, parent_col) >= minimum


@pytest.mark.parametrize("child, parent_table, parent_col", [
    ("product_id", "cat.s.customers", "customer_id"),
    ("line_no", "cat.s.orders", "order_id"),
    ("id", "cat.s.orders", "id"),
])
def test_fk_name_score_rejects(child, parent_table, parent_col):
    assert detect.fk_name_score(child, parent_table, parent_col) < 0.8


@pytest.mark.parametrize("value, sem, matches", [
    ("jane.doe@example.com", "email", True), ("not an email", "email", False),
    ("+33 6 12 34 56 78", "phone", True), ("(555) 123-4567", "phone", True), ("0612345678", "phone", True),
    ("192.168.1.10", "phone", False), ("2024-01-05", "phone", False), ("P-00001", "phone", False),
    ("12345", "phone", False), ("192.168.1.10", "ip_address", True),
    ("FR7630006000011234567890189", "iban", True),
    ("123e4567-e89b-12d3-a456-426614174000", "uuid", True),
])
def test_value_regexes(value, sem, matches):
    # Same syntax in Python and Java for these expressions.
    assert bool(re.search(detect.VALUE_REGEXES[sem], value)) is matches


def test_value_pii_skips_phone_for_identifiers(cfg):
    shares = {"phone": 0.9, "email": 0.0}
    assert detect.value_pii_reasons(shares, id_named=False, cfg=cfg) == ["value_pattern:phone"]
    assert detect.value_pii_reasons(shares, id_named=True, cfg=cfg) == []
