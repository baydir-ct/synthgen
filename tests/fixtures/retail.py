"""Small fake retail dataset with known statistics and planted privacy traps.

Deterministic for a given seed. Tables: customers, products, orders, order_lines.

Planted traps the tests check for:
- PII in customers (names, emails, phones, birth dates, postcodes, addresses), orders (address, IP);
- `customers.tax_ref`: sensitive only through a (fake) Unity Catalog tag;
- `customers.contact_handle`: sensitive only through config;
- rare labels below k: country 'Liechtenstein' x3, category 'Clearance' x2, status 'lost' x4,
  discount_code 'STAFF50' x3;
- extreme values: orders.subtotal 98765.43, order_lines.quantity 500;
- free text (order_lines.gift_message) and unique product names.
"""

from __future__ import annotations

import random
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "spark_catalog"
SCHEMA = "retail"

FIRST_NAMES = [
    "Mireille", "Thibault", "Anouk", "Bastien", "Clemence", "Dorian", "Eloise", "Fabrizio", "Gwendoline",
    "Hippolyte", "Ingrid", "Joachim", "Katalin", "Leopoldine", "Maximilien", "Nolwenn", "Octavie",
    "Perrine", "Quentin", "Rosalind", "Sigrid", "Tancrede", "Ulrike", "Valentin", "Wilhelmina", "Xaviere",
    "Yannick", "Zephyrine", "Amandine", "Barnabe",
]
LAST_NAMES = [
    "Fontaine", "Lefebvre", "Marchetti", "Kowalski", "Okonkwo", "Haddad", "Lindqvist", "Brunetti",
    "Vasquez", "Delacroix", "Nakamura", "Oyelaran", "Petrovic", "Quaresma", "Rasmussen", "Szabo",
    "Thorvaldsen", "Urquhart", "Villeneuve", "Wojcik", "Yilmaz", "Zielinski", "Abernathy", "Beaulieu",
]
STREETS = ["rue des Lilas", "avenue Foch", "boulevard Voltaire", "chemin des Vignes", "impasse du Moulin",
           "rue Pasteur", "allee des Tilleuls", "quai Saint-Antoine"]
CITIES = [("Lyon", 18), ("Paris", 22), ("Marseille", 10), ("Bordeaux", 8), ("Lille", 7), ("Nantes", 7),
          ("Toulouse", 7), ("Strasbourg", 5), ("Bruxelles", 6), ("Geneve", 4), ("Luxembourg", 3), ("Nice", 3)]
COUNTRIES = [("France", 75), ("Belgium", 10), ("Switzerland", 8), ("Luxembourg", 4), ("Germany", 3)]
SEGMENTS = [("consumer", 70), ("small_business", 22), ("corporate", 8)]
TIERS = [("bronze", 55), ("silver", 28), ("gold", 13), ("platinum", 4)]
CATEGORIES = [("Kitchen", 25), ("Garden", 20), ("Bath", 18), ("Lighting", 15), ("Storage", 15),
              ("Textiles", 12), ("Decor", 13), ("Clearance", 2)]
BRANDS = [f"Brand{chr(65 + i)}{chr(75 + i)}" for i in range(20)]
CHANNELS = [("web", 50), ("store", 25), ("app", 20), ("phone", 5)]
STATUSES = [("delivered", 80), ("shipped", 8), ("cancelled", 5), ("returned", 7)]
PAYMENTS = [("card", 65), ("paypal", 20), ("bank_transfer", 10), ("gift_card", 5)]
DISCOUNT_CODES = [("SUMMER10", 40), ("WELCOME5", 30), ("BLACKFRIDAY", 20), ("VIP20", 10)]
ADJECTIVES = ["Ultra", "Classic", "Nordic", "Rustic", "Compact", "Deluxe", "Eco", "Urban", "Vintage", "Smart"]
NOUNS = ["Kettle", "Lamp", "Planter", "Towel", "Shelf", "Basket", "Mirror", "Vase", "Cushion", "Rack", "Bench", "Jar"]
GIFT_TEMPLATES = ["Joyeux anniversaire {n}, avec toute notre affection numero {i}",
                  "Pour {n}, merci pour tout et a tres vite (ref {i})",
                  "Bonne fete {n}! Un petit cadeau surprise {i}"]

PLANTED_SUBTOTAL = Decimal("98765.43")
PLANTED_QUANTITY = 500


@dataclass
class RetailFixture:
    """Raw values the profile must never contain, grouped by table.column."""

    sensitive_values: dict[str, list[str]] = field(default_factory=dict)
    rare_labels: dict[str, list[str]] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)

    def table(self, name: str) -> str:
        return f"{CATALOG}.{SCHEMA}.{name}"


def _pick(rng: random.Random, weighted: list[tuple[str, int]]) -> str:
    return rng.choices([v for v, _ in weighted], weights=[w for _, w in weighted])[0]


def _ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()


def _field(name: str, dtype, comment: str, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable, metadata={"comment": comment})


def _place_rare(rng: random.Random, rows: list[dict], column: str, value: str, count: int) -> None:
    for row in rng.sample(rows, count):
        row[column] = value


def build_customers(rng: random.Random, fx: RetailFixture, n: int = 2000) -> tuple[StructType, list[dict]]:
    rows = []
    for cid in range(1, n + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        rows.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{_ascii(first)}.{_ascii(last)}{cid}@example-mail.test",
            "phone": "+33 6 " + " ".join(f"{rng.randint(0, 99):02d}" for _ in range(4)),
            "birth_date": date(1945, 1, 1) + timedelta(days=rng.randint(0, 22000)),
            "postcode": f"{rng.randint(1000, 95999):05d}",
            "street_address": f"{rng.randint(1, 180)} {rng.choice(STREETS)}",
            "tax_ref": f"TXR-{rng.randint(10**7, 10**8 - 1)}",
            "contact_handle": f"@{_ascii(first)}_{_ascii(last)[:3]}{rng.randint(10, 9999)}" if rng.random() < 0.6 else None,
            "city": _pick(rng, CITIES),
            "country": _pick(rng, COUNTRIES),
            "segment": _pick(rng, SEGMENTS),
            "loyalty_tier": _pick(rng, TIERS),
            "signup_date": date(2019, 1, 1) + timedelta(days=rng.randint(0, 6 * 365)),
            "is_newsletter": None if rng.random() < 0.03 else rng.random() < 0.4,
            "referred_by_customer_id": rng.randint(1, cid - 1) if cid > 1 and rng.random() < 0.15 else None,
        })
    _place_rare(rng, rows, "country", "Liechtenstein", 3)
    fx.rare_labels["customers.country"] = ["Liechtenstein"]
    for column in ("first_name", "last_name", "email", "phone", "birth_date", "postcode", "street_address",
                   "tax_ref", "contact_handle"):
        fx.sensitive_values[f"customers.{column}"] = sorted({str(r[column]) for r in rows if r[column] is not None})
    schema = StructType([
        _field("customer_id", IntegerType(), "Unique customer identifier", nullable=False),
        _field("first_name", StringType(), "Customer first name"),
        _field("last_name", StringType(), "Customer last name"),
        _field("email", StringType(), "Customer email address"),
        _field("phone", StringType(), "Mobile phone number"),
        _field("birth_date", DateType(), "Date of birth"),
        _field("postcode", StringType(), "Postal code of the billing address"),
        _field("street_address", StringType(), "Billing street address"),
        _field("tax_ref", StringType(), "Tax reference"),
        _field("contact_handle", StringType(), "Social media handle"),
        _field("city", StringType(), "Billing city"),
        _field("country", StringType(), "Billing country"),
        _field("segment", StringType(), "Commercial segment"),
        _field("loyalty_tier", StringType(), "Loyalty programme tier"),
        _field("signup_date", DateType(), "Date the customer account was created"),
        _field("is_newsletter", BooleanType(), "Subscribed to the newsletter"),
        _field("referred_by_customer_id", IntegerType(), "Customer who referred this customer"),
    ])
    return schema, rows


def build_products(rng: random.Random, fx: RetailFixture) -> tuple[StructType, list[dict]]:
    categories = [c for c, count in CATEGORIES for _ in range(count)]
    rng.shuffle(categories)
    rows = []
    for i, category in enumerate(categories, start=1):
        price = Decimal(rng.randint(500, 30000)) / 100
        rows.append({
            "product_id": f"P-{i:05d}",
            "product_name": f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS)} {category[:3].upper()}-{i:03d}",
            "category": category,
            "brand": rng.choice(BRANDS),
            "unit_price": price,
            "cost": (price * Decimal(rng.randint(40, 70)) / 100).quantize(Decimal("0.01")),
            "launch_date": date(2018, 1, 1) + timedelta(days=rng.randint(0, 7 * 365)),
            "is_active": rng.random() < 0.9,
        })
    fx.rare_labels["products.category"] = ["Clearance"]
    fx.sensitive_values["products.product_name"] = [r["product_name"] for r in rows]
    schema = StructType([
        _field("product_id", StringType(), "Product SKU", nullable=False),
        _field("product_name", StringType(), "Commercial product name"),
        _field("category", StringType(), "Product category"),
        _field("brand", StringType(), "Brand name"),
        _field("unit_price", DecimalType(10, 2), "Current list price in EUR"),
        _field("cost", DecimalType(10, 2), "Unit purchase cost in EUR"),
        _field("launch_date", DateType(), "Date the product was launched"),
        _field("is_active", BooleanType(), "Product currently sold"),
    ])
    return schema, rows


def _order_timestamp(rng: random.Random) -> datetime:
    start = date(2023, 1, 1)
    while True:
        d = start + timedelta(days=rng.randint(0, 910))
        # Seasonality: November/December peak, weekends busier.
        weight = (1.8 if d.month in (11, 12) else 1.0) * (1.4 if d.weekday() >= 5 else 1.0)
        if rng.random() < weight / 2.52:
            break
    hour = rng.choices(range(24), weights=[1, 1, 1, 1, 1, 1, 2, 3, 5, 6, 7, 8, 9, 8, 7, 7, 8, 9, 11, 12, 11, 8, 5, 2])[0]
    return datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59))


def build_orders_and_lines(
    rng: random.Random, fx: RetailFixture, customers: list[dict], products: list[dict], n_orders: int = 10000
) -> tuple[StructType, list[dict], StructType, list[dict]]:
    # Skewed customer activity; about a tenth of customers never order.
    active = [c["customer_id"] for c in customers if rng.random() > 0.1]
    weights = [rng.paretovariate(1.5) for _ in active]
    popularity = [rng.paretovariate(1.2) for _ in products]

    orders, lines = [], []
    for oid in range(100001, 100001 + n_orders):
        ts = _order_timestamp(rng)
        status = _pick(rng, STATUSES)
        n_lines = rng.choices([1, 2, 3, 4, 5], weights=[40, 28, 17, 10, 5])[0]
        subtotal = Decimal("0.00")
        for line_no in range(1, n_lines + 1):
            if rng.random() < 0.015:
                product_id, price = f"P-9{rng.randint(1000, 9999)}", Decimal(rng.randint(500, 9000)) / 100
            else:
                p = rng.choices(products, weights=popularity)[0]
                product_id, price = p["product_id"], p["unit_price"]
            qty = rng.choices([1, 2, 3, 4, 5], weights=[60, 22, 10, 5, 3])[0]
            amount = (price * qty).quantize(Decimal("0.01"))
            subtotal += amount
            gift = None
            if rng.random() < 0.04:
                gift = rng.choice(GIFT_TEMPLATES).format(n=rng.choice(FIRST_NAMES), i=rng.randint(1000, 999999))
            lines.append({
                "order_id": oid, "line_no": line_no, "product_id": product_id, "quantity": qty,
                "unit_price": price, "discount_pct": Decimal(rng.choice([0, 0, 0, 5, 10])),
                "line_amount": amount, "gift_message": gift,
            })
        shipping_fee = Decimal("0.00") if subtotal >= 50 else Decimal("4.99")
        channel = _pick(rng, CHANNELS)
        if channel == "phone":
            shipping_fee = Decimal("9.99")
        ship_date = None
        if status != "cancelled":
            delta = rng.randint(-2, -1) if rng.random() < 0.01 else rng.randint(0, 5)
            ship_date = ts.date() + timedelta(days=delta)
        orders.append({
            "order_id": oid,
            "customer_id": rng.choices(active, weights=weights)[0],
            "order_ts": ts,
            "ship_date": ship_date,
            "channel": channel,
            "status": status,
            "payment_method": _pick(rng, PAYMENTS),
            "subtotal": subtotal,
            "shipping_fee": shipping_fee,
            "total": subtotal + shipping_fee,
            "discount_code": _pick(rng, DISCOUNT_CODES) if rng.random() < 0.15 else None,
            "shipping_address": f"{rng.randint(1, 180)} {rng.choice(STREETS)}, {rng.randint(1000, 95999):05d}",
            "ip_address": f"{rng.randint(11, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}",
        })

    _place_rare(rng, orders, "status", "lost", 4)
    _place_rare(rng, [o for o in orders if o["discount_code"] is None], "discount_code", "STAFF50", 3)
    outlier = rng.choice(orders)
    outlier["subtotal"], outlier["total"] = PLANTED_SUBTOTAL, PLANTED_SUBTOTAL + outlier["shipping_fee"]
    rng.choice(lines)["quantity"] = PLANTED_QUANTITY

    fx.rare_labels["orders.status"] = ["lost"]
    fx.rare_labels["orders.discount_code"] = ["STAFF50"]
    fx.sensitive_values["orders.shipping_address"] = sorted({o["shipping_address"] for o in orders})
    fx.sensitive_values["orders.ip_address"] = sorted({o["ip_address"] for o in orders})
    fx.sensitive_values["order_lines.gift_message"] = sorted({ln["gift_message"] for ln in lines if ln["gift_message"]})

    orders_schema = StructType([
        _field("order_id", LongType(), "Unique order identifier", nullable=False),
        _field("customer_id", IntegerType(), "Customer who placed the order", nullable=False),
        _field("order_ts", TimestampType(), "When the order was placed (UTC)"),
        _field("ship_date", DateType(), "Date the order left the warehouse"),
        _field("channel", StringType(), "Sales channel"),
        _field("status", StringType(), "Current order status"),
        _field("payment_method", StringType(), "Payment method"),
        _field("subtotal", DecimalType(12, 2), "Sum of line amounts in EUR"),
        _field("shipping_fee", DecimalType(8, 2), "Shipping fee in EUR"),
        _field("total", DecimalType(12, 2), "Amount charged: subtotal plus shipping fee"),
        _field("discount_code", StringType(), "Promotion code used, if any"),
        _field("shipping_address", StringType(), "Delivery address"),
        _field("ip_address", StringType(), "IP address of the buyer"),
    ])
    lines_schema = StructType([
        _field("order_id", LongType(), "Order this line belongs to", nullable=False),
        _field("line_no", IntegerType(), "Line number within the order", nullable=False),
        _field("product_id", StringType(), "Product sold"),
        _field("quantity", IntegerType(), "Units sold"),
        _field("unit_price", DecimalType(10, 2), "Unit price at the time of sale"),
        _field("discount_pct", DecimalType(5, 2), "Line discount in percent"),
        _field("line_amount", DecimalType(12, 2), "quantity times unit_price"),
        _field("gift_message", StringType(), "Optional gift message written by the buyer"),
    ])
    return orders_schema, orders, lines_schema, lines


TABLE_COMMENTS = {
    "customers": "One row per registered customer",
    "products": "Product catalogue",
    "orders": "One row per customer order",
    "order_lines": "One row per product line in an order",
}


def build_retail(spark: SparkSession, seed: int = 7) -> RetailFixture:
    rng = random.Random(seed)
    fx = RetailFixture()
    c_schema, customers = build_customers(rng, fx)
    p_schema, products = build_products(rng, fx)
    o_schema, orders, l_schema, lines = build_orders_and_lines(rng, fx, customers, products)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")
    for name, schema, rows, partition in [
        ("customers", c_schema, customers, None),
        ("products", p_schema, products, None),
        ("orders", o_schema, orders, "channel"),
        ("order_lines", l_schema, lines, None),
    ]:
        df = spark.createDataFrame(rows, schema=schema)
        writer = df.write.mode("overwrite").format("parquet")
        if partition:
            writer = writer.partitionBy(partition)
        writer.saveAsTable(fx.table(name))
        spark.sql(f"COMMENT ON TABLE {fx.table(name)} IS '{TABLE_COMMENTS[name]}'")
        fx.row_counts[name] = len(rows)
    return fx


def build_fake_information_schema(spark: SparkSession) -> None:
    """Mimic Unity Catalog's information_schema for the fixture: tags and one declared PK/FK."""
    info = f"{CATALOG}.information_schema"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {info}")
    s = SCHEMA

    def table(name: str, columns: str, rows: list[tuple]) -> None:
        spark.createDataFrame(rows, columns).write.mode("overwrite").saveAsTable(f"{info}.{name}")

    table("table_tags", "catalog_name string, schema_name string, table_name string, tag_name string, tag_value string",
          [(CATALOG, s, "customers", "domain", "crm")])
    table("column_tags",
          "catalog_name string, schema_name string, table_name string, column_name string, tag_name string, tag_value string",
          [(CATALOG, s, "customers", "tax_ref", "pii", "true"), (CATALOG, s, "orders", "channel", "business", "kpi")])
    table("table_constraints",
          "constraint_catalog string, constraint_schema string, constraint_name string, table_catalog string, "
          "table_schema string, table_name string, constraint_type string",
          [(CATALOG, s, "customers_pk", CATALOG, s, "customers", "PRIMARY KEY"),
           (CATALOG, s, "orders_customer_fk", CATALOG, s, "orders", "FOREIGN KEY")])
    table("key_column_usage",
          "constraint_catalog string, constraint_schema string, constraint_name string, table_catalog string, "
          "table_schema string, table_name string, column_name string, ordinal_position int, "
          "position_in_unique_constraint int",
          [(CATALOG, s, "customers_pk", CATALOG, s, "customers", "customer_id", 1, None),
           (CATALOG, s, "orders_customer_fk", CATALOG, s, "orders", "customer_id", 1, 1)])
    table("referential_constraints",
          "constraint_catalog string, constraint_schema string, constraint_name string, "
          "unique_constraint_catalog string, unique_constraint_schema string, unique_constraint_name string",
          [(CATALOG, s, "orders_customer_fk", CATALOG, s, "customers_pk")])
