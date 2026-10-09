"""Versioned schema for profile files: the only contract between the two workspaces.

Conventions used throughout:
- Shares of column values (categories, patterns, months, ...) are over the column's non-null values.
  `null_rate` and boolean shares are over all rows.
- A rate or share is `null` and its field name is listed in `suppressed` when the underlying count
  was between 1 and k-1. A share of exactly 0.0 means the count was 0.
- `min_clipped` / `max_clipped` are the configured clip quantiles (default 1st and 99th), never true extremes.
  Numeric mean and std are computed on values capped at those bounds, so outliers cannot inflate them.
- Quantile keys are the quantile written as a string, e.g. "0.5".

Run `python -m synthgen.profile_schema schemas/` to regenerate the JSON Schema files for reviewers.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "1.0"

Share = Annotated[float, Field(ge=0.0, le=1.0)]

Role = Literal["key", "foreign_key", "categorical", "numeric", "date", "text", "boolean", "other"]

SemanticType = Literal[
    "email",
    "phone",
    "person_name",
    "postcode",
    "address",
    "uuid",
    "ip_address",
    "iban",
    "national_id",
    "birth_date",
    "geo",
    "secret",
    "special_category",
    "identifier",
    "free_text",
    "short_text",
    "unknown",
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, allow_inf_nan=False)


class LengthStats(_Model):
    """String length statistics. Min and max are clipped quantiles of the length."""

    min_clipped: float | None = None
    mean: float | None = None
    max_clipped: float | None = None


class ValueShare(_Model):
    value: str
    share: Share


class PatternShare(_Model):
    pattern: str = Field(description="Letters mapped to A, digits to 9, punctuation and spaces kept.")
    share: Share


class NumericStats(_Model):
    min_clipped: float | None = None
    max_clipped: float | None = None
    mean: float | None = Field(None, description="Mean of values capped at min_clipped and max_clipped.")
    std: float | None = Field(None, description="Sample std of values capped at min_clipped and max_clipped.")
    quantiles: dict[str, float] = Field(default_factory=dict)
    zero_share: Share | None = None
    negative_share: Share | None = None
    is_integer: bool
    decimals: int = Field(ge=0)


class CategoricalStats(_Model):
    top_values: list[ValueShare] = Field(default_factory=list)
    other_share: Share = Field(description="Share of non-null values in labels not listed (rare or beyond top K).")
    labels_not_exported: int = Field(ge=0, description="Approximate number of labels folded into 'other'.")
    length: LengthStats = Field(default_factory=LengthStats)


class TextStats(_Model):
    """Exported for sensitive, identifier and high-cardinality columns. Never contains values."""

    length: LengthStats = Field(default_factory=LengthStats)
    patterns: list[PatternShare] = Field(default_factory=list)
    other_pattern_share: Share
    semantic_type: SemanticType


class DateStats(_Model):
    """Date and timestamp statistics. Dates are ISO strings; UTC timestamps end in 'Z'."""

    min_clipped: str | None = None
    max_clipped: str | None = None
    quantiles: dict[str, str] = Field(default_factory=dict)
    year_month_shares: dict[str, Share] = Field(default_factory=dict)
    other_year_month_share: Share = 0.0
    day_of_week_shares: dict[str, Share] = Field(default_factory=dict)
    # Timestamp only.
    hour_shares: dict[str, Share] | None = None
    always_midnight: bool | None = None
    timezone: str | None = None


class BooleanStats(_Model):
    true_share: Share | None = None
    false_share: Share | None = None


class ColumnProfile(_Model):
    name: str
    position: int = Field(ge=0)
    type: str = Field(description="Spark SQL type string.")
    nullable: bool
    comment: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    is_partition: bool = False
    role: Role
    sensitive: bool
    sensitive_reasons: list[str] = Field(default_factory=list)
    null_rate: Share | None = None
    distinct_approx: int = Field(ge=0)
    distinct_ratio: float | None = Field(None, ge=0.0, description="distinct_approx / non-null count.")
    suppressed: list[str] = Field(default_factory=list)
    numeric: NumericStats | None = None
    categorical: CategoricalStats | None = None
    text: TextStats | None = None
    date: DateStats | None = None
    boolean: BooleanStats | None = None


class DateOrderRule(_Model):
    kind: Literal["date_order"] = "date_order"
    before: str
    after: str
    match_rate: Share | None = Field(description="Share of rows with both set where before <= after.")
    suppressed: list[str] = Field(default_factory=list)


class NumericIdentityRule(_Model):
    kind: Literal["numeric_identity"] = "numeric_identity"
    target: str
    operation: Literal["product", "sum"]
    operands: list[str] = Field(min_length=2, max_length=2)
    match_rate: Share | None
    sample_fraction: float = Field(gt=0, le=1)
    suppressed: list[str] = Field(default_factory=list)


Rule = Annotated[Union[DateOrderRule, NumericIdentityRule], Field(discriminator="kind")]


class DeclaredForeignKey(_Model):
    name: str | None = None
    columns: list[str] = Field(min_length=1)
    parent_table: str
    parent_columns: list[str] = Field(min_length=1)


class TableProfile(_Model):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    generated_at: datetime
    profiler_version: str
    source_table: str
    row_count: int = Field(ge=0)
    sample_fraction: float = Field(gt=0, le=1)
    k_threshold: int = Field(ge=5)
    comment: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    primary_key: list[str] | None = Field(None, description="Declared primary key, if any.")
    foreign_keys: list[DeclaredForeignKey] = Field(default_factory=list, description="Declared foreign keys.")
    partition_columns: list[str] = Field(default_factory=list)
    columns: list[ColumnProfile]
    rules: list[Rule] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


KeySource = Literal["declared", "config", "inferred"]


class PrimaryKey(_Model):
    table: str
    columns: list[str] = Field(min_length=1)
    source: KeySource
    alternatives: list[list[str]] = Field(default_factory=list, description="Other unique column sets found.")


class ChildrenPerParent(_Model):
    quantiles: dict[str, float] = Field(default_factory=dict)
    zero_children_share: Share | None = None


class ForeignKey(_Model):
    child_table: str
    child_columns: list[str] = Field(min_length=1)
    parent_table: str
    parent_columns: list[str] = Field(min_length=1)
    source: KeySource
    name_score: float | None = None
    containment: Share | None = Field(None, description="Share of non-null child rows whose value is in the parent key.")
    orphan_rate: Share | None = Field(None, description="Share of non-null child rows whose value is not in the parent key.")
    orphan_value_share: Share | None = Field(
        None, description="Share of distinct non-null child values that are not in the parent key."
    )
    children_per_parent: ChildrenPerParent | None = None
    suppressed: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class Relationships(_Model):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    generated_at: datetime
    profiler_version: str
    k_threshold: int = Field(ge=5)
    tables: list[str]
    primary_keys: list[PrimaryKey] = Field(default_factory=list)
    foreign_keys: list[ForeignKey] = Field(default_factory=list)


def write_json_schemas(out_dir: str | Path) -> list[Path]:
    import json

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for model, name in [(TableProfile, "table_profile"), (Relationships, "relationships")]:
        path = out / f"{name}.v{SCHEMA_VERSION}.schema.json"
        path.write_text(json.dumps(model.model_json_schema(), indent=2) + "\n", encoding="utf-8")
        written.append(path)
    return written


if __name__ == "__main__":
    for p in write_json_schemas(sys.argv[1] if len(sys.argv) > 1 else "schemas"):
        print(p)
