"""Dataset configuration.

Every threshold the toolkit uses lives here. Defaults are declared once on these models and
`config/example_dataset.yaml` spells all of them out, so a dataset owner can see and tune each one.
Code never hard-codes a threshold; it reads it from a `SynthgenConfig` instance.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Hard floor for k. The plan allows configuring k but never below 5.
MIN_K = 5


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceConfig(_Section):
    tables: list[str] = Field(default_factory=list, description="Explicit catalog.schema.table names.")
    table_patterns: list[str] = Field(
        default_factory=list, description="Glob patterns on catalog.schema.table, e.g. 'main.sales.*'."
    )
    exclude_patterns: list[str] = Field(default_factory=list)
    include_views: bool = False

    @model_validator(mode="after")
    def _non_empty(self) -> SourceConfig:
        if not self.tables and not self.table_patterns:
            raise ValueError("source needs at least one of 'tables' or 'table_patterns'")
        for name in self.tables:
            if name.count(".") != 2:
                raise ValueError(f"table '{name}' must be fully qualified as catalog.schema.table")
        return self


class OutputConfig(_Section):
    root: str = Field(description="Folder the profiler may write to. Profiles land in <root>/review/.")


DEFAULT_PII_NAME_PATTERNS: dict[str, list[str]] = {
    # semantic type -> regexes matched against the lower-cased column name.
    "person_name": [
        r"(^|_)(first|last|full|given|family|middle|maiden|customer|contact|person|user|owner|holder)_?name($|_)",
        r"^name$",
        r"(^|_)(surname|forename|firstname|lastname)($|_)",
    ],
    "email": [r"e_?mail"],
    "phone": [r"phone", r"mobile", r"(^|_)(tel|telephone|fax|msisdn)($|_)"],
    # ip_address precedes address so "ip_address" gets the more specific semantic type.
    "ip_address": [r"(^|_)ip($|_)", r"ip_?addr"],
    "address": [r"address", r"(^|_)(street|addr|address_line)($|_)"],
    "postcode": [r"postcode", r"postal", r"(^|_)zip($|_|code)"],
    "iban": [r"iban", r"(^|_)(swift|bic)($|_)", r"(account|card)_?(number|no|num|nr)", r"(^|_)ccn($|_)"],
    "national_id": [
        r"(^|_)(ssn|dni|nif|nie|nir|cpf|nino|passport)($|_)",
        r"social_?security",
        r"(tax|national|personal)_?id",
    ],
    "birth_date": [r"birth", r"(^|_)dob($|_)"],
    "geo": [r"(^|_)(lat|lon|lng|latitude|longitude|geo|geolocation)($|_)"],
    "secret": [r"password", r"(^|_)(secret|token|api_?key)($|_)"],
    "special_category": [r"(^|_)(ethnic|ethnicity|religion|health|diagnosis|medical)($|_)"],
}

DEFAULT_ID_NAME_PATTERNS = [r"^id$", r"_id$", r"^id_", r"_key$", r"_uuid$", r"^uuid$", r"_no$", r"_nr$", r"^sku$"]


class PrivacyConfig(_Section):
    k: int = Field(10, description=f"Minimum group size. Never below {MIN_K}.")
    top_k_values: int = Field(50, ge=1, description="Max category labels exported per column.")
    clip_lower: float = Field(0.01, description="Quantile exported as 'min'. Must be >= 0.01.")
    clip_upper: float = Field(0.99, description="Quantile exported as 'max'. Must be <= 0.99.")
    min_count_for_distribution: int = Field(
        50, description="Non-null rows needed before quantiles, mean, std or length stats are exported."
    )
    pii_name_patterns: dict[str, list[str]] = Field(default_factory=lambda: dict(DEFAULT_PII_NAME_PATTERNS))
    pii_tag_patterns: list[str] = Field(
        default_factory=lambda: ["pii", "pii.*", "pii_*", "class.*", "sensitive", "personal*", "gdpr*"],
        description="Glob patterns matched against a UC tag name and against 'name=value'.",
    )
    sensitive_columns: list[str] = Field(
        default_factory=list, description="Glob patterns on catalog.schema.table.column forced to sensitive."
    )
    value_pii_min_share: float = Field(
        0.3, gt=0, le=1, description="Share of values matching an email/phone/IP/IBAN regex that marks a column PII."
    )
    allowed_labels: list[str] = Field(
        default_factory=list,
        description="Columns (glob on catalog.schema.table.column) whose category labels may leave. Used by the guard.",
    )

    @field_validator("k")
    @classmethod
    def _k_floor(cls, v: int) -> int:
        if v < MIN_K:
            raise ValueError(f"k must be >= {MIN_K}, got {v}")
        return v

    @model_validator(mode="after")
    def _bounds(self) -> PrivacyConfig:
        if self.clip_lower < 0.01 or self.clip_upper > 0.99 or self.clip_lower >= self.clip_upper:
            raise ValueError("clip_lower must be >= 0.01, clip_upper <= 0.99, and clip_lower < clip_upper")
        if self.min_count_for_distribution < self.k:
            raise ValueError("min_count_for_distribution must be >= k")
        return self


class ProfilingConfig(_Section):
    max_categorical_distinct: int = Field(200, ge=1)
    max_categorical_ratio: float = Field(0.5, gt=0, le=1)
    high_cardinality_ratio: float = Field(
        0.5, gt=0, le=1, description="String columns above this distinct ratio export no values."
    )
    quantiles: list[float] = Field(default_factory=lambda: [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    percentile_accuracy: int = Field(10000, ge=100)
    approx_distinct_rsd: float = Field(0.02, gt=0, le=0.1)
    pattern_top_n: int = Field(5, ge=1)
    pattern_max_length: int = Field(40, ge=1, description="Values longer than this get no format pattern.")
    max_year_months: int = Field(600, ge=1, description="Max year-month buckets exported per date column.")
    max_decimals: int = Field(6, ge=0)
    free_text_min_mean_length: float = Field(30.0, gt=0)
    id_name_patterns: list[str] = Field(default_factory=lambda: list(DEFAULT_ID_NAME_PATTERNS))
    sample_fraction: float = Field(1.0, gt=0, le=1)
    sample_fractions: dict[str, float] = Field(
        default_factory=dict, description="Per-table override, keyed by catalog.schema.table."
    )
    sample_seed: int = 42

    @field_validator("quantiles")
    @classmethod
    def _quantiles(cls, v: list[float]) -> list[float]:
        if any(q <= 0 or q >= 1 for q in v):
            raise ValueError("quantiles must be strictly between 0 and 1")
        return sorted(set(v))

    @field_validator("sample_fractions")
    @classmethod
    def _fractions(cls, v: dict[str, float]) -> dict[str, float]:
        for name, f in v.items():
            if not 0 < f <= 1:
                raise ValueError(f"sample fraction for {name} must be in (0, 1]")
        return v


class ForcedForeignKey(_Section):
    child: str = Field(description="catalog.schema.table.column")
    parent: str = Field(description="catalog.schema.table.column")


class RelationshipsConfig(_Section):
    fk_min_containment: float = Field(0.9, gt=0, le=1)
    name_similarity_threshold: float = Field(0.8, gt=0, le=1)
    pk_prefilter_ratio: float = Field(
        0.9, gt=0, le=1, description="Approx distinct ratio a column needs before an exact uniqueness check."
    )
    max_pk_pair_columns: int = Field(6, ge=2)
    children_quantiles: list[float] = Field(default_factory=lambda: [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
    forced_pks: dict[str, list[str]] = Field(default_factory=dict, description="table -> key columns")
    forced_fks: list[ForcedForeignKey] = Field(default_factory=list)


class RulesConfig(_Section):
    date_order_min_rate: float = Field(0.9, gt=0.5, le=1)
    identity_min_rate: float = Field(0.95, gt=0.5, le=1)
    identity_sample_rows: int = Field(20000, ge=100)
    identity_abs_tolerance: float = Field(0.01, ge=0)
    identity_rel_tolerance: float = Field(1e-6, ge=0)
    max_identity_columns: int = Field(10, ge=3)


class SynthgenConfig(_Section):
    dataset: str
    source: SourceConfig
    output: OutputConfig
    metadata_provider: Literal["auto", "unity_catalog", "spark"] = "auto"
    privacy: PrivacyConfig = Field(default_factory=PrivacyConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    relationships: RelationshipsConfig = Field(default_factory=RelationshipsConfig)
    rules: RulesConfig = Field(default_factory=RulesConfig)

    @classmethod
    def load(cls, path: str | Path) -> SynthgenConfig:
        import yaml  # only needed for YAML configs; notebooks configure in Python

        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        return cls.model_validate(raw)

    def sample_fraction_for(self, table: str) -> float:
        return self.profiling.sample_fractions.get(table, self.profiling.sample_fraction)
