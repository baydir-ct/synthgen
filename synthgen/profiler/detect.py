"""Pure-Python heuristics: PII detection, identifier names, roles, semantic types, FK name matching.

Nothing here touches Spark or data; it works on names, types, tags and aggregate statistics only.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from synthgen.config import SynthgenConfig

# Value regexes evaluated in Spark (Java regex syntax); shares feed semantic typing and value-based PII.
VALUE_REGEXES: dict[str, str] = {
    "email": r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    # 8-15 digits with common separators; IPv4 addresses and ISO dates excluded.
    "phone": r"^(?!([0-9]{1,3}\.){3}[0-9]{1,3}$)(?![0-9]{4}-[0-9]{2}-[0-9]{2})(?=(\D*\d){8,15}\D*$)\+?[0-9(][0-9 ().-]{6,22}[0-9]$",
    "ip_address": r"^([0-9]{1,3}\.){3}[0-9]{1,3}$|^[0-9a-fA-F:]{2,39}:[0-9a-fA-F]{1,4}$",
    "iban": r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{10,30}$",
}
# Value patterns strong enough on their own to mark a column sensitive.
VALUE_PII_TYPES = ("email", "phone", "ip_address", "iban")

INTEGER_TYPES = {"tinyint", "smallint", "int", "bigint", "byte", "short", "integer", "long"}
FLOAT_TYPES = {"float", "double", "real"}


def type_family(spark_type: str) -> str:
    """Coarse type family: integer, decimal, float, string, date, timestamp, boolean, other."""
    t = spark_type.lower()
    if t in INTEGER_TYPES:
        return "integer"
    if t.startswith("decimal"):
        m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
        return "integer" if m and m.group(2) == "0" else "decimal"
    if t in FLOAT_TYPES:
        return "float"
    if t == "string" or t.startswith("varchar") or t.startswith("char"):
        return "string"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        return "timestamp"
    if t == "boolean":
        return "boolean"
    return "other"


def is_numeric_family(family: str) -> bool:
    return family in ("integer", "decimal", "float")


def decimal_scale(spark_type: str) -> int | None:
    m = re.match(r"decimal\((\d+),\s*(\d+)\)", spark_type.lower())
    return int(m.group(2)) if m else None


def _matches_any(name: str, patterns: list[str]) -> bool:
    return any(re.search(p, name) for p in patterns)


def is_id_name(column: str, cfg: SynthgenConfig) -> bool:
    return _matches_any(column.lower(), cfg.profiling.id_name_patterns)


def pii_name_matches(column: str, cfg: SynthgenConfig) -> list[str]:
    """Semantic types whose name patterns match the column name, in config order."""
    name = column.lower()
    return [sem for sem, pats in cfg.privacy.pii_name_patterns.items() if _matches_any(name, pats)]


def pii_tag_matches(tags: dict[str, str], cfg: SynthgenConfig) -> list[str]:
    hits = []
    for name, value in tags.items():
        candidates = [name.lower(), f"{name}={value}".lower()]
        if any(fnmatch.fnmatch(c, p.lower()) for c in candidates for p in cfg.privacy.pii_tag_patterns):
            hits.append(f"{name}={value}" if value else name)
    return hits


def config_marks_sensitive(full_column: str, cfg: SynthgenConfig) -> bool:
    return any(fnmatch.fnmatch(full_column.lower(), p.lower()) for p in cfg.privacy.sensitive_columns)


@dataclass
class Sensitivity:
    sensitive: bool
    reasons: list[str] = field(default_factory=list)
    semantic_hint: str | None = None


def static_sensitivity(table: str, column: str, tags: dict[str, str], cfg: SynthgenConfig) -> Sensitivity:
    """Sensitivity known before reading data: UC tags, name heuristics, config."""
    reasons: list[str] = []
    for hit in pii_tag_matches(tags, cfg):
        reasons.append(f"uc_tag:{hit}")
    name_hits = pii_name_matches(column, cfg)
    reasons.extend(f"name:{sem}" for sem in name_hits)
    if config_marks_sensitive(f"{table}.{column}", cfg):
        reasons.append("config")
    return Sensitivity(bool(reasons), reasons, name_hits[0] if name_hits else None)


def value_pii_reasons(regex_shares: dict[str, float], id_named: bool, cfg: SynthgenConfig) -> list[str]:
    """Reasons from value patterns. Phone is skipped for identifier columns (numeric-looking ids)."""
    out = []
    for sem in VALUE_PII_TYPES:
        if sem == "phone" and id_named:
            continue
        if regex_shares.get(sem, 0.0) >= cfg.privacy.value_pii_min_share:
            out.append(f"value_pattern:{sem}")
    return out


def guess_semantic_type(
    *,
    name_hint: str | None,
    regex_shares: dict[str, float],
    id_named: bool,
    mean_length: float | None,
    space_share: float,
    cfg: SynthgenConfig,
) -> str:
    if name_hint:
        return name_hint
    best = max(regex_shares.items(), key=lambda kv: kv[1], default=(None, 0.0))
    if best[0] and best[1] >= cfg.privacy.value_pii_min_share:
        return best[0]
    if id_named:
        return "identifier"
    if mean_length is not None and mean_length >= cfg.profiling.free_text_min_mean_length and space_share >= 0.5:
        return "free_text"
    if mean_length is not None:
        return "short_text"
    return "unknown"


def string_is_categorical(distinct: int, ratio: float | None, cfg: SynthgenConfig) -> bool:
    p = cfg.profiling
    if ratio is None:
        return True  # all null: nothing to export either way
    return (
        distinct <= p.max_categorical_distinct
        and ratio <= p.max_categorical_ratio
        and ratio <= p.high_cardinality_ratio
    )


# --- foreign key name matching -------------------------------------------------------------

_KEY_SUFFIX = re.compile(r"(_?(id|key|uuid|no|nr|code))$")


def singular(word: str) -> str:
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith(("ses", "xes", "ches", "shes")):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 3:
        return word[:-1]
    return word


def normalize_key_name(name: str) -> str:
    return _KEY_SUFFIX.sub("", name.lower()).strip("_")


def fk_name_score(child_column: str, parent_table: str, parent_column: str) -> float:
    """How strongly a child column name suggests a reference to parent_table.parent_column (0..1)."""
    child = child_column.lower()
    pcol = parent_column.lower()
    ptable = singular(parent_table.split(".")[-1].lower())
    stem = normalize_key_name(child)
    if child == pcol and pcol != "id":
        return 1.0
    if stem == ptable:
        return 0.95
    if stem.endswith("_" + ptable):  # e.g. referred_by_customer_id -> customers
        return 0.85
    if stem and stem == normalize_key_name(pcol):
        return 0.9
    if not stem:
        return 0.0
    return max(SequenceMatcher(None, stem, ptable).ratio(), SequenceMatcher(None, stem, normalize_key_name(pcol)).ratio())
