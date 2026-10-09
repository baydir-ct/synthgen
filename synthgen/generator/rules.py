"""Post-processing: make the generated rows obey the rules detected in the source."""

from __future__ import annotations

import random
import re
from datetime import date, datetime, timedelta

from synthgen.profile_schema import DateOrderRule, NumericIdentityRule, TableProfile


def _as_date(v) -> date:
    return v.date() if isinstance(v, datetime) else v


def _median_gap_days(profile: TableProfile, before: str, after: str) -> float:
    cols = {c.name: c for c in profile.columns}
    try:
        b = cols[before].date.quantiles["0.5"][:10]
        a = cols[after].date.quantiles["0.5"][:10]
        return (date.fromisoformat(a) - date.fromisoformat(b)).days
    except (AttributeError, KeyError, TypeError, ValueError):
        return 1.0


def apply_date_order(rule: DateOrderRule, profile: TableProfile, data: dict[str, list], rng: random.Random) -> None:
    """after = before + gap for a match_rate share of rows. The gap is drawn up to twice the observed
    difference between the two medians, so e.g. ship dates land a few days after order dates."""
    before, after = data.get(rule.before), data.get(rule.after)
    if before is None or after is None:
        return
    rate = 1.0 if rule.match_rate is None else rule.match_rate
    max_gap = max(1, int(round(2 * max(0.0, _median_gap_days(profile, rule.before, rule.after)))))
    for i, (b, a) in enumerate(zip(before, after)):
        if b is None or a is None or rng.random() >= rate:
            continue
        gap = timedelta(days=rng.randint(0, max_gap))
        if isinstance(a, datetime):
            base = b if isinstance(b, datetime) else datetime(b.year, b.month, b.day, tzinfo=a.tzinfo)
            moved = base + gap
            after[i] = moved if moved >= base else base
        else:
            after[i] = _as_date(b) + gap


def apply_identity(rule: NumericIdentityRule, profile: TableProfile, data: dict[str, list], rng: random.Random) -> None:
    target, (x, y) = rule.target, rule.operands
    if target not in data or x not in data or y not in data:
        return
    col = next(c for c in profile.columns if c.name == target)
    decimals = col.numeric.decimals if col.numeric else 2
    integer = bool(col.numeric and col.numeric.is_integer)
    rate = 1.0 if rule.match_rate is None else rule.match_rate
    values = data[target]
    for i, (a, b) in enumerate(zip(data[x], data[y])):
        if a is None or b is None or values[i] is None or rng.random() >= rate:
            continue
        v = a * b if rule.operation == "product" else a + b
        values[i] = int(round(v)) if integer else round(v, decimals)


def align_emails(profile: TableProfile, data: dict[str, list]) -> None:
    """Emails built from the same row's generated first and last name, when the table has both."""
    sem = {c.name: c.text.semantic_type for c in profile.columns if c.text}
    first = next((c for c, s in sem.items() if s == "person_name" and re.search(r"first|given|forename", c)), None)
    last = next((c for c, s in sem.items() if s == "person_name" and re.search(r"last|family|surname", c)), None)
    if not (first and last):
        return
    for column in (c for c, s in sem.items() if s == "email"):
        for i, email in enumerate(data[column]):
            f, la = data[first][i], data[last][i]
            if email and f and la:
                local = re.sub(r"[^a-z0-9.]", "", f"{f}.{la}".lower())
                data[column][i] = f"{local}{i + 1}@{email.split('@', 1)[1]}"


def apply_rules(profile: TableProfile, data: dict[str, list], seed: int) -> None:
    align_emails(profile, data)
    for i, rule in enumerate(profile.rules):
        rng = random.Random(f"{seed}:{profile.source_table}:rule{i}")
        if isinstance(rule, DateOrderRule):
            apply_date_order(rule, profile, data, rng)
        elif isinstance(rule, NumericIdentityRule):
            apply_identity(rule, profile, data, rng)
