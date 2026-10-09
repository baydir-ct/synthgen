"""Small helpers shared by profiler modules: k-suppression, rounding, column references, conversions."""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F


def col(name: str) -> Column:
    return F.col("`" + name.replace("`", "``") + "`")


def rnd(x: float | int | None, digits: int = 6) -> float | None:
    if x is None:
        return None
    x = float(x)
    if math.isnan(x) or math.isinf(x):
        return None
    return round(x, digits)


def safe_share(count: int | None, total: int | None, k: int) -> float | None:
    """count/total, or None when 0 < count < k (k-suppression). 0 stays 0."""
    if count is None or not total:
        return None
    if count == 0:
        return 0.0
    if count < k:
        return None
    return rnd(count / total)


def share_or_suppress(name: str, count: int | None, total: int | None, k: int, suppressed: list[str]) -> float | None:
    value = safe_share(count, total, k)
    if value is None and count:
        suppressed.append(name)
    return value


def qkey(q: float) -> str:
    return f"{q:g}"


def sum_if(cond: Column) -> Column:
    return F.sum(F.when(cond, 1).otherwise(0))


EPOCH_DATE = date(1970, 1, 1)
EPOCH_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)


def days_to_iso(days: float | None) -> str | None:
    return None if days is None else (EPOCH_DATE + timedelta(days=int(days))).isoformat()


def micros_to_iso(micros: float | None, *, ntz: bool, session_tz: str) -> str | None:
    """UTC timestamps render as ...Z; TIMESTAMP_NTZ renders as local wall time without offset."""
    if micros is None:
        return None
    instant = EPOCH_TS + timedelta(microseconds=int(micros))
    if ntz:
        return instant.astimezone(ZoneInfo(session_tz)).replace(tzinfo=None, microsecond=0).isoformat()
    return instant.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
