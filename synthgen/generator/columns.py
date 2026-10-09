"""Value samplers, one per kind of column profile. Pure Python, deterministic for a given Random."""

from __future__ import annotations

import bisect
import calendar
import random
import re
import string
import uuid
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from synthgen.generator import vocab
from synthgen.profile_schema import ColumnProfile, DateStats, TextStats
from synthgen.profiler.detect import decimal_scale, type_family

DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def rng_for(seed: int, *parts: object) -> random.Random:
    """Independent, reproducible stream per (seed, table, column, purpose)."""
    return random.Random(":".join(str(p) for p in (seed, *parts)))


# --- distributions ---------------------------------------------------------------------------


def quantile_points(quantiles: dict[str, float]) -> list[tuple[float, float]]:
    return sorted((float(q), float(v)) for q, v in quantiles.items())


def stratified_uniforms(n: int, rng: random.Random) -> list[float]:
    """n uniforms, one per equal-width stratum, shuffled: quantiles are reproduced even for small n."""
    us = [(i + rng.random()) / n for i in range(n)]
    rng.shuffle(us)
    return us


def inverse_cdf(points: list[tuple[float, float]], u: float) -> float:
    """Linear interpolation through (probability, value) points; clamps outside the first/last point."""
    if u <= points[0][0]:
        return points[0][1]
    if u >= points[-1][0]:
        return points[-1][1]
    i = bisect.bisect_right([p for p, _ in points], u)
    (p0, v0), (p1, v1) = points[i - 1], points[i]
    return v0 + (v1 - v0) * (u - p0) / (p1 - p0)


class Weighted:
    """Fast repeated weighted choice."""

    def __init__(self, items: list, weights: list[float]):
        total = sum(weights)
        if not items or total <= 0:
            raise ValueError("Weighted needs items with a positive total weight")
        self.items, self.cum, acc = items, [], 0.0
        for w in weights:
            acc += w
            self.cum.append(acc)

    def pick(self, rng: random.Random):
        return self.at(rng.random())

    def at(self, u: float):
        """The item at cumulative position u in [0, 1): use with stratified uniforms for exact shares."""
        return self.items[min(bisect.bisect_right(self.cum, u * self.cum[-1]), len(self.items) - 1)]


def null_mask(n: int, null_rate: float | None, rng: random.Random) -> list[bool]:
    rate = null_rate or 0.0  # a suppressed rate means fewer than k nulls: treat as none
    return [rng.random() < rate for _ in range(n)]


# --- numeric, categorical, boolean -----------------------------------------------------------


def step_inverse_cdf(points: list[tuple[float, float]], u: float) -> float:
    """Discrete version: the value of the quantile point nearest to u, so only observed values appear
    and each gets roughly the probability mass between its neighbours' midpoints."""
    probs = [p for p, _ in points]
    i = bisect.bisect_left(probs, u)
    if i <= 0:
        return points[0][1]
    if i >= len(points):
        return points[-1][1]
    return points[i - 1][1] if u - probs[i - 1] < probs[i] - u else points[i][1]


# Numeric columns with at most this many distinct values (quantity, fee tiers, ratings) are sampled
# from their quantile values only, so no in-between values appear (no fee of 7.30 between 4.99 and 9.99).
DISCRETE_MAX_DISTINCT = 20


def numeric_values(col: ColumnProfile, n: int, rng: random.Random) -> list[float]:
    s = col.numeric
    points = quantile_points(s.quantiles) if s and s.quantiles else []
    sample = step_inverse_cdf if col.distinct_approx <= DISCRETE_MAX_DISTINCT else inverse_cdf
    out = []
    for u in stratified_uniforms(n, rng):
        if points:
            v = sample(points, u)
        elif s and s.mean is not None:
            v = rng.gauss(s.mean, s.std or abs(s.mean) * 0.1 or 1.0)
        else:
            v = rng.uniform(0, 100)
        out.append(int(round(v)) if (s is None or s.is_integer) else round(v, s.decimals))
    return out


def _title(column: str) -> str:
    return column.replace("_", " ").strip().title() or "Value"


def categorical_values(col: ColumnProfile, n: int, rng: random.Random) -> list[str]:
    c = col.categorical
    items = [v.value for v in c.top_values]
    weights = [v.share for v in c.top_values]
    if c.other_share > 0 or not items:
        # Labels below k were never exported: invent stand-ins sharing the "other" share.
        count = max(1, min(c.labels_not_exported or 1, 50))
        title = _title(col.name)
        invented = [f"{title} {chr(65 + i)}" if count <= 26 else f"{title} {i + 1:02d}" for i in range(count)]
        items += invented
        weights += [max(c.other_share, 1e-9) / count] * count
    w = Weighted(items, weights)
    return [w.at(u) for u in stratified_uniforms(n, rng)]


def boolean_values(col: ColumnProfile, n: int, rng: random.Random) -> list[bool]:
    t, f = (col.boolean.true_share or 0.0), (col.boolean.false_share or 0.0)
    p = t / (t + f) if t + f > 0 else 0.5
    return [u < p for u in stratified_uniforms(n, rng)]


# --- dates ------------------------------------------------------------------------------------


def _to_date(text: str) -> date:
    return date.fromisoformat(text[:10])


class DateSampler:
    def __init__(self, stats: DateStats, timestamp: bool):
        self.s, self.timestamp = stats, timestamp
        # Hours were measured in the source session time zone; aware datetimes keep them there when
        # Spark converts (naive ones would be read in the driver's local zone). NTZ stays naive.
        self.tz = ZoneInfo(stats.timezone) if timestamp and stats.timezone else None
        self.points = [(p, _to_date(v).toordinal()) for p, v in quantile_points_str(stats.quantiles)]
        self.lo = _to_date(stats.min_clipped).toordinal() if stats.min_clipped else None
        self.hi = _to_date(stats.max_clipped).toordinal() if stats.max_clipped else None
        dow = stats.day_of_week_shares
        self.dow_weight = [dow.get(d, 0.0) for d in DOW] if dow else [1.0] * 7
        if sum(self.dow_weight) <= 0:
            self.dow_weight = [1.0] * 7
        months = stats.year_month_shares
        self.months = Weighted(list(months), list(months.values())) if months and stats.other_year_month_share <= 0.3 else None
        hours = stats.hour_shares or {}
        self.hours = Weighted([int(h) for h in hours], list(hours.values())) if hours and sum(hours.values()) > 0 else None
        self._month_days: dict[str, Weighted] = {}

    def _day_in_month(self, ym: str, rng: random.Random) -> date:
        if ym not in self._month_days:
            y, m = int(ym[:4]), int(ym[5:7])
            days = [date(y, m, d) for d in range(1, calendar.monthrange(y, m)[1] + 1)]
            if self.lo is not None and self.hi is not None:
                days = [d for d in days if self.lo <= d.toordinal() <= self.hi] or days
            self._month_days[ym] = Weighted(days, [self.dow_weight[d.weekday()] + 1e-9 for d in days])
        return self._month_days[ym].pick(rng)

    def _day(self, rng: random.Random) -> date:
        if self.months:
            return self._day_in_month(self.months.pick(rng), rng)
        top = max(self.dow_weight)
        for _ in range(20):  # rejection sampling towards the weekday profile
            if self.points:
                d = date.fromordinal(int(round(inverse_cdf(self.points, rng.random()))))
            else:
                d = date.today() - timedelta(days=rng.randint(0, 3 * 365))
            if rng.random() <= self.dow_weight[d.weekday()] / top:
                return d
        return d

    def sample(self, rng: random.Random):
        d = self._day(rng)
        if not self.timestamp:
            return d
        if self.s.always_midnight:
            return datetime(d.year, d.month, d.day, tzinfo=self.tz)
        hour = self.hours.pick(rng) if self.hours else rng.randint(0, 23)
        return datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59), tzinfo=self.tz)


def quantile_points_str(quantiles: dict[str, str]) -> list[tuple[float, str]]:
    return sorted((float(q), v) for q, v in quantiles.items())


def date_values(col: ColumnProfile, n: int, rng: random.Random) -> list:
    sampler = DateSampler(col.date, timestamp=type_family(col.type) == "timestamp")
    return [sampler.sample(rng) for _ in range(n)]


# --- text -------------------------------------------------------------------------------------

_RUN = re.compile(r"A+|9+")
_WORDS_BY_LEN: dict[int, list[str]] = {}
for _w in vocab.WORDS:
    _WORDS_BY_LEN.setdefault(len(_w), []).append(_w)


def fill_pattern(pattern: str, rng: random.Random) -> str:
    """A -> random capital letter, 9 -> random digit, anything else kept."""
    return "".join(
        rng.choice(string.ascii_uppercase) if ch == "A" else str(rng.randint(0, 9)) if ch == "9" else ch
        for ch in pattern
    )


def _word(length: int, rng: random.Random) -> str:
    for delta in range(0, 10):
        for size in (length - delta, length + delta):
            if size in _WORDS_BY_LEN:
                return rng.choice(_WORDS_BY_LEN[size])
    return rng.choice(vocab.WORDS)


def wordy_pattern(pattern: str, rng: random.Random) -> str:
    """Like fill_pattern, but letter runs become real words: 'AAAAA AAAA AAA-999' -> 'Great Item Box-417'."""
    def repl(m: re.Match) -> str:
        run = m.group(0)
        if run[0] == "9":
            return "".join(str(rng.randint(0, 9)) for _ in run)
        return _word(len(run), rng).capitalize() if len(run) > 1 else rng.choice(string.ascii_uppercase)
    return _RUN.sub(repl, pattern)


def counter_pattern(pattern: str, i: int, letters: str) -> str:
    """Key i rendered in the shape of pattern: the last digit run becomes a zero-padded counter,
    letters are taken from `letters` (e.g. 'A-99999' + 'PRODUCTS' -> 'P-00042')."""
    runs = list(re.finditer(r"9+", pattern))
    letters = (letters.upper() or "K")
    out, li = [], 0
    for ch in pattern:
        if ch == "A":
            out.append(letters[li % len(letters)])
            li += 1
        else:
            out.append(ch)
    text = "".join(out)
    if not runs:
        return f"{text}{i}"
    run = runs[-1]
    width = run.end() - run.start()
    return text[: run.start()] + str(i).zfill(width) + text[run.end():].replace("9", "0")


def _pattern_picker(text: TextStats | None) -> Weighted | None:
    if not text or not text.patterns:
        return None
    return Weighted([p.pattern for p in text.patterns], [p.share for p in text.patterns])


def _target_length(text: TextStats | None, rng: random.Random, default: int = 12) -> int:
    if not text or text.length.mean is None:
        return default
    lo = int(text.length.min_clipped or 1)
    hi = max(lo, int(text.length.max_clipped or lo))
    drawn = int(round(rng.gauss(text.length.mean, (hi - lo) / 4 or 1)))
    return max(1, lo, min(hi, drawn))


def _sentence(length: int, rng: random.Random) -> str:
    words: list[str] = []
    while len(" ".join(words)) < length:
        words.append(rng.choice(vocab.WORDS))
    return " ".join(words).capitalize()[: max(length, 1)].rstrip() + "."


def _person(column: str, rng: random.Random) -> str:
    name = column.lower()
    if re.search(r"first|given|forename", name):
        return rng.choice(vocab.FIRST_NAMES)
    if re.search(r"last|family|surname|maiden", name):
        return rng.choice(vocab.LAST_NAMES)
    return f"{rng.choice(vocab.FIRST_NAMES)} {rng.choice(vocab.LAST_NAMES)}"


def text_value(col: ColumnProfile, rng: random.Random, patterns: Weighted | None, serial: int) -> str:
    sem = col.text.semantic_type if col.text else "unknown"
    pattern = patterns.pick(rng) if patterns else None
    if sem == "person_name":
        return _person(col.name, rng)
    if sem == "email":
        first, last = rng.choice(vocab.FIRST_NAMES), rng.choice(vocab.LAST_NAMES).replace(" ", "")
        return f"{first}.{last}{serial}@{rng.choice(vocab.EMAIL_DOMAINS)}".lower()
    if sem == "address":
        street = f"{rng.randint(1, 250)} {rng.choice(vocab.STREETS)}"
        return f"{street}, {rng.choice(vocab.CITIES)}" if pattern and "," in pattern else street
    if sem == "ip_address":
        return ".".join(str(rng.randint(1, 254)) for _ in range(4))
    if sem == "uuid":
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))
    if sem == "birth_date":
        return (date(1940, 1, 1) + timedelta(days=rng.randint(0, 65 * 365))).isoformat()
    if sem == "free_text":
        return _sentence(_target_length(col.text, rng, 60), rng)
    if sem == "short_text" and not col.sensitive:  # e.g. product names; sensitive codes stay code-like
        return wordy_pattern(pattern, rng) if pattern else _sentence(_target_length(col.text, rng), rng).rstrip(".")
    if pattern:  # phone, postcode, iban, national_id, identifier, ...
        return fill_pattern(pattern, rng)
    return fill_pattern("A" * _target_length(col.text, rng, 8), rng)


def text_values(col: ColumnProfile, n: int, rng: random.Random) -> list[str]:
    """Respects the observed number of distinct values: draws from a pool when values repeat."""
    patterns = _pattern_picker(col.text)
    ratio = col.distinct_ratio if col.distinct_ratio is not None else 1.0
    pool_size = max(1, round(ratio * n))
    if pool_size >= 0.9 * n:
        return [text_value(col, rng, patterns, i + 1) for i in range(n)]
    pool = [text_value(col, rng, patterns, i + 1) for i in range(pool_size)]
    return [rng.choice(pool) for _ in range(n)]


def key_values(col: ColumnProfile, n: int, table_name: str, offset: int = 0) -> list:
    """Unique keys 1..n shaped like the source keys (digit count, prefix letters)."""
    family = type_family(col.type)
    pattern = col.text.patterns[0].pattern if col.text and col.text.patterns else None
    if family == "integer":
        digits = pattern.count("9") if pattern and set(pattern) <= {"9", "-"} else 1
        start = 10 ** (digits - 1) if digits > 1 else 1
        return [start + offset + i for i in range(n)]
    if col.text and col.text.semantic_type == "uuid":
        rng = random.Random(f"{table_name}:{col.name}:{offset}")
        return [str(uuid.UUID(int=rng.getrandbits(128), version=4)) for _ in range(n)]
    letters = re.sub(r"[^A-Za-z]", "", table_name) or "K"
    return [counter_pattern(pattern or "A-99999", offset + i + 1, letters) for i in range(n)]


# --- final typing -------------------------------------------------------------------------------


def to_spark_value(value, spark_type: str):
    """Convert a generated Python value to what Spark expects for the column type."""
    if value is None:
        return None
    family = type_family(spark_type)
    try:
        if family == "integer":
            if isinstance(value, str):
                digits = re.sub(r"\D", "", value)
                return int(digits[-15:]) if digits else None
            return int(round(float(value)))
        if family == "decimal":
            scale = decimal_scale(spark_type) or 0
            precision = int(re.match(r"decimal\((\d+)", spark_type.lower()).group(1))
            limit = 10 ** (precision - scale) - 10 ** (-scale)
            v = max(-limit, min(limit, float(value)))
            return Decimal(str(v)).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
        if family == "float":
            return float(value)
        if family == "boolean":
            return bool(value)
        if family == "date":
            if isinstance(value, datetime):
                return value.date()
            return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        if family == "timestamp":
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime(value.year, value.month, value.day)
            return datetime.fromisoformat(str(value).replace("Z", ""))
        if family == "string":
            return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
    except (ValueError, TypeError, ArithmeticError):
        return None
    return None  # complex types (arrays, maps, structs) are left empty
