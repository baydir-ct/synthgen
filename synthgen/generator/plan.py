"""Generation plan: which tables, in which order, how many rows, which FK drives each table's size."""

from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from synthgen.io import PROFILE_SUFFIX, RELATIONSHIPS_FILE
from synthgen.profile_schema import ColumnProfile, ForeignKey, PrimaryKey, Relationships, TableProfile

log = logging.getLogger(__name__)


@dataclass
class GeneratorSettings:
    scale: float = 0.01  # multiplies every source row count
    min_rows: int = 100  # tables never shrink below this (or their real size, if smaller)
    max_rows: int = 1_000_000  # hard cap per table: rows are generated in Python on the driver
    seed: int = 42

    def __post_init__(self) -> None:
        if not 0 < self.scale <= 10:
            raise ValueError("scale must be in (0, 10]")
        if self.min_rows < 1 or self.max_rows < self.min_rows:
            raise ValueError("need 1 <= min_rows <= max_rows")


@dataclass
class TablePlan:
    profile: TableProfile
    target_rows: int
    primary_key: list[str] = field(default_factory=list)
    driving_fk: ForeignKey | None = None  # child rows are created per parent row along this FK
    foreign_keys: dict[str, ForeignKey] = field(default_factory=dict)  # child column -> FK

    @property
    def table(self) -> str:
        return self.profile.source_table

    @property
    def name(self) -> str:
        return self.table.split(".")[-1]

    def column(self, name: str) -> ColumnProfile:
        return next(c for c in self.profile.columns if c.name == name)


def load_profiles(source: str | Path) -> tuple[list[TableProfile], Relationships]:
    """Read profiles from a folder (or its review/ subfolder) or from the zip sent by the customer."""
    path = Path(source)
    files: dict[str, str] = {}
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                if name.endswith(".json"):
                    files[Path(name).name] = archive.read(name).decode("utf-8")
    else:
        folder = path / "review" if (path / "review" / RELATIONSHIPS_FILE).exists() else path
        files = {p.name: p.read_text(encoding="utf-8") for p in folder.glob("*.json")}
    if RELATIONSHIPS_FILE not in files:
        raise FileNotFoundError(f"{RELATIONSHIPS_FILE} not found in {source}")
    profiles = [TableProfile.model_validate_json(t) for n, t in sorted(files.items()) if n.endswith(PROFILE_SUFFIX)]
    if not profiles:
        raise FileNotFoundError(f"no *{PROFILE_SUFFIX} files in {source}")
    return profiles, Relationships.model_validate_json(files[RELATIONSHIPS_FILE])


def _scaled(row_count: int, s: GeneratorSettings) -> int:
    return min(row_count, s.max_rows, max(s.min_rows, round(row_count * s.scale))) if row_count else 0


def _choose_driving_fk(plan: TablePlan, fks: list[ForeignKey], sizes: dict[str, int]) -> ForeignKey | None:
    candidates = [
        fk for fk in fks
        if fk.parent_table != plan.table and len(fk.child_columns) == 1 and fk.children_per_parent
        and fk.children_per_parent.quantiles and fk.parent_table in sizes
    ]
    if not candidates:
        return None
    # Prefer an FK that is part of the child's key (order_lines.order_id), then the biggest parent.
    return max(candidates, key=lambda fk: (fk.child_columns[0] in plan.primary_key, sizes[fk.parent_table]))


def build_plan(profiles: list[TableProfile], rel: Relationships, settings: GeneratorSettings) -> list[TablePlan]:
    """Tables in generation order: every driving parent comes before its children."""
    pks: dict[str, PrimaryKey] = {pk.table: pk for pk in rel.primary_keys}
    by_table = {p.source_table: p for p in profiles}
    fks_of: dict[str, list[ForeignKey]] = {}
    for fk in rel.foreign_keys:
        if fk.child_table in by_table and fk.parent_table in by_table:
            fks_of.setdefault(fk.child_table, []).append(fk)

    plans = {
        t: TablePlan(
            profile=p,
            target_rows=_scaled(p.row_count, settings),
            primary_key=list(pks[t].columns) if t in pks else [],
            foreign_keys={fk.child_columns[0]: fk for fk in fks_of.get(t, []) if len(fk.child_columns) == 1},
        )
        for t, p in by_table.items()
    }

    def parents(t: str) -> set[str]:
        return {fk.parent_table for fk in fks_of.get(t, []) if fk.parent_table != t}

    ordered: list[TablePlan] = []
    done: dict[str, int] = {}
    pending = sorted(plans)
    while pending:
        ready = [t for t in pending if parents(t) <= set(done)] or pending[:1]  # pending[:1] breaks a cycle
        for t in ready:
            plan = plans[t]
            plan.driving_fk = _choose_driving_fk(plan, fks_of.get(t, []), done)
            ordered.append(plan)
            done[t] = plan.target_rows
            pending.remove(t)
    return ordered


def describe_plan(plans: list[TablePlan], settings: GeneratorSettings) -> str:
    out = io.StringIO()
    out.write(f"scale={settings.scale:g}  min_rows={settings.min_rows}  max_rows={settings.max_rows}  seed={settings.seed}\n")
    for i, p in enumerate(plans, 1):
        size = (
            f"rows from {p.driving_fk.parent_table.split('.')[-1]}.{p.driving_fk.parent_columns[0]} "
            f"(children per parent)"
            if p.driving_fk else f"~{p.target_rows} rows (source {p.profile.row_count})"
        )
        links = ", ".join(f"{c} -> {fk.parent_table.split('.')[-1]}.{fk.parent_columns[0]}"
                          for c, fk in p.foreign_keys.items())
        out.write(f"{i}. {p.name}: {size}; key {p.primary_key or '-'}; links: {links or '-'}\n")
    return out.getvalue()
