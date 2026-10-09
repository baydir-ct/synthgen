"""Reading and writing profile files.

All profile output goes through `ProfileStore`, which only writes inside its own root folder and
validates every file against the profile schema on write and on read.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from synthgen.profile_schema import Relationships, TableProfile

REVIEW_DIR = "review"
RELATIONSHIPS_FILE = "relationships.json"
PROFILE_SUFFIX = ".profile.json"

M = TypeVar("M", bound=BaseModel)


class OutputPathError(RuntimeError):
    pass


def dump_json(model: BaseModel) -> str:
    """Human-readable, stable JSON: indented, field order fixed by the schema."""
    return json.dumps(model.model_dump(mode="json"), indent=2, ensure_ascii=False) + "\n"


class ProfileStore:
    """A folder of profile files, e.g. <output_root>/review."""

    def __init__(self, folder: str | Path):
        self.folder = Path(folder).resolve()

    @classmethod
    def review(cls, output_root: str | Path) -> ProfileStore:
        return cls(Path(output_root) / REVIEW_DIR)

    def _path(self, filename: str) -> Path:
        if "/" in filename or "\\" in filename or filename.startswith("."):
            raise OutputPathError(f"invalid profile file name: {filename!r}")
        path = (self.folder / filename).resolve()
        if path.parent != self.folder:
            raise OutputPathError(f"{path} is outside {self.folder}")
        return path

    def _write(self, filename: str, model: BaseModel) -> Path:
        text = dump_json(model)
        # Round-trip through the schema so nothing invalid is ever written.
        type(model).model_validate_json(text)
        self.folder.mkdir(parents=True, exist_ok=True)
        path = self._path(filename)
        path.write_text(text, encoding="utf-8")
        return path

    def write_table_profile(self, profile: TableProfile) -> Path:
        return self._write(profile.source_table + PROFILE_SUFFIX, profile)

    def write_relationships(self, rel: Relationships) -> Path:
        return self._write(RELATIONSHIPS_FILE, rel)

    def _read(self, filename: str, model: type[M]) -> M:
        return model.model_validate_json(self._path(filename).read_text(encoding="utf-8"))

    def read_table_profile(self, table: str) -> TableProfile:
        return self._read(table + PROFILE_SUFFIX, TableProfile)

    def read_relationships(self) -> Relationships:
        return self._read(RELATIONSHIPS_FILE, Relationships)

    def table_profiles(self) -> list[TableProfile]:
        return [
            TableProfile.model_validate_json(p.read_text(encoding="utf-8"))
            for p in sorted(self.folder.glob("*" + PROFILE_SUFFIX))
        ]
