"""Search profile output for raw values that must never appear in it."""

from __future__ import annotations

import json
from pathlib import Path


def _string_leaves(node) -> list[str]:
    if isinstance(node, dict):
        return [s for k, v in node.items() for s in [k, *_string_leaves(v)]]
    if isinstance(node, list):
        return [s for v in node for s in _string_leaves(v)]
    return [node] if isinstance(node, str) else []


def find_leaks(files: list[Path], forbidden: dict[str, list[str]]) -> list[str]:
    """Return 'file: column=value' for every forbidden raw value found.

    Values with a letter and at least 4 characters, or with at least 8 characters, are searched as
    case-insensitive substrings of the whole file. Shorter, digit-only values (postcodes, small ids)
    would collide with counts and dates, so they are matched against whole JSON strings instead.
    """
    leaks = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        leaves = {s.lower() for s in _string_leaves(json.loads(text))}
        for column, values in forbidden.items():
            for value in values:
                v = value.lower()
                substring = len(v) >= 8 or (len(v) >= 4 and any(ch.isalpha() for ch in v))
                if (substring and v in lower) or (not substring and v in leaves):
                    leaks.append(f"{path.name}: {column}={value!r}")
    return leaks
