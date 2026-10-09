from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from synthgen.config import SynthgenConfig
from synthgen.io import OutputPathError, ProfileStore
from synthgen.profile_schema import ColumnProfile, TableProfile, TextStats, write_json_schemas

ROOT = Path(__file__).resolve().parents[1]
BASE = {"dataset": "t", "source": {"tables": ["c.s.t"]}, "output": {"root": "/tmp/x"}}


def _cfg(**sections) -> SynthgenConfig:
    return SynthgenConfig.model_validate({**BASE, **sections})


def test_example_config_loads_and_lists_every_threshold():
    cfg = SynthgenConfig.load(ROOT / "config" / "example_dataset.yaml")
    assert cfg.privacy.k == 10
    # The example spells out every setting, so it matches the full dump with nothing missing.
    raw = __import__("yaml").safe_load((ROOT / "config" / "example_dataset.yaml").read_text())
    for section in ("privacy", "profiling", "relationships", "rules"):
        assert set(raw[section]) == set(getattr(cfg, section).model_dump()), section


@pytest.mark.parametrize("k", [0, 4])
def test_k_below_five_is_rejected(k):
    with pytest.raises(ValidationError, match="k must be >= 5"):
        _cfg(privacy={"k": k})


@pytest.mark.parametrize("lower, upper", [(0.0, 0.99), (0.01, 1.0), (0.5, 0.4)])
def test_clipping_cannot_be_loosened(lower, upper):
    with pytest.raises(ValidationError):
        _cfg(privacy={"clip_lower": lower, "clip_upper": upper})


def test_unknown_config_keys_are_rejected():
    with pytest.raises(ValidationError):
        _cfg(privacy={"kk": 10})
    with pytest.raises(ValidationError):
        SynthgenConfig.model_validate({**BASE, "source": {"tables": ["not_qualified"]}})


def _profile(**overrides) -> TableProfile:
    col = ColumnProfile(
        name="email", position=0, type="string", nullable=True, role="text", sensitive=True,
        sensitive_reasons=["name:email"], null_rate=0.0, distinct_approx=100, distinct_ratio=1.0,
        text=TextStats(patterns=[], other_pattern_share=1.0, semantic_type="email"),
    )
    data = dict(generated_at=datetime(2026, 10, 9, tzinfo=timezone.utc), profiler_version="0.1.0",
                source_table="c.s.t", row_count=100, sample_fraction=1.0, k_threshold=10, columns=[col])
    data.update(overrides)
    return TableProfile(**data)


def test_store_roundtrip_and_stable_output(tmp_path):
    store = ProfileStore.review(tmp_path)
    path = store.write_table_profile(_profile())
    assert path == tmp_path.resolve() / "review" / "c.s.t.profile.json"
    first = path.read_text()
    assert store.read_table_profile("c.s.t") == _profile()
    store.write_table_profile(store.read_table_profile("c.s.t"))
    assert path.read_text() == first  # stable key order and formatting
    assert first.startswith('{\n  "schema_version": "1.0",\n  "generated_at"')


def test_read_rejects_unknown_fields_and_versions(tmp_path):
    store = ProfileStore.review(tmp_path)
    path = store.write_table_profile(_profile())
    data = json.loads(path.read_text())
    data["columns"][0]["sample_values"] = ["jane@example.com"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValidationError, match="Extra inputs"):
        store.read_table_profile("c.s.t")
    data["columns"][0].pop("sample_values")
    data["schema_version"] = "2.0"
    path.write_text(json.dumps(data))
    with pytest.raises(ValidationError):
        store.read_table_profile("c.s.t")


def test_schema_rejects_invalid_numbers():
    with pytest.raises(ValidationError):
        _profile(k_threshold=3)
    with pytest.raises(ValidationError):
        ColumnProfile(name="x", position=0, type="int", nullable=True, role="numeric", sensitive=False,
                      null_rate=1.5, distinct_approx=1)
    with pytest.raises(ValidationError):
        _profile(sample_fraction=float("nan"))


def test_store_refuses_paths_outside_its_folder(tmp_path):
    store = ProfileStore.review(tmp_path)
    for bad in ("../escape.profile.json", "a/b.json", ".hidden"):
        with pytest.raises(OutputPathError):
            store._path(bad)


def test_committed_json_schemas_are_current(tmp_path):
    for generated in write_json_schemas(tmp_path):
        committed = ROOT / "schemas" / generated.name
        assert committed.read_text() == generated.read_text(), f"run: python -m synthgen.profile_schema schemas"
