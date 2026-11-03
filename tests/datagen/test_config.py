import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml
from pydantic import ValidationError

from predictive_cache.datagen.config import Config, load_config


@pytest.mark.parametrize(
    ("preset", "volumes", "files", "users", "weeks"),
    [("tiny", 1, 300, 10, 2), ("small", 1, 5000, 50, 8), ("medium", 3, 50000, 300, 12)],
)
def test_preset_targets(preset, volumes, files, users, weeks):
    cfg = load_config(preset=preset)
    assert (len(cfg.volumes), cfg.files.target_count, cfg.actors.users, cfg.period.weeks) == (
        volumes,
        files,
        users,
        weeks,
    )


def test_medium_spans_dst():
    cfg = load_config(preset="medium")
    assert len({v.timezone for v in cfg.volumes}) == 3
    start = datetime.combine(cfg.period.start, time())
    end = start + timedelta(weeks=cfg.period.weeks)
    assert any(
        start.replace(tzinfo=ZoneInfo(v.timezone)).utcoffset()
        != end.replace(tzinfo=ZoneInfo(v.timezone)).utcoffset()
        for v in cfg.volumes
    )


def test_default_small_and_no_shared_state():
    assert Config() == load_config(preset="small")
    first = load_config(preset="tiny")
    first.volumes[0].holidays.clear()
    assert load_config(preset="tiny").volumes[0].holidays


def test_deep_overrides_and_cli_precedence(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "preset: medium\nseed: 12\nformat: jsonl\n"
        "patterns:\n  popularity:\n    working_set_share: 0\n"
        "faults:\n  late:\n    enabled: false\n"
        "files:\n  target_count: 123\n"
    )
    cfg = load_config(path, preset="tiny", seed=0, format="parquet")
    assert cfg.preset == "tiny"
    assert cfg.seed == 0
    assert cfg.format == "parquet"
    assert cfg.files.target_count == 123
    assert cfg.period.weeks == 2
    assert cfg.patterns.popularity.working_set_share == 0
    assert cfg.patterns.popularity.revisit_share == 0.2
    assert cfg.faults.late.enabled is False
    assert cfg.faults.late.rate == 0.01


def test_volume_lists_replace(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("preset: medium\nvolumes:\n  - timezone: UTC\n    holidays: []\n")
    cfg = load_config(path)
    assert len(cfg.volumes) == 1
    assert cfg.volumes[0].work_hours == ("08:00", "18:00")


@pytest.mark.parametrize("preset", ["tiny", "small", "medium", "custom"])
def test_resolved_json_and_yaml_roundtrip(tmp_path, preset):
    cfg = load_config(preset=preset)
    resolved = cfg.model_dump(mode="json")
    assert json.loads(json.dumps(resolved)) == resolved
    path = tmp_path / "resolved.yaml"
    path.write_text(yaml.safe_dump(resolved))
    assert load_config(path) == cfg


def test_all_spec_yaml_controls_parse(tmp_path):
    spec = Path("docs/datagen/spec.md").read_text()
    section = spec.split("### 10.1 Shape", 1)[1]
    example = section.split("```yaml\n", 1)[1].split("```", 1)[0]
    path = tmp_path / "example.yaml"
    path.write_text(example)
    cfg = load_config(path)
    assert cfg == Config()


def test_all_patterns_and_faults_disable_independently():
    baseline = Config().model_dump(mode="json")
    assert len(baseline["patterns"]) == 9
    assert len(baseline["faults"]) == 11
    for group in ("patterns", "faults"):
        for name in baseline[group]:
            cfg = Config.model_validate({group: {name: {"enabled": False}}})
            assert getattr(getattr(cfg, group), name).enabled is False
    cfg = Config.model_validate(
        {
            "patterns": {name: {"enabled": False} for name in baseline["patterns"]},
            "faults": {name: {"enabled": False} for name in baseline["faults"]},
        }
    )
    assert all(not p.enabled for p in cfg.patterns.__dict__.values())
    assert all(not p.enabled for p in cfg.faults.__dict__.values())


@pytest.mark.parametrize(
    "overrides",
    [
        {"wat": 1},
        {"patterns": {"popularity": {"wat": 1}}},
        {"faults": {"late": {"wat": 1}}},
        {"volumes": [{"timezone": "Not/A_Zone"}]},
        {"volumes": [{"work_hours": ["18:00", "08:00"]}]},
        {"volumes": [{"work_hours": ["8:00", "18:00"]}]},
        {"volumes": [{"work_days": [0, 8]}]},
        {"volumes": [{"work_days": [1, 1]}]},
        {"volumes": []},
        {"period": {"start": "2026-02-30"}},
        {"period": {"start": "2026-01-01T00:00:00"}},
        {"period": {"start": 0}},
        {"period": {"start": "9999-12-01", "weeks": 8}},
        {"period": {"weeks": 0}},
        {"period": {"weeks": True}},
        {"files": {"target_count": -1}},
        {"files": {"kinds": {"doc": 0.9}}},
        {"projects": {"lifecycle_weeks": {"ramp": [4, 2]}}},
        {"patterns": {"weekly_schedule": {"files_per_set": [20, 1]}}},
        {"patterns": {"workflow_chain": {"context_order": 3}}},
        {"patterns": {"workflow_chain": {"context_order": True}}},
        {"patterns": {"workflow_chain": {"max_steps": 0}}},
        {"patterns": {"burst": {"arrival_spread_s": 10801}}},
        {"patterns": {"batch_job": {"local_time": "24:00"}}},
        {"patterns": {"popularity": {"working_set_share": 1.1}}},
        {"patterns": {"popularity": {"revisit_share": -0.1}}},
        {"patterns": {"popularity": {"working_set_size": 0}}},
        {"patterns": {"popularity": {"enabled": "false"}}},
        {"faults": {"clock_skew": {"outlier_share": 1.1}}},
        {"faults": {"loss_window": {"hours": [6, 1]}}},
        {"faults": {"loss_window": {"loss_fraction": [0.8, 0.2]}}},
        {"faults": {"late": {"max_delay_s": 21601}}},
        {"realize": {"read_mbps_median": 0}},
        {"realize": {"read_mbps_sigma": float("nan")}},
        {"realize": {"service_think_time_max_s": -1}},
        {"seed": -1},
        {"format": "csv"},
    ],
)
def test_reject_invalid_config(overrides):
    with pytest.raises(ValidationError):
        Config.model_validate(overrides)


def test_named_controls_and_neutral_values():
    cfg = Config.model_validate(
        {
            "calendar": {"holiday_activity": 0},
            "patterns": {
                "popularity": {
                    "working_set_share": 0,
                    "revisit_share": 1,
                    "recency_boost_half_life_days": 0,
                },
                "workflow_chain": {"context_order": 2, "continue_p": 1, "interruption_p": 0},
                "burst": {"participation": 0, "shared_file_share": 0, "arrival_spread_s": 10800},
                "batch_job": {"head_bytes": 1},
            },
            "realize": {"stale_open_rate": 0, "eacces_rate": 0, "read_mbps_sigma": 0},
            "randomness": {"noise_share": 0, "rate_scale": 0, "jitter_scale": 0},
            "faults": {"sampling": {"rate": 0}, "loss_window": {"per_volume_month": 0}},
        }
    )
    assert cfg.patterns.workflow_chain.context_order == 2
    assert cfg.patterns.weekly_schedule.files_per_set == (1, 20)
    assert cfg.patterns.workflow_chain.length_geom_p == 0.35
    assert cfg.patterns.workflow_chain.delay_sigma == 1.0
    assert cfg.faults.sampling.rate == 0
    assert cfg.realize.service_think_time_max_s == 0


@pytest.mark.parametrize("text", ["[]", "123", "preset: invalid", "patterns: {typo: {}}"])
def test_invalid_yaml_configs(tmp_path, text):
    path = tmp_path / "bad.yaml"
    path.write_text(text)
    with pytest.raises((ValueError, ValidationError)):
        load_config(path)


def test_empty_yaml_uses_defaults(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text("")
    assert load_config(path) == Config()
