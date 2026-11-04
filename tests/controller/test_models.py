"""Model replacement fixtures test control flow, never predictive quality."""

import numpy as np
import pyarrow as pa
import pytest
import yaml

from predictive_cache.controller.models import ModelManager, RuntimeContract, fetch_model
from predictive_cache.ml.baselines import ReadDemandFrequencyScorer
from predictive_cache.ml.dummy import fixture_rows, make_dummy_package
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY, write_package
from predictive_cache.ml.read_demand import READ_DEMAND_SERVING_CONTRACT, feature_schema
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID


class Clock:
    t = 0

    def __call__(self):
        return self.t


def point(pointer, uri, version="fv1"):
    pointer.write_text(
        yaml.safe_dump(
            dict(
                model_uri=uri,
                feature_version=version,
                fallback="baseline-read-frequency"
                if version == "read-demand-fv1"
                else "baseline-frequency",
            )
        )
    )


def test_staging_is_explicit_and_bundle_changes_together(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    one = make_dummy_package(tmp_path / "one", value=0.2)
    two = make_dummy_package(tmp_path / "two", value=0.8)
    clock = Clock()
    manager = ModelManager(pointer, clock=clock)
    try:
        point(pointer, one.as_uri())
        assert manager.poll()
        assert manager.active() is None
        assert manager.readiness() == ("not_ready", ["no_model"])
        assert manager.activate()
        old = manager.active()
        np.testing.assert_allclose(old.score(fixture_rows()), 0.2)
        point(pointer, two.as_uri())
        clock.t = 30
        assert manager.poll()
        assert manager.active() is old
        assert manager.activate()
        new = manager.active()
        assert new.package_id != old.package_id
        np.testing.assert_allclose(new.score(fixture_rows()), 0.8)
        assert new.contract == old.contract
        assert manager.readiness() == ("ready", [])
        with pytest.raises(TypeError):
            new.serving_policy["max_items"] = 100
    finally:
        manager.close()
    assert manager.active() is None


def test_failed_unchanged_pointer_retries_and_keeps_good_model(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "one")
    clock = Clock()
    calls = []
    fail = [False]

    def fetch(uri):
        calls.append(uri)
        if fail[0]:
            raise RuntimeError("SECRET_URI_PASSWORD")
        return package

    point(pointer, "file:///fixture")
    manager = ModelManager(pointer, fetch=fetch, clock=clock)
    try:
        assert manager.poll() and manager.activate()
        active = manager.active()
        fail[0] = True
        point(pointer, "file:///other")
        clock.t = 30
        assert not manager.poll()
        assert manager.active() is active
        assert manager.readiness() == ("degraded", ["model_load_failed"])
        assert manager.plans_allowed()
        clock.t = 60
        assert not manager.poll() and len(calls) == 2
        clock.t = 330
        fail[0] = False
        assert manager.poll() and len(calls) == 3
        assert manager.active() is active
        assert "SECRET" not in repr(manager.readiness())
    finally:
        manager.close()


def test_incompatible_package_keeps_good_model_and_gates_plans(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    good = make_dummy_package(tmp_path / "good")
    bad = make_dummy_package(tmp_path / "bad", contract_config={"fixture": "other"})
    clock = Clock()
    manager = ModelManager(pointer, clock=clock)
    try:
        point(pointer, good.as_uri())
        assert manager.poll() and manager.activate()
        old = manager.active()
        point(pointer, bad.as_uri())
        clock.t = 30
        assert not manager.poll()
        assert manager.active() is old
        assert manager.readiness() == ("degraded", ["contract_mismatch"])
        assert not manager.plans_allowed()
    finally:
        manager.close()


def test_bad_pointer_discards_staged_model(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "package")
    clock = Clock()
    manager = ModelManager(pointer, clock=clock)
    try:
        point(pointer, package.as_uri())
        assert manager.poll()
        pointer.write_text("invalid: true")
        clock.t = 30
        assert not manager.poll()
        assert not manager.activate()
        assert manager.active() is None
    finally:
        manager.close()


def test_contract_known_fallback_and_no_contract_not_ready(tmp_path):
    pointer = tmp_path / "missing"
    generic = ModelManager(pointer, contract=RuntimeContract("fv1", "1" * 64))
    read = ModelManager(pointer, contract=RuntimeContract.read_demand())
    unknown = ModelManager(pointer)
    try:
        assert not generic.poll()
        assert generic.readiness() == ("degraded", ["model_load_failed", "fallback_model"])
        assert generic.plans_allowed()
        bundle = read.active()
        assert bundle.scorer.model_type == "baseline-read-frequency"
        assert bundle.serving_policy["max_items"] == 5
        assert bundle.serving_policy["max_bytes"] == 64 * 2**20
        assert unknown.active() is None and not unknown.plans_allowed()
    finally:
        generic.close()
        read.close()
        unknown.close()


def test_read_package_uses_existing_semantic_loader_and_runtime_capabilities(tmp_path):
    rows = pa.table(
        {
            "volume_id": ["v"],
            "file_id": ["f"],
            "t": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            "read_count_24h": [2],
        }
    )
    package = write_package(
        ReadDemandFrequencyScorer(),
        feature_schema(),
        None,
        rows,
        {
            "semantic": {"semantic_view_id": SEMANTIC_VIEW_ID},
            "feature_version": "read-demand-fv1",
            "serving_contract": {"config": READ_DEMAND_SERVING_CONTRACT},
            "serving_policy": dict(DEFAULT_SERVING_POLICY),
            "data": {"data_kind": "synthetic"},
            "training": {"params": {}},
        },
        tmp_path / "read",
    )
    pointer = tmp_path / "pointer.yaml"
    point(pointer, package.as_uri(), "read-demand-fv1")
    manager = ModelManager(
        pointer, contract=RuntimeContract.read_demand(verified_file_size=True, anchor_context=True)
    )
    try:
        assert manager.poll() and manager.activate()
        assert manager.active().model_version.startswith("baseline-read-frequency@")
        assert manager.active().score(rows).tolist() == [2.0]
    finally:
        manager.close()


def test_fetch_rejects_implicit_or_credential_bearing_uris(tmp_path):
    assert fetch_model((tmp_path / "model.zip").as_uri()) == tmp_path / "model.zip"
    for uri in (
        "http://example/model",
        "clearml://latest",
        "file://host/path",
        "file:///path?secret=credential",
        "clearml://user:pass@server",
    ):
        with pytest.raises(ValueError, match="invalid"):
            fetch_model(uri)


def test_pointer_change_during_download_does_not_stage_old_package(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "one")
    point(pointer, package.as_uri())

    def fetch(uri):
        pointer.write_text("invalid: true")
        return package

    manager = ModelManager(pointer, fetch=fetch)
    try:
        assert not manager.poll() and not manager.activate()
        assert manager.active() is None
    finally:
        manager.close()


def test_corrupt_package_keeps_active_bundle(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "good")
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"corrupt")
    clock = Clock()
    manager = ModelManager(pointer, clock=clock)
    try:
        point(pointer, package.as_uri())
        assert manager.poll() and manager.activate()
        old = manager.active()
        point(pointer, bad.as_uri())
        clock.t = 30
        assert not manager.poll() and not manager.activate()
        assert manager.active() is old
        assert manager.readiness() == ("degraded", ["model_load_failed"])
    finally:
        manager.close()


def test_background_fetch_does_not_block_active_reader(tmp_path):
    from threading import Event, Thread

    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "good")
    clock = Clock()
    entered, release = Event(), Event()
    slow = [False]

    def fetch(uri):
        if slow[0]:
            entered.set()
            assert release.wait(3)
        return package

    manager = ModelManager(pointer, clock=clock, fetch=fetch)
    point(pointer, "file:///first")
    assert manager.poll() and manager.activate()
    active = manager.active()
    slow[0] = True
    point(pointer, "file:///second")
    clock.t = 30
    worker = Thread(target=manager.poll)
    worker.start()
    try:
        assert entered.wait(2)
        assert manager.active() is active
        assert not manager.poll()
    finally:
        release.set()
        worker.join(3)
        assert not worker.is_alive()
        manager.close()


def test_owner_only_cache_is_verified_and_bounded_without_deleting(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    one = make_dummy_package(tmp_path / "one", value=0.2)
    two = make_dummy_package(tmp_path / "two", value=0.8)
    cache = tmp_path / "cache"
    clock = Clock()
    manager = ModelManager(pointer, cache_dir=cache, cache_max_packages=1, clock=clock)
    try:
        point(pointer, one.as_uri())
        assert manager.poll() and manager.activate()
        package_id = manager.active().package_id
        assert (cache / package_id).is_dir()
        assert (cache.stat().st_mode & 0o777) == 0o700
        assert all((p.stat().st_mode & 0o777) == 0o600 for p in (cache / package_id).iterdir())
        point(pointer, two.as_uri())
        clock.t = 30
        assert manager.poll() and manager.activate()
        assert manager.active().package_id != package_id
        assert [p.name for p in cache.iterdir()] == [package_id]
        # Cache corruption is refused on reuse instead of silently activated.
        (cache / package_id / "model.json").write_text("{}")
        point(pointer, one.as_uri())
        clock.t = 60
        assert not manager.poll()
        assert manager.readiness() == ("degraded", ["model_load_failed"])
    finally:
        manager.close()
    assert one.exists() and two.exists()


def test_restored_pointer_after_parse_failure_retries_at_next_poll(tmp_path):
    pointer = tmp_path / "pointer.yaml"
    package = make_dummy_package(tmp_path / "package")
    point(pointer, package.as_uri())
    original = pointer.read_bytes()
    clock = Clock()
    manager = ModelManager(pointer, clock=clock)
    try:
        assert manager.poll()
        pointer.write_text("invalid: true")
        clock.t = 30
        assert not manager.poll() and not manager.activate()
        pointer.write_bytes(original)
        clock.t = 60
        assert manager.poll() and manager.activate()
        assert manager.readiness() == ("ready", [])
    finally:
        manager.close()


def test_partial_read_contract_cannot_activate_semanticless_read_package(tmp_path):
    from predictive_cache.ml.read_demand import READ_DEMAND_SERVING_CONTRACT_SHA256

    rows = pa.table(
        {
            "volume_id": ["v"],
            "file_id": ["f"],
            "t": pa.array([None], type=pa.timestamp("us", tz="UTC")),
            "read_count_24h": [2],
        }
    )
    # This intentionally omits meta.semantic: legacy package writing accepts it.
    package = write_package(
        ReadDemandFrequencyScorer(),
        feature_schema(),
        None,
        rows,
        {
            "serving_contract": {"config": READ_DEMAND_SERVING_CONTRACT},
            "serving_policy": dict(DEFAULT_SERVING_POLICY),
            "data": {"data_kind": "synthetic"},
        },
        tmp_path / "semanticless",
    )
    pointer = tmp_path / "pointer.yaml"
    point(pointer, package.as_uri(), "read-demand-fv1")
    manager = ModelManager(
        pointer, contract=RuntimeContract("read-demand-fv1", READ_DEMAND_SERVING_CONTRACT_SHA256)
    )
    try:
        assert not manager.poll() and not manager.activate()
        assert manager.active() is None
        assert manager.readiness() == ("not_ready", ["contract_mismatch"])
        assert not manager.plans_allowed()
    finally:
        manager.close()
