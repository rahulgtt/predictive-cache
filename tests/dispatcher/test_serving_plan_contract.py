from __future__ import annotations

import base64
from datetime import timedelta

from predictive_cache.dispatcher.schema import MAX_TASK_BYTES, Plan
from predictive_cache.ml.dummy import make_dummy_package
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY, load_package


def test_dummy_package_policy_produces_dispatcher_plan(tmp_path, clock):
    package_path = make_dummy_package(tmp_path / "packages")
    with load_package(package_path, "fv1") as package:
        policy = package.serving_policy
        assert policy == DEFAULT_SERVING_POLICY
        assert policy == {
            "target": "demand",
            "lead_s": 0,
            "horizon_s": 300,
            "tick_s": 60,
            "min_score": None,
            "recent_skip_s": 300,
            "cooldown_s": 1800,
            "max_items": 20,
            "max_bytes": 256 * 2**20,
            "range_cap": 16 * 2**20,
        }

        created_at = clock.value
        deadline = created_at + timedelta(seconds=policy["horizon_s"])
        handle = base64.urlsafe_b64encode(b"dispatcher-contract-handle").rstrip(b"=").decode()
        lengths = (4096, policy["range_cap"])
        raw_plan = {
            "schema_version": 2,
            "run_id": "run-serving-fixture",
            "plan_id": "plan-serving-fixture",
            "model_version": package.model_version,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "deadline": deadline.isoformat().replace("+00:00", "Z"),
            "max_bytes": policy["max_bytes"],
            "items": [
                {
                    "task_id": f"task-{index}",
                    "volume_id": "vol-1",
                    "file_id": f"file-{index}",
                    "export_name": "export-1",
                    "export_id": "export-id-1",
                    "target_fh_b64": handle,
                    "target_valid_until": deadline.isoformat().replace("+00:00", "Z"),
                    "offset": 0,
                    "length": length,
                    "max_bytes": length,
                    "score": 0.5,
                    "reasons": ["dummy-package-fixture"],
                }
                for index, length in enumerate(lengths, start=1)
            ],
        }

    plan = Plan.model_validate(raw_plan)
    assert plan.max_bytes == policy["max_bytes"]
    assert len(plan.items) <= policy["max_items"]
    assert all(item.length <= policy["range_cap"] for item in plan.items)
    assert all(item.length <= MAX_TASK_BYTES for item in plan.items)
