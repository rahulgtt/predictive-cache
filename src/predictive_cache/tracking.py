"""Thin, offline-safe ClearML tracking wrapper (docs/ml/spec.md §9).

Training/evaluation code talks to a ``Tracker``. Without ClearML installed,
with ``--no-track``, or with ``PCACHE_TRACKING=off`` it is a no-op tracker
with the same API, so nothing requires the server.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol


class Tracker(Protocol):
    task_id: str | None

    def connect(self, params: dict, name: str) -> dict: ...

    def connect_configuration(self, config: dict, name: str) -> dict: ...

    def scalar(self, title: str, series: str, value: float, iteration: int = 0) -> None: ...

    def table(self, title: str, rows: list[dict]) -> None: ...

    def artifact(self, name: str, path: Path) -> None: ...

    def register_model(
        self, package_zip: Path, name: str, tags: list[str], metadata: dict[str, str]
    ) -> str | None: ...

    def add_tags(self, tags: list[str]) -> None: ...

    def get_model_copy(self, model_id: str) -> Path: ...

    def tag_model(self, model_id: str, tags: list[str]) -> None: ...

    def prior_runs(self, project: str, tags: list[str]) -> int: ...

    def fail(self, message: str) -> None: ...

    def close(self) -> None: ...


class NullTracker:
    """Same API, records nothing remotely; returns inputs unchanged."""

    task_id: str | None = None

    def connect(self, params: dict, name: str) -> dict:
        return params

    def connect_configuration(self, config: dict, name: str) -> dict:
        return config

    def scalar(self, title: str, series: str, value: float, iteration: int = 0) -> None:
        return None

    def table(self, title: str, rows: list[dict]) -> None:
        return None

    def artifact(self, name: str, path: Path) -> None:
        return None

    def register_model(
        self, package_zip: Path, name: str, tags: list[str], metadata: dict[str, str]
    ) -> str | None:
        return None

    def add_tags(self, tags: list[str]) -> None:
        return None

    def get_model_copy(self, model_id: str) -> Path:
        # Reading the registry creates no task, so untracked runs may use it when configured.
        if not (clearml_available() and clearml_configured()):
            raise ValueError("clearml:// package references need a configured ClearML server")
        from clearml import Model

        return Path(Model(model_id=model_id).get_local_copy(raise_on_error=True))

    def tag_model(self, model_id: str, tags: list[str]) -> None:
        return None

    def prior_runs(self, project: str, tags: list[str]) -> int:
        # Untracked runs still honour the server's record when one is configured.
        return query_prior_runs(project, tags)

    def fail(self, message: str) -> None:
        return None

    def close(self) -> None:
        return None


def on_clearml_agent() -> bool:
    """True inside a task executed by a ClearML agent (it sets CLEARML_TASK_ID)."""
    return bool(os.environ.get("CLEARML_TASK_ID"))


def clearml_available() -> bool:
    try:
        import clearml  # noqa: F401
    except ImportError:
        return False
    return True


def clearml_configured() -> bool:
    """True only if a ClearML server is explicitly configured.

    Without this the SDK may fall back to a public demo server, which must
    never receive project data.
    """
    if os.environ.get("CLEARML_API_HOST") or on_clearml_agent():
        return True
    config = os.environ.get("CLEARML_CONFIG_FILE")
    if config:
        return Path(config).is_file()
    return (Path.home() / "clearml.conf").is_file()


def tracking_enabled(no_track: bool = False) -> bool:
    if no_track or os.environ.get("PCACHE_TRACKING", "on").lower() == "off":
        return False
    return clearml_available() and clearml_configured()


class ClearMLTracker:
    """Adapter over a ``clearml.Task``. Never log secrets or file paths of real data."""

    def __init__(self, task: Any) -> None:
        self.task = task
        self.task_id = task.id

    @classmethod
    def start(
        cls,
        project: str,
        name: str,
        tags: list[str],
        task_type: str = "training",
        requirements: tuple[str, ...] = (),
    ) -> ClearMLTracker:
        from clearml import Task

        for package in requirements:
            Task.add_requirements(package)
        task = Task.init(
            project_name=project,
            task_name=name,
            task_type=getattr(Task.TaskTypes, task_type),
            tags=tags,
            output_uri=True,
            reuse_last_task_id=False,
            auto_connect_arg_parser=False,
            auto_connect_frameworks=False,
        )
        return cls(task)

    def running_locally(self) -> bool:
        from clearml import Task

        return Task.running_locally()

    def execute_remotely(self, queue: str) -> None:
        self.task.execute_remotely(queue_name=queue, clone=False)

    def connect(self, params: dict, name: str) -> dict:
        return dict(self.task.connect(dict(params), name=name))

    def connect_configuration(self, config: dict, name: str) -> dict:
        return dict(self.task.connect_configuration(dict(config), name=name))

    def scalar(self, title: str, series: str, value: float, iteration: int = 0) -> None:
        if value is not None:
            self.task.get_logger().report_scalar(title, series, float(value), iteration)

    def table(self, title: str, rows: list[dict]) -> None:
        if not rows:
            return
        header = list(rows[0])
        data = [header] + [[row.get(k) for k in header] for row in rows]
        self.task.get_logger().report_table(title, "table", 0, table_plot=data)

    def artifact(self, name: str, path: Path) -> None:
        self.task.upload_artifact(name, str(path), wait_on_upload=True)

    def register_model(
        self, package_zip: Path, name: str, tags: list[str], metadata: dict[str, str]
    ) -> str | None:
        from clearml import OutputModel

        model = OutputModel(task=self.task, name=name, tags=tags, framework="custom")
        model.update_weights(weights_filename=str(package_zip), auto_delete_file=False)
        for key, value in metadata.items():
            try:
                model.set_metadata(key, str(value), "str")
            except Exception:  # metadata is best-effort; the package carries its own manifest
                pass
        return model.id

    def add_tags(self, tags: list[str]) -> None:
        self.task.add_tags(list(tags))

    def get_model_copy(self, model_id: str) -> Path:
        from clearml import Model

        return Path(Model(model_id=model_id).get_local_copy(raise_on_error=True))

    def tag_model(self, model_id: str, tags: list[str]) -> None:
        from clearml import Model

        model = Model(model_id=model_id)
        model.tags = sorted(set(model.tags or []) | set(tags))

    def prior_runs(self, project: str, tags: list[str]) -> int:
        """Completed runs in ``project`` carrying all ``tags`` (excluding this task)."""
        return query_prior_runs(project, tags, exclude_task=self.task_id)

    def fail(self, message: str) -> None:
        self.task.mark_failed(
            status_reason="pcache error", status_message=message[:250], force=True
        )

    def close(self) -> None:
        self.task.close()


def get_dataset_copy(dataset_id: str) -> Path:
    """Read-only local copy of a ClearML Dataset (cached by the SDK)."""
    from clearml import Dataset

    return Path(Dataset.get(dataset_id=dataset_id).get_local_copy())


def query_prior_runs(project: str, tags: list[str], exclude_task: str | None = None) -> int:
    """Completed/published tasks in ``project`` carrying all ``tags``; 0 if no server is configured."""
    if not (clearml_available() and clearml_configured()):
        return 0
    from clearml import Task

    # "__$all" makes ClearML require every tag (its default matches any).
    tasks = Task.get_tasks(
        project_name=project,
        tags=["__$all", *tags],
        task_filter={"status": ["completed", "published"]},
    )
    return sum(1 for t in tasks if t.id != exclude_task)


def fail_and_exit(tracker: Tracker, message: str) -> None:
    """Mark a tracked run failed, report ``message`` while the console is still captured, exit 1.

    A closed ClearML task stops forwarding stdout/stderr, so the message is printed
    before closing. Untracked runs raise a normal ClickException.
    """
    import click

    tracker.fail(message)
    if isinstance(tracker, NullTracker):
        tracker.close()
        raise click.ClickException(message)
    click.echo(f"Error: {message}", err=True)
    tracker.close()
    raise SystemExit(1)
