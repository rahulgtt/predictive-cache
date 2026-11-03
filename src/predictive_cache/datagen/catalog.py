"""Public catalog projection; private simulation controls never become features."""

from dataclasses import asdict

from .model import World
from .schema import SCHEMAS


def catalog_rows(world: World) -> dict[str, list[dict]]:
    return {
        name: [
            {key: value for key, value in asdict(obj).items() if key in SCHEMAS[name].names}
            for obj in getattr(world, name)
        ]
        for name in ("volumes", "directories", "files", "file_paths", "actors")
    }
