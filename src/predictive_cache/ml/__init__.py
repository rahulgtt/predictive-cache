"""Model scoring interface, model packages and baselines (docs/ml/spec.md)."""

from . import baselines as _baselines  # noqa: F401  (registers baseline-*)
from . import dummy as _dummy  # noqa: F401  (registers dummy-constant)
from .package import LoadedPackage, PackageError, load_package, write_package
from .scorer import KEY_COLUMNS, Scorer, register_scorer, scorer_class

__all__ = [
    "KEY_COLUMNS",
    "LoadedPackage",
    "PackageError",
    "Scorer",
    "load_package",
    "register_scorer",
    "scorer_class",
    "write_package",
]
