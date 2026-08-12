"""Topology: a named, reusable way to build a `TestBench`.

Before this module, `examples/bench/main.py` hardcoded one topology directly as
imperative Python with no pattern for a second one. This is the same registry idiom
`bench.bus.register_transport` uses for transports: adding a topology means writing
`examples/bench/topologies/<name>.py` with a `build(run_id=None) -> TestBench` function
and calling `register_topology()` once -- not editing `main.py`. `discover()` imports
every module under `topologies/` so each one's registration call actually runs; call it
once (e.g. from `main.py`) before `get_topology()`/`list_topologies()`.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable

from bench.bench import TestBench

BuildFn = Callable[..., TestBench]

_TOPOLOGIES: dict[str, BuildFn] = {}


def register_topology(name: str, build: BuildFn) -> None:
    """Register `build` (a `(run_id: str | None = None) -> TestBench` factory) under
    `name`. Re-registering an existing name replaces it.
    """
    _TOPOLOGIES[name] = build


def get_topology(name: str) -> BuildFn:
    """Return the `build` function registered under `name`.

    Raises ValueError for an unregistered name -- call `discover()` first if `name`
    should have come from `examples/bench/topologies/`.
    """
    try:
        return _TOPOLOGIES[name]
    except KeyError:
        raise ValueError(f"unknown topology {name!r}; registered: {sorted(_TOPOLOGIES)}") from None


def list_topologies() -> list[str]:
    """Return every currently-registered topology name, sorted."""
    return sorted(_TOPOLOGIES)


def discover(package_name: str = "topologies") -> None:
    """Import every submodule of `package_name`, so each one's module-level
    `register_topology()` call runs.

    Topologies are otherwise invisible to `get_topology()`/`list_topologies()` until
    their module has actually been imported -- this is what makes adding
    `topologies/<name>.py` enough on its own, with no import to add anywhere else.
    """
    package = importlib.import_module(package_name)
    for module_info in pkgutil.iter_modules(package.__path__, prefix=f"{package_name}."):
        importlib.import_module(module_info.name)
