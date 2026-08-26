"""Topology: a named, reusable way to build a `TestBench`.

The same registry idiom `bench.bus.register_transport` uses for transports: adding a
topology means writing `examples/bench/topologies/<name>.py` with a
`build(config=None, run_id=None) -> TestBench` function (see `BuildFn` below) and calling
`register_topology()` once -- not editing `main.py`. `discover()` imports every module
under `topologies/` so each one's registration call actually runs; call it once (e.g.
from `main.py`) before `get_topology()`/`list_topologies()`.

`config` is optional in `BuildFn` so a caller holding only a `BuildFn` can still call one
when `main.py` resolves nothing from the environment (a fully offline run leaving
LAKEBASE_*/ZEROBUS_* unset). A topology that needs real credentials states that at
runtime via `require_config()`, which raises `MissingConfig` for `main.py` to turn into a
CLI error, rather than declaring `config` required in its own signature. Fully offline
topologies (see topologies/quickstart.py) just ignore the argument.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Protocol

from transport.connection import ConnectionConfig

from bench.bench import TestBench


class BuildFn(Protocol):
    def __call__(self, config: ConnectionConfig | None = None, run_id: str | None = None) -> TestBench: ...


class MissingConfig(Exception):
    """A topology was built without the connection settings it needs.

    Carries `topology` so a caller can name it; `main.py` catches this to report an
    unset LAKEBASE_*/ZEROBUS_* environment as a CLI usage error rather than a traceback.
    """

    def __init__(self, topology: str) -> None:
        super().__init__(f"topology {topology!r} needs Lakebase/Zerobus connection settings")
        self.topology = topology


def require_config(config: ConnectionConfig | None, topology: str) -> ConnectionConfig:
    """Return `config`, or raise `MissingConfig(topology)` when it is None.

    For topologies that cannot run without real credentials: `BuildFn` makes `config`
    optional (see the module docstring), so the requirement is expressed here instead of
    in the signature.
    """
    if config is None:
        raise MissingConfig(topology)
    return config


_TOPOLOGIES: dict[str, BuildFn] = {}


def register_topology(name: str, build: BuildFn) -> None:
    """Register `build` (a `(config=None, run_id=None) -> TestBench` factory, see
    `BuildFn`) under `name`. Re-registering an existing name replaces it.
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
