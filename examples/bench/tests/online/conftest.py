"""Gating and shared fixtures for the online tier: tests that authenticate against a real
Databricks workspace and write to it.

Opt-in. Set `BENCH_ONLINE=1` to run them:

    BENCH_ONLINE=1 uv run --group testing pytest examples/bench/tests/online -v

Without it every test here skips, so `uv run pytest` stays offline and hermetic; they are
also marked `online`, so `-m "not online"` excludes them explicitly.

Connection settings come from `ConnectionConfig.from_environ()`, the same
LAKEBASE_*/ZEROBUS_* variables `main.py` reads. `examples/bench/.env` is loaded first for
any variable not already set in the environment, matching docker-compose.yml. A tier
whose settings are unset skips on its own (see `derive_or_skip`), so setting only
LAKEBASE_* still runs the whole Lakebase tier.

Everything these tests create is cleaned up: the Lakebase tier writes to a table named
per run and drops it, and the Zerobus tier deletes its own rows by run_id. The Zerobus
write test requires a SQL warehouse (DATABRICKS_WAREHOUSE_ID) and skips without one,
rather than leaving rows it cannot remove.
"""

from __future__ import annotations

import os
import pathlib
import uuid
from collections.abc import Callable
from typing import TypeVar

import pytest
from databricks.sdk.core import Config
from transport.connection import ConnectionConfig, MissingSetting

T = TypeVar("T")

BENCH_DIR = pathlib.Path(__file__).resolve().parents[2]
ENV_FILE = BENCH_DIR / ".env"

# The profile used for the SQL-warehouse verification queries. The Lakebase profile is a
# workspace user profile, which is what a `SELECT`/`DELETE` on the target table needs;
# the Zerobus one is a service principal scoped to Ingest.
VERIFY_PROFILE_VAR = "LAKEBASE_PROFILE"


def _load_dotenv(path: pathlib.Path) -> None:
    """Load `KEY=VALUE` lines from `path`, never overriding an already-set variable.

    Shell-style: blank lines and `#` comments are skipped, and surrounding quotes are
    stripped. A real environment always wins, so exporting a variable overrides the file.
    """
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


@pytest.fixture(scope="session", autouse=True)
def online_enabled() -> None:
    if os.environ.get("BENCH_ONLINE") != "1":
        pytest.skip("online tests are opt-in: set BENCH_ONLINE=1")
    _load_dotenv(ENV_FILE)


@pytest.fixture(scope="session")
def connection_config(online_enabled) -> ConnectionConfig:
    return ConnectionConfig.from_environ()


def derive_or_skip(derive: Callable[[], T]) -> T:
    """Return `derive()`, or skip when the settings that bus config needs are unset.

    Per bus rather than per session: the Lakebase tier still runs with ZEROBUS_* unset,
    and vice versa -- see transport/connection.py.
    """
    try:
        return derive()
    except MissingSetting as exc:
        pytest.skip(f"{exc}, or in {ENV_FILE}")


@pytest.fixture(scope="session")
def verify_config(online_enabled) -> Config:
    """A Databricks SQL Connector config for the warehouse used to verify/clean up writes.

    Skips when no warehouse is configured -- `bench.db.connect` needs one, and a write
    test that cannot verify or clean up after itself should not run at all.
    """
    config = Config(profile=os.environ.get(VERIFY_PROFILE_VAR) or None)
    if not config.warehouse_id:
        pytest.skip("no SQL warehouse configured (set DATABRICKS_WAREHOUSE_ID)")
    return config


@pytest.fixture
def run_id() -> str:
    """A fresh run id per test, so one test's frames are invisible to another's receiver
    even when they share a bus table.
    """
    return f"bench-online-{uuid.uuid4().hex[:12]}"
