"""Shared fixtures for examples/bench's tests.

The suite is split into three tiers, one directory each:

* `unit/`   -- pure, offline, no I/O of any kind. Milliseconds.
* `local/`  -- real moving parts (threads, sockets, an actual Ecu run loop), but nothing
  outside this process: loopback buses and python-can's in-process `virtual` interface.
  Seconds.
* `online/` -- a real Databricks workspace: Lakebase authentication and writes, Zerobus
  ingest. Skipped unless `BENCH_ONLINE=1`; see `online/conftest.py`.

What is deliberately *not* tested anywhere here: anything a type checker already proves.
`uv run ty check` rejects a missing or misspelled constructor argument and a class that
does not satisfy `Clock`/`ChannelTx`/`ChannelRx`, so a test asserting the same thing at
runtime only restates the annotation. Tests here cover behaviour a type checker cannot
see: runtime validation of *values* (a blank string, a table name that is not a bare
identifier), dispatch decided by a string key, SQL text, wire encodings, and timing.
"""

from __future__ import annotations

import pytest
from transport.loopback import reset


@pytest.fixture(autouse=True)
def _isolate_loopback_segments():
    """Loopback segments are process-global (keyed by bus name), so reset them around
    every test to keep tests from leaking frames into each other.
    """
    reset()
    yield
    reset()
