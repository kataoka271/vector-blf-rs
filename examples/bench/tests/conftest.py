"""Shared fixtures for examples/bench's tests."""

from __future__ import annotations

import pytest
from bench.channel import reset


@pytest.fixture(autouse=True)
def _isolate_loopback_segments():
    """Loopback segments are process-global (keyed by bus name), so reset them around
    every test to keep tests from leaking frames into each other.
    """
    reset()
    yield
    reset()
