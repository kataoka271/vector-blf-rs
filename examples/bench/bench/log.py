"""Run-tagged stdout logging.

Every bench line goes through log(), so a run's output stays attributable when several
runs (or several containers writing to one stream) are interleaved. The run id is
process-wide rather than passed down: transports and helpers log from call sites that
never see the TestBench, and one process only ever drives one run.
"""

from __future__ import annotations

_run_id = "-"


def set_run_id(run_id: str) -> None:
    """Set the run id every later log() line is tagged with. TestBench.start() calls it."""
    global _run_id
    _run_id = run_id or "-"


def get_run_id() -> str:
    return _run_id


def log(tag: str, message: str) -> None:
    """Print one `[run_id] [tag] message` line, unbuffered."""
    print(f"[{_run_id}] [{tag}] {message}", flush=True)
