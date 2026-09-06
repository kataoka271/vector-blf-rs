"""Run-tagged stdout logging.

Every bench line goes through log(), so a run's output stays attributable when several
runs (or several containers writing to one stream) are interleaved. The run id is
process-wide rather than passed down: transports and helpers log from call sites that
never see the TestBench, and one process only ever drives one run.

Each line is prefixed with the wall clock, the time since the run's epoch, the run id,
and -- on a thread running an Ecu whose clock has ticked -- that Ecu's tick index, all
`|`-separated:

    12:34:56.789 | +3.214s | run_001 | t=3 | gateway | bus_a -> bus_b: 0100

The epoch is process-wide for the same reason the run id is, while the tick belongs to
one Ecu's clock and so is thread-local: TestBench gives every Ecu its own thread, and
each run loop binds its clock via `bind_clock()`. Both fields are dropped when unset --
a line logged before start(), or from a thread with no Ecu on it, keeps the shorter
prefix rather than reporting a time it does not have.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bench.clock import Clock

SEPARATOR = " | "

_run_id = "-"
_run_epoch_ns: int | None = None
_local = threading.local()


def set_run_id(run_id: str) -> None:
    """Set the run id every later log() line is tagged with. TestBench.start() calls it."""
    global _run_id
    _run_id = run_id or "-"


def get_run_id() -> str:
    return _run_id


def set_run_epoch(run_epoch_ns: int | None) -> None:
    """Set the epoch later log() lines report their elapsed time against, or None to
    stop reporting one. TestBench.start() calls it with the same epoch it binds into
    every Ecu, so a log line's `+Ns` and a frame's `timestamp_ns` share an origin.
    """
    global _run_epoch_ns
    _run_epoch_ns = run_epoch_ns


def bind_clock(clock: Clock | None) -> None:
    """Bind `clock` as the source of the tick index on this thread's log lines.

    Each Ecu run loop calls this on entry; it is thread-local because a bench's Ecus run
    concurrently on their own threads, each with its own clock and tick count.
    """
    _local.clock = clock


def _fields() -> list[str]:
    now_ns = time.time_ns()
    fields = [f"{time.strftime('%H:%M:%S', time.localtime(now_ns / 1e9))}.{now_ns // 1_000_000 % 1000:03d}"]
    if _run_epoch_ns is not None:
        fields.append(f"{(now_ns - _run_epoch_ns) / 1e9:+.3f}s")
    fields.append(_run_id)
    clock = getattr(_local, "clock", None)
    # -1 is a bound clock that has not ticked yet: a reactive Ecu (gateway, receiver)
    # never ticks at all, and an empty t= field on all of its lines is just noise.
    if clock is not None and clock.tick_index >= 0:
        fields.append(f"t={clock.tick_index}")
    return fields


def log(tag: str, message: str) -> None:
    """Print one `|`-separated line ending in `tag` and `message`, unbuffered. See the
    module docstring for what the leading fields carry.
    """
    print(SEPARATOR.join([*_fields(), tag, message]), flush=True)
