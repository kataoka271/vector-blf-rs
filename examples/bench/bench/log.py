"""Run-tagged stdout logging.

Every bench line goes through log(), so a run's output stays attributable when several
runs (or several containers writing to one stream) are interleaved.

Each line carries the wall clock, the time since the run's epoch, the run id, and -- on
a thread running an Ecu whose clock has ticked -- that Ecu's tick index, all
`|`-separated:

    12:34:56.789 | +3.214s | run_001 | t=3 | gateway | bus_a -> bus_b: 0100

The run a line belongs to is thread-local (`bind_run()`), not passed down: transports and
helpers log from call sites that never see the TestBench, while every Ecu gets its own
thread and binds its run there. That is what lets two TestBenches run at once in one
process and still tag their lines apart.

A thread that binds nothing falls back to the process-wide `set_run_id()`/
`set_run_epoch()` values -- lines logged before start(), and a transport's own background
threads (the Lakebase tx/rx workers), which are not covered by the thread that spawned
them. With two benches running concurrently that fallback names whichever started last,
so a caller that has to be exact passes `log(..., run_id=...)` explicitly. Unset fields
are dropped rather than printed empty.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bench.clock import Clock

SEPARATOR = " | "

_local = threading.local()
_run_id = "-"
_run_epoch_ns: int | None = None


def set_run_id(run_id: str) -> None:
    """Set the run id lines from an unbound thread are tagged with."""
    global _run_id
    _run_id = run_id or "-"


def get_run_id() -> str:
    """Return the run id this thread's log lines carry."""
    return getattr(_local, "run_id", None) or _run_id


def set_run_epoch(run_epoch_ns: int | None) -> None:
    """Set the epoch lines from an unbound thread report their elapsed time against, or
    None to stop reporting one.
    """
    global _run_epoch_ns
    _run_epoch_ns = run_epoch_ns


def bind_run(
    *,
    run_id: str | None = None,
    run_epoch_ns: int | None = None,
    clock: Clock | None = None,
) -> None:
    """Bind this thread's log context: the run its lines belong to, the epoch their
    elapsed time is measured from, and the clock their tick index is read off.

    Each Ecu run loop calls this on entry with its own run and clock; TestBench calls it
    for the thread driving the run. Every argument is optional so a caller can bind only
    what it knows.
    """
    if run_id is not None:
        _local.run_id = run_id
    if run_epoch_ns is not None:
        _local.run_epoch_ns = run_epoch_ns
    if clock is not None:
        _local.clock = clock


def _fields(run_id: str | None) -> list[str]:
    now_ns = time.time_ns()
    fields = [f"{time.strftime('%H:%M:%S', time.localtime(now_ns / 1e9))}.{now_ns // 1_000_000 % 1000:03d}"]
    epoch_ns = getattr(_local, "run_epoch_ns", None)
    if epoch_ns is None:
        epoch_ns = _run_epoch_ns
    if epoch_ns is not None:
        fields.append(f"{(now_ns - epoch_ns) / 1e9:+.3f}s")
    fields.append(run_id or get_run_id())
    clock = getattr(_local, "clock", None)
    # -1 is a bound clock that has not ticked yet: a reactive Ecu (gateway, receiver)
    # never ticks at all, and an empty t= field on all of its lines is just noise.
    if clock is not None and clock.tick_index >= 0:
        fields.append(f"t={clock.tick_index}")
    return fields


def log(tag: str, message: str, *, run_id: str | None = None) -> None:
    """Print one `|`-separated line ending in `tag` and `message`, unbuffered.

    `run_id` overrides the thread's own, for a caller logging on behalf of a run it is
    not itself bound to -- one thread driving two concurrent TestBenches.
    """
    print(SEPARATOR.join([*_fields(run_id), tag, message]), flush=True)
