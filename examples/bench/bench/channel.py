"""ChannelTx/ChannelRx: the contract every channel implementation satisfies, regardless
of what is on the wire underneath. This is what lets `bench.bus.Bus` treat the transport
as a config choice rather than an architectural fork -- `BusHandle` and Ecu code
downstream only ever see these two protocols.

The implementations live one per transport, colocated with that transport's config and
connection details rather than bundled here:

* `transport.loopback` -- in-process, synchronous, deterministic. Default for tests.
* `transport.lakebase` -- managed Postgres, LISTEN/NOTIFY.
* `transport.zerobus` -- Ingest-only sink into a Delta table. Send-only.
* `transport.device` -- python-can (udp_multicast by default, needs no hardware).

Splitting them out this way means importing, say, `transport.loopback` alone never pulls
in psycopg, the Databricks SDK, or python-can -- each transport's heavy dependencies stay
behind that transport's own module boundary.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from bench.frame import Frame


@runtime_checkable
class ChannelTx(Protocol):
    def send(self, frame: Frame) -> None:
        """Send `frame`. May return before it is durable; flush() bounds that."""
        ...

    def flush(self) -> None:
        """Block until everything sent so far has been accepted by the far side."""
        ...

    def close(self) -> None:
        """Flush and release the channel's resources. Idempotent."""
        ...


@runtime_checkable
class ChannelRx(Protocol):
    def poll(self, timeout: float = 1.0) -> list[Frame]:
        """Return frames received since the previous call, oldest first.

        Blocks up to `timeout` seconds for the first frame when none have arrived, and
        returns an empty list on timeout rather than raising.
        """
        ...

    def close(self) -> None:
        """Stop receiving and release resources. Idempotent."""
        ...
