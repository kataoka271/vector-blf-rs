"""Channel implementations: how a Frame actually moves between two Ecus.

Every channel satisfies the same two-method-pair contract (ChannelTx.send/flush/close,
ChannelRx.poll/close) regardless of what is on the wire underneath, which is what lets
`bench.bus.Bus` treat the transport as a config choice rather than an architectural
fork. Four implementations:

* Loopback -- in-process, synchronous, deterministic. Default transport for tests.
* Lakebase -- managed Postgres, LISTEN/NOTIFY. Minimal: one INSERT+NOTIFY per send(), no
  batching (see transport/lakebase.py's module docstring for what that defers).
* Zerobus -- Ingest-only sink into a Delta table. Send-only; ZerobusRx.poll() raises.
* CAN device -- python-can (udp_multicast by default, needs no hardware), for bridging
  to an external test-ECU process or real hardware.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from transport.device import CanDeviceConfig
from transport.lakebase import (
    CATCHUP_LOOKBACK_SECONDS,
    LakebaseConfig,
    SeenIds,
    accepts,
    catchup_sql,
    ensure_schema,
    insert_and_notify_sql,
    insert_params,
    notify_channel,
    select_by_id_sql,
)
from transport.lakebase import connect as _lakebase_connect
from transport.lakebase import sql as _sql
from transport.zerobus import ZerobusConfig, ZerobusStream

from bench.frame import CAN, CAN_FD, DIR_RX, FRAME_COLUMNS, Frame


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


# ── Loopback: in-process, deterministic ─────────────────────────────────────────

# Frames retained per segment for late subscribers, mirroring the catch-up the Lakebase
# channel performs on connect -- without this an Ecu that starts polling after a frame
# was already sent would behave differently depending on which transport it used.
BACKLOG_LIMIT = 10_000

_segments: dict[str, _Segment] = {}
_segments_lock = threading.Lock()


class _Segment:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.receivers: list[LoopbackRx] = []
        self.backlog: list[Frame] = []


def _segment(name: str) -> _Segment:
    with _segments_lock:
        return _segments.setdefault(name, _Segment())


def reset() -> None:
    """Drop every loopback segment and its attached receivers.

    Intended for test isolation; segments are otherwise process-global so that two
    channels constructed independently with the same table name find each other.
    """
    with _segments_lock:
        _segments.clear()


class LoopbackTx:
    """Transmit side of an in-process channel. Delivery is synchronous inside send()."""

    def __init__(self, *, table: str) -> None:
        self._segment = _segment(table)
        self._closed = False

    def send(self, frame: Frame) -> None:
        if self._closed:
            raise RuntimeError("loopback channel tx is closed")
        with self._segment.lock:
            self._segment.backlog.append(frame)
            del self._segment.backlog[:-BACKLOG_LIMIT]
            receivers = list(self._segment.receivers)
        for rx in receivers:
            rx._offer(frame)

    def flush(self) -> None:
        pass  # send() has already delivered

    def close(self) -> None:
        self._closed = True


class LoopbackRx:
    """Receive side of an in-process channel, filtered to this `run_id` and optionally
    `can_ids`/`message_types`.
    """

    def __init__(
        self,
        *,
        table: str,
        run_id: str,
        can_ids: Sequence[int] | None = None,
        message_types: Sequence[str] | None = None,
    ) -> None:
        self._run_id = run_id
        self._can_ids = set(can_ids) if can_ids else None
        self._message_types = set(message_types) if message_types else None
        self._queue: queue.Queue[Frame] = queue.Queue()
        self._segment = _segment(table)
        # Attach and replay under one lock hold, so a concurrent send is either in the
        # backlog we replay or fanned out to us, never both and never neither.
        with self._segment.lock:
            backlog = list(self._segment.backlog)
            self._segment.receivers.append(self)
        for frame in backlog:
            self._offer(frame)

    def _offer(self, frame: Frame) -> None:
        if frame.run_id != self._run_id:
            return
        if accepts(frame, can_ids=self._can_ids, message_types=self._message_types):
            self._queue.put(frame)

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        try:
            frames.append(self._queue.get(timeout=timeout))
        except queue.Empty:
            return frames
        while True:
            try:
                frames.append(self._queue.get_nowait())
            except queue.Empty:
                return frames

    def close(self) -> None:
        with self._segment.lock:
            if self in self._segment.receivers:
                self._segment.receivers.remove(self)


# ── Lakebase: minimal, non-batched ───────────────────────────────────────────────


class LakebaseTx:
    """Transmit side of a Lakebase channel: one synchronous INSERT+NOTIFY per send()."""

    def __init__(self, *, config: LakebaseConfig, create_schema: bool = True) -> None:
        self._config = config
        self._table = config.table
        self._channel = notify_channel(config.table)
        self._conn = _lakebase_connect(config.profile, config.endpoint_name, config.dbname)
        if create_schema:
            ensure_schema(self._conn, config.table)
        self._stmt = _sql(insert_and_notify_sql(config.table))
        self._closed = False

    def send(self, frame: Frame) -> None:
        if self._closed:
            raise RuntimeError(f"lakebase channel {self._table!r} is closed")
        params = insert_params(frame, self._channel)
        try:
            self._conn.execute(self._stmt, params)
        except Exception as exc:
            # One reconnect covers the connection drops a long run inevitably sees: an
            # idle timeout, or Lakebase scaling its compute to zero and back.
            print(f"[bench] lakebase tx {self._table}: {exc!r}; reconnecting and retrying once", flush=True)
            self._conn = _lakebase_connect(self._config.profile, self._config.endpoint_name, self._config.dbname)
            self._conn.execute(self._stmt, params)

    def flush(self) -> None:
        pass  # send() is already synchronous

    def close(self) -> None:
        self._closed = True
        try:
            self._conn.close()
        except Exception:
            pass


class LakebaseRx:
    """Receive side of a Lakebase channel: LISTEN on a background thread, with a
    catch-up SELECT on every (re)connect and id-based dedup, since NOTIFY is
    fire-and-forget and can drop or duplicate around a reconnect.
    """

    def __init__(
        self,
        *,
        config: LakebaseConfig,
        run_id: str,
        can_ids: Sequence[int] | None = None,
        message_types: Sequence[str] | None = None,
        create_schema: bool = True,
    ) -> None:
        self._config = config
        self._table = config.table
        self._channel = notify_channel(config.table)
        self._run_id = run_id
        self._can_ids = set(can_ids) if can_ids else None
        self._message_types = set(message_types) if message_types else None
        self._queue: queue.Queue[Frame] = queue.Queue()
        self._seen = SeenIds()
        self._watermark = 0
        self._stop = threading.Event()
        self._conn = None
        if create_schema:
            # Create the table before the listener thread can race an unprovisioned
            # database: the transmitting Ecu may not have started yet.
            conn = _lakebase_connect(config.profile, config.endpoint_name, config.dbname)
            try:
                ensure_schema(conn, config.table)
            finally:
                conn.close()
        self._thread = threading.Thread(target=self._run, name=f"lakebase-rx-{config.table}", daemon=True)
        self._thread.start()

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        try:
            frames.append(self._queue.get(timeout=timeout))
        except queue.Empty:
            return frames
        while True:
            try:
                frames.append(self._queue.get_nowait())
            except queue.Empty:
                return frames

    def close(self) -> None:
        self._stop.set()
        if self._conn is not None:
            # Closing the connection unblocks the notifies() iterator immediately
            # rather than leaving the thread parked until the next notification.
            try:
                self._conn.close()
            except Exception:
                pass
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._conn = _lakebase_connect(self._config.profile, self._config.endpoint_name, self._config.dbname)
                # LISTEN before catching up: a notification arriving during the catch-up
                # query is then queued by the server rather than lost, and any duplicate
                # it causes is absorbed by the id dedup.
                self._conn.execute(_sql(f'LISTEN "{self._channel}"'))
                self._catch_up(self._conn)
                for notify in self._conn.notifies():
                    self._on_notify(self._conn, notify.payload)
                    if self._stop.is_set():
                        return
            except Exception as exc:
                if self._stop.is_set():
                    return
                print(f"[bench] lakebase rx {self._table}: {exc!r}; reconnecting", flush=True)
                self._stop.wait(1.0)

    def _catch_up(self, conn) -> None:
        cur = conn.execute(_sql(catchup_sql(self._table)), (self._run_id, self._watermark, CATCHUP_LOOKBACK_SECONDS))
        self._emit(cur.fetchall())

    def _on_notify(self, conn, payload: str) -> None:
        row_id = int(payload)
        cur = conn.execute(_sql(select_by_id_sql(self._table)), (self._run_id, row_id))
        self._emit(cur.fetchall())

    def _emit(self, rows: Sequence[Sequence]) -> None:
        for row in rows:
            row_id = row[0]
            if not self._seen.add_if_new(row_id):
                continue
            frame = Frame.from_row(dict(zip(FRAME_COLUMNS, row[1:])))
            if accepts(frame, can_ids=self._can_ids, message_types=self._message_types):
                self._queue.put(frame)
            self._watermark = max(self._watermark, row_id)


# ── Zerobus: Ingest-only sink ────────────────────────────────────────────────────


class ZerobusTx:
    """Transmit side of a Zerobus channel: fire-and-forget protobuf ingest into a
    Delta table.
    """

    def __init__(self, *, config: ZerobusConfig, default_catalog: str = "main", default_schema: str = "blf") -> None:
        self._stream = ZerobusStream(config, default_catalog=default_catalog, default_schema=default_schema)

    def send(self, frame: Frame) -> None:
        self._stream.record(frame)

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        self._stream.close()


class ZerobusRx:
    """Zerobus is Ingest-only -- there is no subscribe/receive API. Kept so
    `Bus.connect()`'s dispatch stays total; poll() always raises. Use a Lakebase or
    loopback channel for Ecu-to-Ecu receive.
    """

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        raise NotImplementedError(
            "Zerobus is Ingest-only (no subscribe/receive API); use a Lakebase or"
            " loopback channel for Ecu-to-Ecu receive."
        )

    def close(self) -> None:
        pass


# ── CAN device: python-can, for bridging to an external process or hardware ──────


def _open_can_bus(config: CanDeviceConfig):
    import can

    kwargs: dict = {
        "interface": config.interface,
        "channel": config.channel,
        "receive_own_messages": config.receive_own_messages,
    }
    if config.bitrate is not None:
        kwargs["bitrate"] = config.bitrate
    if config.port is not None:
        kwargs["port"] = config.port
    if config.fd is not None:
        kwargs["fd"] = config.fd
    return can.interface.Bus(**kwargs)


class CanDeviceTx:
    """Transmit side of a CAN device channel. CAN-only: raises on an Ethernet frame."""

    def __init__(self, *, config: CanDeviceConfig) -> None:
        self._bus = _open_can_bus(config)

    def send(self, frame: Frame) -> None:
        import can

        if not frame.is_can or frame.can_id is None:
            raise ValueError(f"CAN device channel cannot send message_type={frame.message_type!r}")
        # python-can requires dlc == len(data) for non-remote frames; a CAN-FD dlc is a
        # code (e.g. 15 -> 64 bytes) rather than a byte count, so use the byte length
        # instead of the frame's own dlc field when is_fd (see transport can_io history).
        dlc = len(frame.data) if frame.is_fd else (frame.dlc if frame.dlc is not None else len(frame.data))
        self._bus.send(
            can.Message(
                arbitration_id=frame.can_id,
                is_extended_id=bool(frame.is_ext_id),
                is_remote_frame=bool(frame.rtr),
                is_fd=bool(frame.is_fd),
                dlc=dlc,
                data=frame.data,
            )
        )

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self._bus.shutdown()


class CanDeviceRx:
    """Receive side of a CAN device channel. Stamps received messages into Frames using
    `channel` as the BLF channel number (distinct from `config.channel`, which is the
    interface's own multicast address / channel name).
    """

    def __init__(
        self,
        *,
        config: CanDeviceConfig,
        run_id: str,
        source_file: str,
        channel: int,
        run_epoch_ns: int,
    ) -> None:
        import can

        self._bus = _open_can_bus(config)
        self._reader = can.BufferedReader()
        self._notifier = can.Notifier(self._bus, [self._reader])
        self._run_id = run_id
        self._source_file = source_file
        self._channel = channel
        self._run_epoch_ns = run_epoch_ns

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        frames: list[Frame] = []
        msg = self._reader.get_message(timeout=timeout)
        while msg is not None:
            frames.append(self._to_frame(msg))
            msg = self._reader.get_message(timeout=0)
        return frames

    def _to_frame(self, msg) -> Frame:
        # msg.timestamp is float seconds since the epoch; the udp_multicast backend
        # fills it from the kernel's SO_TIMESTAMPNS control message. Only accurate to
        # ~240ns at present-day epoch magnitudes (float64 resolution), far below CAN
        # frame spacing, so it does not affect ordering.
        observed_ns = int(msg.timestamp * 1e9)
        return Frame(
            run_id=self._run_id,
            source_file=self._source_file,
            message_type=CAN_FD if msg.is_fd else CAN,
            timestamp_ns=max(0, observed_ns - self._run_epoch_ns),
            observed_ns=observed_ns,
            channel=self._channel,
            dir=DIR_RX,
            data=bytes(msg.data),
            can_id=int(msg.arbitration_id),
            is_ext_id=bool(msg.is_extended_id),
            rtr=bool(msg.is_remote_frame),
            dlc=int(msg.dlc),
            is_fd=bool(msg.is_fd),
        )

    def close(self) -> None:
        self._notifier.stop()
        self._reader.stop()
        self._bus.shutdown()
