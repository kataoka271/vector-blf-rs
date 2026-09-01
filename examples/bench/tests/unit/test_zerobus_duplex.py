"""Offline tests for transport/zerobus_duplex.py: that the bus really is bidirectional,
the catch-up query's text and parameters, the watermark/dedup behaviour that makes a
re-scanned overlap harmless, the poll-interval rate limit that keeps an Ecu drain loop off
the warehouse, and the reconnect on a failing query.

The receive side is driven through a fake connection and the transmit side through a fake
ZerobusTx, so nothing here needs a SQL warehouse or a Zerobus endpoint; the transport's
own SQL is exercised as text rather than against a real table.
"""

from __future__ import annotations

from typing import Any

import pytest
from bench.bus import ZerobusDuplex
from bench.frame import FRAME_COLUMNS, make_can_frame
from pydantic import ValidationError
from transport.zerobus import ZerobusConfig
from transport.zerobus_duplex import ZerobusDuplexConfig, ZerobusDuplexRx, select_sql

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000

INGEST = ZerobusConfig(
    catalog="main",
    schema="blf",
    table="blf_testbench_frames",
    workspace_id="1234567890",
    region="us-east-2",
    service_principal_id="1122334455",
)
CONFIG = ZerobusDuplexConfig.from_zerobus(INGEST, poll_interval_s=0.0001, lookback_s=1.0)


def _row(*, can_id: int, observed_ns: int) -> tuple[Any, ...]:
    frame = make_can_frame(
        run_id=RUN_ID,
        source_file=f"testbench/{RUN_ID}.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=can_id,
        data=b"\x01\x02",
        observed_ns=observed_ns,
    )
    row = frame.to_row()
    return tuple(row[name] for name in FRAME_COLUMNS)


class _FakeCursor:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, statement: str, params: list[Any]) -> None:
        self._conn.calls.append((statement, list(params)))
        if self._conn.fail_once:
            self._conn.fail_once = False
            raise RuntimeError("connection closed by server")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._conn.rows.pop(0) if self._conn.rows else []


class _FakeConnection:
    def __init__(self, rows: list[list[tuple[Any, ...]]], *, fail_once: bool = False) -> None:
        self.rows = rows
        self.fail_once = fail_once
        self.calls: list[tuple[str, list[Any]]] = []
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def close(self) -> None:
        self.closed = True


def _rx(
    rows: list[list[tuple[Any, ...]]], *, fail_first_query: bool = False
) -> tuple[ZerobusDuplexRx, list[_FakeConnection]]:
    """An rx whose every connection is a fresh _FakeConnection over the same row batches,
    plus the list those connections are recorded in (one entry per connect). Only the
    first connection fails, so a retry on a second one succeeds.
    """
    conns: list[_FakeConnection] = []

    def connect_fn(config: ZerobusDuplexConfig) -> _FakeConnection:
        conn = _FakeConnection(rows, fail_once=fail_first_query and not conns)
        conns.append(conn)
        return conn

    return ZerobusDuplexRx(config=CONFIG, run_id=RUN_ID, connect_fn=connect_fn), conns


def test_select_sql_names_every_frame_column_and_filters_on_run_and_watermark():
    statement = select_sql(CONFIG)
    for name in FRAME_COLUMNS:
        assert f"`{name}`" in statement
    assert "`main`.`blf`.`blf_testbench_frames`" in statement
    assert "WHERE run_id = ? AND observed_ns >= ?" in statement
    assert statement.endswith("ORDER BY observed_ns")


def test_poll_returns_frames_and_advances_the_watermark():
    rx, conns = _rx([[_row(can_id=0x100, observed_ns=EPOCH_NS)]])

    frames = rx.poll(timeout=1.0)

    assert [frame.can_id for frame in frames] == [0x100]
    statement, params = conns[0].calls[0]
    assert params == [RUN_ID, 0]
    # Second query starts one lookback below the frame just seen, not at 0.
    assert rx.poll(timeout=0.05) == []
    assert conns[0].calls[1][1] == [RUN_ID, EPOCH_NS - 1_000_000_000]


def test_a_row_returned_twice_by_the_lookback_overlap_is_delivered_once():
    row = _row(can_id=0x100, observed_ns=EPOCH_NS)
    # Every batch re-includes the first row, as the lookback overlap does against a real table.
    rx, _ = _rx([[row], [row], [row, _row(can_id=0x101, observed_ns=EPOCH_NS + 1)]])

    delivered = [frame.can_id for _ in range(3) for frame in rx.poll(timeout=1.0)]

    assert delivered == [0x100, 0x101]


def test_poll_does_not_query_again_before_the_interval_elapses():
    config = ZerobusDuplexConfig.from_zerobus(INGEST, poll_interval_s=30.0)
    conns: list[_FakeConnection] = []

    def connect_fn(_config: ZerobusDuplexConfig) -> _FakeConnection:
        conn = _FakeConnection([[_row(can_id=0x100, observed_ns=EPOCH_NS)]])
        conns.append(conn)
        return conn

    rx = ZerobusDuplexRx(config=config, run_id=RUN_ID, connect_fn=connect_fn)
    assert len(rx.poll(timeout=1.0)) == 1
    # An Ecu's drain loop polls every 50 ms; the interval, not the loop, sets the query rate.
    for _ in range(5):
        assert rx.poll(timeout=0.01) == []
    assert len(conns[0].calls) == 1


def test_a_failing_query_reconnects_once_and_retries():
    rx, conns = _rx([[_row(can_id=0x100, observed_ns=EPOCH_NS)]], fail_first_query=True)

    frames = rx.poll(timeout=1.0)

    assert [frame.can_id for frame in frames] == [0x100]
    assert len(conns) == 2
    assert conns[0].closed


def test_filters_reject_a_can_id_the_receiver_did_not_ask_for():
    conns: list[_FakeConnection] = []

    def connect_fn(_config: ZerobusDuplexConfig) -> _FakeConnection:
        conn = _FakeConnection([[_row(can_id=0x100, observed_ns=EPOCH_NS), _row(can_id=0x101, observed_ns=EPOCH_NS)]])
        conns.append(conn)
        return conn

    rx = ZerobusDuplexRx(config=CONFIG, run_id=RUN_ID, can_ids=[0x101], connect_fn=connect_fn)

    assert [frame.can_id for frame in rx.poll(timeout=1.0)] == [0x101]


def test_from_zerobus_widens_an_ingest_config_without_restating_it():
    ingest = ZerobusConfig(**{**INGEST.model_dump(), "profile": "bench"})

    config = ZerobusDuplexConfig.from_zerobus(ingest, poll_interval_s=5.0)

    assert config.qualified_table == "`main`.`blf`.`blf_testbench_frames`"
    # Every ingest field survives, so one config drives both directions.
    assert config.model_dump(include=set(ZerobusConfig.model_fields)) == ingest.model_dump()
    assert config.poll_interval_s == 5.0


def test_rejects_a_non_bare_name_and_a_non_positive_interval():
    with pytest.raises(ValidationError):
        ZerobusDuplexConfig.from_zerobus(ZerobusConfig(**{**INGEST.model_dump(), "table": "a.b"}))
    with pytest.raises(ValidationError):
        ZerobusDuplexConfig.from_zerobus(INGEST, poll_interval_s=0)


def test_the_bus_transmits_over_zerobus_ingest_and_receives_by_query(monkeypatch):
    sent: list[ZerobusDuplexConfig] = []

    class _FakeZerobusTx:
        def __init__(self, *, config: ZerobusDuplexConfig) -> None:
            sent.append(config)

    monkeypatch.setattr("transport.zerobus_duplex.ZerobusTx", _FakeZerobusTx)
    bus = ZerobusDuplex(CONFIG, name="link")

    assert isinstance(bus.open_tx(), _FakeZerobusTx)
    # The ingest side gets the config unchanged -- a ZerobusDuplexConfig is a ZerobusConfig.
    assert sent == [CONFIG]
    assert isinstance(bus.open_rx(run_id=RUN_ID), ZerobusDuplexRx)
