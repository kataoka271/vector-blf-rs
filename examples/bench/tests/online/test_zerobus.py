"""Online tests for the Zerobus channel: real service-principal authentication against
the real Ingest endpoint, and rows actually landing in the target Delta table.

Zerobus is Ingest-only, so there is no receive side to read back with -- the write is
verified with a SQL query through a warehouse, which is also how the rows are cleaned up
afterwards. Both are keyed on this run's own run_id, so nothing else in the table is
touched.

The target table is `ZEROBUS_CATALOG.ZEROBUS_SCHEMA.<ZEROBUS_TABLE>` (default
`blf_testbench_frames`, the same table topologies/reference.py uploads to). It must
already exist as a MANAGED table whose columns match transport/record.proto.

The last test deliberately reproduces the SDK hazard the durability accounting in
`ZerobusStream.close()` exists for: a stream opened after an earlier one in the same
process was closed can have its records silently dropped. The plain write test runs
first, before anything in this process has opened a stream, so the two are not testing
the same path.
"""

from __future__ import annotations

import os
import time

import pytest
from bench.db import connect as sql_connect
from bench.frame import Frame, make_can_frame, make_eth_frame
from transport.zerobus import ZerobusConfig, ZerobusStream, ZerobusTx

pytestmark = pytest.mark.online

EPOCH_NS = 1_700_000_000_000_000_000
VISIBILITY_TIMEOUT_S = 120.0


@pytest.fixture(scope="module")
def zerobus_config(connection_config) -> ZerobusConfig:
    return connection_config.zerobus_config(table=os.environ.get("ZEROBUS_TABLE", "blf_testbench_frames"))


def _frames(run_id: str) -> list[Frame]:
    source_file = f"testbench/{run_id}.blf"
    return [
        make_can_frame(
            run_id=run_id,
            source_file=source_file,
            run_epoch_ns=EPOCH_NS,
            channel=1,
            can_id=0x310,
            data=b"\x01\x02\x03\x04\x05\x06\x07\x08",
        ),
        make_can_frame(
            run_id=run_id,
            source_file=source_file,
            run_epoch_ns=EPOCH_NS,
            channel=1,
            can_id=0x311,
            data=bytes(24),
            is_fd=True,
        ),
        make_eth_frame(
            run_id=run_id,
            source_file=source_file,
            run_epoch_ns=EPOCH_NS,
            channel=2,
            src_addr=bytes.fromhex("001122334455"),
            dst_addr=bytes.fromhex("66778899aabb"),
            ether_type=0x0800,
            data=b"payload",
        ),
    ]


def _query(verify_config, statement: str, params: list | None = None) -> list:
    with sql_connect(verify_config) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or None)
            return cur.fetchall()


def _table(config: ZerobusConfig) -> str:
    return f"{config.catalog}.{config.schema}.{config.table}"


def _close_reporting_loss(tx: ZerobusTx) -> RuntimeError | None:
    """Close `tx`, returning the loss it reported instead of raising it.

    Zerobus itself sometimes drops records (see the last test); the guarantee the
    transport gives is not that this never happens, but that it is never silent. Each
    test therefore checks the two consistent outcomes -- every record landed, or close()
    said so -- and treats a reported loss as an environment failure rather than a code
    one.
    """
    try:
        tx.close()
    except RuntimeError as exc:
        return exc
    return None


def _wait_for_rows(verify_config, table: str, run_id: str, expected: int) -> list:
    rows: list = []
    deadline = time.monotonic() + VISIBILITY_TIMEOUT_S
    while len(rows) < expected and time.monotonic() < deadline:
        rows = _query(
            verify_config,
            f"SELECT message_type, channel, can_id, dlc, is_fd, data, ether_type"
            f" FROM {table} WHERE run_id = ? ORDER BY timestamp_ns",
            [run_id],
        )
        if len(rows) < expected:
            time.sleep(2.0)
    return rows


def test_ingested_frames_land_in_the_delta_table(zerobus_config, verify_config, run_id):
    table = _table(zerobus_config)
    sent = _frames(run_id)
    try:
        tx = ZerobusTx(config=zerobus_config)
        for frame in sent:
            tx.send(frame)
        tx.flush()
        reported_loss = _close_reporting_loss(tx)

        rows = _wait_for_rows(verify_config, table, run_id, len(sent))
        assert len(rows) == len(sent) or reported_loss is not None, (
            f"only {len(rows)}/{len(sent)} row(s) visible after {VISIBILITY_TIMEOUT_S}s, and close() did not say so"
        )
        if reported_loss is not None:
            pytest.xfail(f"Zerobus dropped records and reported it: {reported_loss}")
        assert [row.message_type for row in rows] == ["CAN", "CAN_FD", "ETH"]
        assert [row.can_id for row in rows] == [0x310, 0x311, None]
        assert [row.is_fd for row in rows] == [False, True, None]
        # The payload survives protobuf `bytes` -> Delta BINARY unchanged.
        assert bytes(rows[0].data) == sent[0].data
        assert bytes(rows[2].data) == b"payload"
        # Protobuf drops the None-valued variant fields, so a CAN row leaves the Ethernet
        # columns null rather than writing a placeholder.
        assert rows[0].ether_type is None
        assert rows[2].ether_type == 0x0800
    finally:
        _query(verify_config, f"DELETE FROM {table} WHERE run_id = ?", [run_id])

    # The tier's own promise: a write test against a shared table takes its rows back out
    # again, so running it repeatedly does not accumulate anything.
    assert _query(verify_config, f"SELECT count(*) AS c FROM {table} WHERE run_id = ?", [run_id])[0].c == 0


def test_a_stream_authenticates_and_opens_against_the_real_endpoint(zerobus_config):
    # Zerobus needs OAuth service-principal credentials specifically, not the bearer
    # token the SQL connector uses, and mints a per-table UC token to open the stream.
    # Runs after the write test: closing this stream is what breaks the next one.
    stream = ZerobusStream(zerobus_config)
    stream.close()


def test_records_sent_after_an_earlier_stream_was_closed_still_land(zerobus_config, verify_config, run_id):
    """Deliberately reproduces the hazard the durability accounting exists for.

    A stream opened after an earlier one in the same process was closed sometimes has its
    records dropped by the SDK: the ingest call only logs "Stream closed" and raises
    nothing, and flush() then returns successfully. So the assertion is not that the
    records always survive -- that is not ours to promise -- but that a run never ends
    believing it uploaded frames that are not there.
    """
    table = _table(zerobus_config)
    sent = _frames(run_id)
    try:
        ZerobusStream(zerobus_config).close()

        tx = ZerobusTx(config=zerobus_config)
        for frame in sent:
            tx.send(frame)
        tx.flush()
        reported_loss = _close_reporting_loss(tx)

        rows = _wait_for_rows(verify_config, table, run_id, len(sent))
        assert len(rows) == len(sent) or reported_loss is not None, (
            f"only {len(rows)}/{len(sent)} row(s) survived the reopened stream, and close() did not say so"
        )
        if reported_loss is not None:
            pytest.xfail(f"Zerobus dropped records on the reopened stream and reported it: {reported_loss}")
    finally:
        _query(verify_config, f"DELETE FROM {table} WHERE run_id = ?", [run_id])
