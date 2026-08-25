"""Online tests for the Lakebase channel: real OAuth authentication against a real
Lakebase endpoint, a real INSERT, and the frames coming back out.

The offline tier already checks the SQL text and the NOTIFY envelope
(`unit/test_lakebase_sql.py`); what only a real endpoint can show is that the credential
path works, that the DDL Postgres actually accepts round-trips a Frame unchanged through
BYTEA and BIGINT, and that both delivery paths -- an inline NOTIFY and the catch-up
SELECT a receiver runs on connect -- deliver.

Every test shares one bus table, created by the channel's own `ensure_schema` and dropped
at the end of the module; runs are kept apart by run_id.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pytest
from bench.frame import Frame, make_can_frame, make_eth_frame
from transport.lakebase import LakebaseConfig, LakebaseRx, LakebaseTx, connect, sql

pytestmark = pytest.mark.online

EPOCH_NS = 1_700_000_000_000_000_000
POLL_TIMEOUT_S = 30.0


@pytest.fixture(scope="module")
def bus_config(connection_config) -> Iterator[LakebaseConfig]:
    config = connection_config.lakebase_config(table=f"bench_online_{uuid.uuid4().hex[:12]}")
    yield config
    conn = connect(config.profile, config.endpoint_name, config.dbname)
    try:
        conn.execute(sql(f'DROP TABLE IF EXISTS "{config.table}"'))
    finally:
        conn.close()


def _can(run_id: str, can_id: int, *, data: bytes = b"\x01\x02\x03\x04") -> Frame:
    return make_can_frame(
        run_id=run_id,
        source_file=f"testbench/{run_id}.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=can_id,
        data=data,
        is_ext_id=True,
    )


def _eth(run_id: str, payload_len: int = 1500) -> Frame:
    return make_eth_frame(
        run_id=run_id,
        source_file=f"testbench/{run_id}.blf",
        run_epoch_ns=EPOCH_NS,
        channel=2,
        src_addr=bytes.fromhex("001122334455"),
        dst_addr=bytes.fromhex("66778899aabb"),
        ether_type=0x0800,
        data=bytes(range(256)) * (payload_len // 256) + bytes(payload_len % 256),
    )


def _receive(rx: LakebaseRx, count: int, timeout: float = POLL_TIMEOUT_S) -> list[Frame]:
    frames: list[Frame] = []
    deadline = time.monotonic() + timeout
    while len(frames) < count and time.monotonic() < deadline:
        frames += rx.poll(timeout=1.0)
    return frames


def test_connect_authenticates_and_opens_a_usable_session(connection_config):
    # The whole credential path in one call: resolve the endpoint host, resolve the
    # current user to a Postgres role, mint a database credential, connect over TLS.
    conn = connect(
        connection_config.lakebase_profile,
        connection_config.lakebase_endpoint,
        connection_config.lakebase_database,
    )
    try:
        assert conn.execute(sql("SELECT 1")).fetchone() == (1,)
    finally:
        conn.close()


def test_a_written_frame_is_delivered_to_a_listening_receiver(bus_config, run_id):
    rx = LakebaseRx(config=bus_config, run_id=run_id)
    tx = LakebaseTx(config=bus_config)
    try:
        sent = [_can(run_id, 0x310), _can(run_id, 0x311, data=bytes(range(8)))]
        for frame in sent:
            tx.send(frame)
        tx.flush()

        got = _receive(rx, len(sent))
        # Frame equality covers every column: the payload through BYTEA, can_id through
        # BIGINT, the flags through BOOLEAN, and both timestamps through BIGINT.
        assert got == sent
    finally:
        tx.close()
        rx.close()


def test_a_receiver_connecting_after_the_write_catches_up_and_ignores_other_runs(bus_config, run_id):
    # A second run writing to the same bus table needs its own transmitter: batches are
    # cut by arrival, so one Tx used for two runs would eventually put both in a batch,
    # and an envelope must carry exactly one run_id.
    other = LakebaseTx(config=bus_config)
    tx = LakebaseTx(config=bus_config)
    try:
        other.send(_can(f"{run_id}-other", 0x400))
        other.flush()
        tx.send(_can(run_id, 0x310))
        tx.send(_can(run_id, 0x311))
        tx.flush()

        # Nothing is listening yet, so these rows can only arrive through the catch-up
        # SELECT that runs on connect -- the NOTIFY for them is long gone.
        rx = LakebaseRx(config=bus_config, run_id=run_id)
        try:
            got = _receive(rx, 2)
            assert [f.can_id for f in got] == [0x310, 0x311]
            assert all(f.run_id == run_id for f in got)
        finally:
            rx.close()
    finally:
        tx.close()
        other.close()


def test_a_batch_too_large_to_inline_is_fetched_by_id(bus_config, run_id):
    # Five full-size Ethernet frames exceed Postgres's 8000-byte NOTIFY cap, so the
    # notification carries only row ids and the receiver has to fetch them itself.
    rx = LakebaseRx(config=bus_config, run_id=run_id)
    tx = LakebaseTx(config=bus_config)
    try:
        sent = [_eth(run_id) for _ in range(5)]
        for frame in sent:
            tx.send(frame)
        tx.flush()

        got = _receive(rx, len(sent))
        assert len(got) == len(sent)
        assert all(f.message_type == "ETH" and len(f.data) == 1500 for f in got)
        assert {f.src_addr for f in got} == {bytes.fromhex("001122334455")}
    finally:
        tx.close()
        rx.close()


def test_a_receiver_filtered_to_one_can_id_gets_only_that_frame(bus_config, run_id):
    rx = LakebaseRx(config=bus_config, run_id=run_id, can_ids=[0x310])
    tx = LakebaseTx(config=bus_config)
    try:
        tx.send(_can(run_id, 0x311))
        tx.send(_can(run_id, 0x310))
        tx.flush()

        got = _receive(rx, 1)
        assert [f.can_id for f in got] == [0x310]
    finally:
        tx.close()
        rx.close()
