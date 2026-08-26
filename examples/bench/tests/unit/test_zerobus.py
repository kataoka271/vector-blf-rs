"""Offline tests for transport/zerobus.py: the protobuf record's contract with the frame
model, the Ingest-only receive side, and the credential requirement. Nothing here opens a
gRPC stream -- `online/test_zerobus.py` does that against the real endpoint.
"""

from __future__ import annotations

import pytest
from bench.bus import Zerobus
from bench.frame import FRAME_COLUMNS, make_can_frame
from transport.zerobus import ZerobusConfig, ZerobusRx, ZerobusStream, load_record_pb2

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000

CONFIG = ZerobusConfig(
    catalog="main",
    schema="blf",
    table="testbench_frames",
    workspace_id="1234567890",
    region="us-east-2",
)


def _frame():
    return make_can_frame(
        run_id=RUN_ID,
        source_file="testbench/run_001.blf",
        run_epoch_ns=EPOCH_NS,
        channel=1,
        can_id=0x310,
        data=b"\x01\x02",
    )


def test_the_protobuf_record_has_exactly_the_frame_columns():
    # record.proto's fields must line up 1:1 with the target Delta table's columns, and
    # the Delta table's columns are FRAME_COLUMNS. A drift either way silently drops a
    # column on ingest, since Frame.to_row() is splatted straight into the constructor.
    pb = load_record_pb2()
    assert set(pb.TestbenchFrame.DESCRIPTOR.fields_by_name) == set(FRAME_COLUMNS)


def test_a_frame_row_is_accepted_verbatim_as_protobuf_constructor_kwargs():
    record = load_record_pb2().TestbenchFrame(**_frame().to_row())
    assert record.can_id == 0x310
    assert record.data == b"\x01\x02"
    # Protobuf's constructor skips None, so a CAN frame leaves the Ethernet fields unset
    # rather than writing nulls it would have to be taught about.
    assert not record.HasField("src_addr")
    assert not record.HasField("vlan_id")


def test_receive_is_refused_with_a_pointer_to_a_transport_that_can():
    with pytest.raises(NotImplementedError, match="Lakebase or"):
        ZerobusRx().poll()


def test_a_zerobus_bus_still_opens_an_rx_so_dispatch_stays_total():
    # Bus.open_rx() is total over registered transports; a send-only Zerobus leg is only
    # ever opened lazily by BusHandle, so this object exists but is never polled.
    assert isinstance(Zerobus(CONFIG, name="up").open_rx(run_id=RUN_ID), ZerobusRx)


def test_missing_client_credentials_name_the_variables_to_set(monkeypatch):
    class _NoCredentialsConfig:
        def __init__(self, **kwargs) -> None:
            self.host = "https://example.cloud.databricks.com"
            self.client_id = ""
            self.client_secret = ""

    monkeypatch.setattr("databricks.sdk.core.Config", _NoCredentialsConfig)
    with pytest.raises(RuntimeError, match="DATABRICKS_CLIENT_ID"):
        ZerobusStream(CONFIG)
