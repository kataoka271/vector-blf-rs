"""Offline tests for transport/zerobus.py: the protobuf record's contract with the frame
model, the Ingest-only receive side, the credential requirement, and the durability
policy -- what close() does when the server never acknowledged everything.

The policy tests drive ZerobusStream through a fake StreamFactory, so they need no gRPC
endpoint; `online/test_zerobus.py` exercises the same path against the real one. The fake
stream refuses `get_unacked_records()` while open, as the real one does -- that is the
constraint that forces the accounting to happen at close() rather than at flush().
"""

from __future__ import annotations

import builtins
import time
from types import SimpleNamespace
from typing import Any

import pytest
from bench.bus import Zerobus
from bench.frame import FRAME_COLUMNS, make_can_frame
from transport.zerobus import (
    _SECRET_LIFETIME_S,
    _SECRET_ROTATION_INTERVAL_S,
    ZerobusConfig,
    ZerobusRx,
    ZerobusStream,
    _cache_secret,
    _SdkStreamFactory,
    load_record_pb2,
)

RUN_ID = "run_001"
EPOCH_NS = 1_700_000_000_000_000_000

CONFIG = ZerobusConfig(
    catalog="main",
    schema="blf",
    table="testbench_frames",
    workspace_id="1234567890",
    region="us-east-2",
    service_principal_id="1122334455",
)


@pytest.fixture(autouse=True)
def _isolated_secret_cache(tmp_path, monkeypatch):
    # Every test gets its own empty cache directory, so a real ~/.cache entry left by a
    # previous run (or another test) never leaks into what a test observes.
    monkeypatch.setattr("transport.zerobus._SECRET_CACHE_DIR", tmp_path / "secret-cache")


class _CountingSecretsProxy:
    """Stands in for the SDK's ServicePrincipalSecretsProxyAPI, counting calls so a test
    can assert whether a rotation happened.
    """

    def __init__(self) -> None:
        self.create_calls = 0
        self.delete_calls = 0
        self.lifetimes: builtins.list[str | None] = []
        self._existing_secret_id: str | None = None

    def list(self, service_principal_id: str) -> builtins.list[Any]:
        return [SimpleNamespace(id=self._existing_secret_id)] if self._existing_secret_id else []

    def create(self, service_principal_id: str, *, lifetime: str | None = None) -> SimpleNamespace:
        self.create_calls += 1
        self.lifetimes.append(lifetime)
        self._existing_secret_id = f"secret-{self.create_calls}"
        return SimpleNamespace(secret=f"plaintext-{self.create_calls}")

    def delete(self, service_principal_id: str, secret_id: str) -> None:
        self.delete_calls += 1
        self._existing_secret_id = None


def _fake_workspace_client_cls(secrets_proxy: _CountingSecretsProxy):
    class _ServicePrincipals:
        def get(self, service_principal_id: str) -> SimpleNamespace:
            return SimpleNamespace(id=service_principal_id, application_id="an-app-id")

    class _FakeWorkspaceClient:
        def __init__(self, **kwargs: Any) -> None:
            self.service_principals = _ServicePrincipals()
            self.service_principal_secrets_proxy = secrets_proxy
            self.config = SimpleNamespace(host="https://example.cloud.databricks.com")

    return _FakeWorkspaceClient


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


class FakeStream:
    """Stands in for the SDK's ZerobusStream.

    `unacked` is how many records the SDK still holds unacknowledged, which -- as in the
    real SDK -- can only be asked once the stream is closed. `swallows` is the nastier
    case: records the stream accepts and drops without ever holding them, which is what
    a broken Zerobus stream does and what `get_unacked_records()` cannot report.
    """

    def __init__(self, *, unacked: int = 0, ingest_fails: bool = False, swallows: bool = False) -> None:
        self.records: list[Any] = []
        self.flushes = 0
        self.closed = False
        self.unacked = unacked
        self.ingest_fails = ingest_fails
        self.swallows = swallows

    def ingest_record_nowait(self, record) -> None:
        if self.ingest_fails:
            raise RuntimeError("stream closed")
        if not self.swallows:
            self.records.append(record)

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closed = True

    def get_unacked_records(self):
        if not self.closed:
            raise RuntimeError("Cannot get unacked records from an active stream")
        return [b"record"] * self.unacked


class FakeFactory:
    """Hands out `streams` in order, recording how each one was obtained.

    `acked` counts what the streams actually kept, standing in for the SDK's ack
    callback -- which, like this, counts one per record across recreations.
    """

    def __init__(self, *streams: FakeStream) -> None:
        self._streams = list(streams)
        self._handed_out: list[FakeStream] = []
        self.recreated_from: list[FakeStream] = []

    @property
    def acked(self) -> int:
        return sum(len(stream.records) for stream in self._handed_out)

    def create(self) -> FakeStream:
        return self._next()

    def recreate(self, old: FakeStream) -> FakeStream:
        self.recreated_from.append(old)
        replacement = self._next()
        # The real recreate_stream() re-ingests what the old stream never got acked.
        replacement.records.extend([b"record"] * old.unacked)
        return replacement

    def _next(self) -> FakeStream:
        stream = self._streams.pop(0)
        self._handed_out.append(stream)
        return stream

    def build_record(self, frame):
        return frame


def test_close_returns_quietly_when_every_record_was_acknowledged():
    stream = FakeStream()
    factory = FakeFactory(stream)
    zerobus = ZerobusStream(CONFIG, factory=factory)

    zerobus.record(_frame())
    zerobus.flush()
    zerobus.close()

    assert stream.closed
    assert factory.recreated_from == []


def test_close_resends_records_the_server_never_acknowledged():
    lost, resent = FakeStream(unacked=2), FakeStream()
    factory = FakeFactory(lost, resent)
    zerobus = ZerobusStream(CONFIG, factory=factory)

    for _ in range(2):
        zerobus.record(_frame())
    zerobus.close()

    # Recreating from the broken stream is what carries its unacknowledged records over;
    # a fresh create() would start empty and drop exactly those records.
    assert factory.recreated_from == [lost]
    assert resent.closed


def test_close_raises_when_records_are_lost_for_good():
    lost, still_lost = FakeStream(unacked=3), FakeStream(unacked=1)
    zerobus = ZerobusStream(CONFIG, factory=FakeFactory(lost, still_lost))

    for _ in range(3):
        zerobus.record(_frame())
    # Silence here is the failure this guards against: the frame is gone from a Delta
    # table nobody will notice a hole in until much later.
    with pytest.raises(RuntimeError, match="1 record"):
        zerobus.close()


def test_close_catches_records_the_stream_swallowed_without_holding_them():
    # The failure mode a broken Zerobus stream actually has: the ingest call accepts the
    # record, logs "Stream closed", and drops it -- so the SDK reports nothing
    # unacknowledged and only the ack count is short.
    swallowed = FakeStream(swallows=True)
    zerobus = ZerobusStream(CONFIG, factory=FakeFactory(swallowed, FakeStream(swallows=True)))

    for _ in range(2):
        zerobus.record(_frame())
    with pytest.raises(RuntimeError, match="2 record"):
        zerobus.close()


def test_a_failed_ingest_recreates_the_stream_rather_than_starting_an_empty_one():
    broken, replacement = FakeStream(ingest_fails=True), FakeStream()
    factory = FakeFactory(broken, replacement)
    zerobus = ZerobusStream(CONFIG, factory=factory)

    zerobus.record(_frame())

    assert factory.recreated_from == [broken]
    assert len(replacement.records) == 1


def test_a_rotation_that_yields_no_secret_names_the_service_principal(monkeypatch):
    # _SdkStreamFactory has no ambient-credential fallback -- if secret rotation itself
    # does not yield a usable client id/secret, the RuntimeError must name the service
    # principal that failed to rotate, since that's the only thing left to check.
    class _RotatedSecret:
        id = "secret-1"
        secret = ""

    class _ServicePrincipal:
        id = CONFIG.service_principal_id
        application_id = "an-app-id"

    class _ServicePrincipals:
        def get(self, service_principal_id: str) -> _ServicePrincipal:
            assert service_principal_id == CONFIG.service_principal_id
            return _ServicePrincipal()

    class _SecretsProxy:
        def list(self, service_principal_id: str) -> builtins.list[Any]:
            return []

        def create(self, service_principal_id: str, *, lifetime: str | None = None) -> _RotatedSecret:
            return _RotatedSecret()

        def delete(self, service_principal_id: str, secret_id: str) -> None:
            raise AssertionError("nothing to delete: list() returned no existing secrets")

    class _FakeWorkspaceClient:
        def __init__(self, **kwargs: Any) -> None:
            self.service_principals = _ServicePrincipals()
            self.service_principal_secrets_proxy = _SecretsProxy()

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _FakeWorkspaceClient)
    with pytest.raises(RuntimeError, match=CONFIG.service_principal_id):
        ZerobusStream(CONFIG)


def test_a_rotated_secret_is_created_with_the_configured_server_side_lifetime(monkeypatch):
    secrets_proxy = _CountingSecretsProxy()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _fake_workspace_client_cls(secrets_proxy))

    _SdkStreamFactory(CONFIG)

    assert secrets_proxy.lifetimes == [f"{_SECRET_LIFETIME_S}s"]


def test_a_second_factory_within_the_rotation_interval_reuses_the_cached_secret(monkeypatch):
    secrets_proxy = _CountingSecretsProxy()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _fake_workspace_client_cls(secrets_proxy))

    first = _SdkStreamFactory(CONFIG)
    second = _SdkStreamFactory(CONFIG)

    # One rotation serves both factories -- the second construction found a fresh enough
    # cache entry and never called the secrets proxy at all.
    assert secrets_proxy.create_calls == 1
    assert (second._client_id, second._client_secret) == (first._client_id, first._client_secret)


def test_a_factory_after_the_rotation_interval_rotates_again(monkeypatch):
    secrets_proxy = _CountingSecretsProxy()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", _fake_workspace_client_cls(secrets_proxy))

    _SdkStreamFactory(CONFIG)
    _cache_secret(CONFIG.service_principal_id, "stale-id", "stale-secret")
    monkeypatch.setattr(
        "transport.zerobus.time", SimpleNamespace(time=lambda: time.time() + _SECRET_ROTATION_INTERVAL_S)
    )

    second = _SdkStreamFactory(CONFIG)

    assert secrets_proxy.create_calls == 2
    assert secrets_proxy.delete_calls == 1
    assert (second._client_id, second._client_secret) != ("stale-id", "stale-secret")
