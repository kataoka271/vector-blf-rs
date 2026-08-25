"""Zerobus channel: streams protobuf `TestbenchFrame` records into a Delta table over
Zerobus's gRPC endpoint, which the blf_ingestion DLT pipeline unions into blf_bronze
(see the `blf.testbench_frames_table` pipeline parameter).

Zerobus is Ingest-only -- there is no subscribe/receive API -- so this module only ever
backs the transmit side of a bus (`ZerobusTx`); `ZerobusRx.poll()` raises
NotImplementedError. Use Lakebase or loopback for ECU-to-ECU receive.

`databricks.sdk`/`zerobus.sdk` are imported lazily inside `_SdkStreamFactory.__init__`, not
at module level, so constructing a `ZerobusConfig` never requires either package to be
installed.
"""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys
import time
import warnings
from typing import TYPE_CHECKING, Annotated, Any, Protocol

from bench.frame import Frame
from pydantic import BaseModel, ConfigDict, StringConstraints, ValidationInfo, field_validator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bench.bus import Bus

# Joined unquoted into `catalog.schema.table` for Zerobus, so a dot/backtick/space in a
# name would silently address a different table. Unity Catalog also allows a leading
# digit, unlike a bare SQL identifier.
_UC_NAME = re.compile(r"^[A-Za-z0-9_]+$")

# workspace_id and region are interpolated into the endpoint hostname (see
# ZerobusConfig.endpoint), so each has to be a single DNS label. This is what catches a
# whole URL or a `https://` prefix pasted into either one.
_HOST_LABEL = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$")

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# ZerobusConfig.schema shadows pydantic v1's deprecated BaseModel.schema(), hence the
# warning suppression. The name (Unity Catalog's "catalog.schema.table") is worth keeping;
# instance access still resolves to the field since a classmethod is a non-data descriptor.
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message='Field name "schema"', category=UserWarning)

    class ZerobusConfig(BaseModel):
        """Connection settings for one Zerobus Ingest stream into `catalog.schema.table`.

        `service_principal_id` names the service principal `_SdkStreamFactory` periodically
        rotates a fresh OAuth client id/secret for (see its docstring).

        Raises `pydantic.ValidationError` on a missing, blank, or unknown field, on a
        `catalog`/`schema`/`table` that is not a bare Unity Catalog name, or on a
        `workspace_id`/`region` that is not a single DNS label.
        """

        # Frozen because ZerobusStream holds one across stream recreations; extra="forbid"
        # so a misspelled key is an error rather than a silently ignored one.
        model_config = ConfigDict(frozen=True, extra="forbid")

        catalog: _NonEmpty
        schema: _NonEmpty
        table: _NonEmpty
        workspace_id: _NonEmpty
        region: _NonEmpty
        profile: _NonEmpty | None = None
        service_principal_id: _NonEmpty

        @field_validator("catalog", "schema", "table")
        @classmethod
        def _bare_unity_catalog_name(cls, value: str, info: ValidationInfo) -> str:
            if not _UC_NAME.match(value):
                raise ValueError(f"{info.field_name} must match {_UC_NAME.pattern!r}, got {value!r}")
            return value

        @field_validator("workspace_id", "region")
        @classmethod
        def _single_dns_label(cls, value: str, info: ValidationInfo) -> str:
            if not _HOST_LABEL.match(value):
                raise ValueError(f"{info.field_name} must match {_HOST_LABEL.pattern!r}, got {value!r}")
            return value

        @field_validator("profile", mode="before")
        @classmethod
        def _blank_profile_means_none(cls, value: Any) -> Any:
            # `ZEROBUS_PROFILE=` (set but empty) reaches ConnectionConfig as "" rather
            # than None; both mean "no profile", and ZerobusStream already collapses them.
            return None if isinstance(value, str) and not value.strip() else value

        @property
        def endpoint(self) -> str:
            return f"https://{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com"


def _generate_record_pb2() -> bool:
    """Run protoc over record.proto in place. Returns whether it succeeded."""
    package_dir = pathlib.Path(__file__).resolve().parent
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"--python_out={package_dir}",
                f"--proto_path={package_dir}",
                "record.proto",
            ],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, ImportError):
        return False
    return True


def load_record_pb2():
    """Import the protoc-generated record_pb2 module for transport/record.proto.

    Generated protobuf code is a build artifact tied to the protobuf runtime version, so
    it is not checked in; this builds it on first use when grpc_tools is installed.
    Raises RuntimeError naming the manual command when that is not possible.
    """
    # importlib, not a relative `from . import record_pb2`, so a type checker does not
    # try (and fail) to statically resolve a module that exists only after the build step.
    import importlib

    try:
        return importlib.import_module(".record_pb2", __package__)
    except ImportError:
        pass

    if _generate_record_pb2():
        try:
            return importlib.import_module(".record_pb2", __package__)
        except ImportError:
            pass
    raise RuntimeError(
        "record_pb2 not found and could not be generated; build it manually:\n"
        "  cd examples/bench/transport && uv run --group testing python -m grpc_tools.protoc"
        " --python_out=. --proto_path=. record.proto"
    )


def _make_ack_ledger():
    """Return an SDK AckCallback that counts server acknowledgements. One ledger is
    shared across a stream and its recreations, so a resent record's acknowledgement
    lands in the same count as the original send.
    """
    # Subclassed here, not at module level: the base class comes from the SDK, which this
    # module imports lazily (see the module docstring).
    from zerobus.sdk.sync.zerobus_sdk import AckCallback

    class _AckLedger(AckCallback):
        def __init__(self) -> None:
            super().__init__()
            self.acked = 0

        def on_ack(self, offset: int) -> None:
            self.acked += 1

        def on_error(self, offset: int, error_message: str) -> None:
            print(f"[bench] zerobus: record at offset {offset} rejected: {error_message}", flush=True)

    return _AckLedger()


# Databricks reveals a service-principal secret's plaintext only once, at creation, so
# rotating it on every stream construction is what forces _SdkStreamFactory to cache the
# plaintext locally rather than just re-deriving it from the SP each time.
_SECRET_CACHE_DIR = pathlib.Path(__file__).parent.parent / ".cache" / "blf-testbench-zerobus"
_SECRET_ROTATION_INTERVAL_S = 30 * 24 * 60 * 60
# A secret already in use is never proactively swapped out mid-stream, so its
# server-side lifetime must outlast one full local rotation interval, not just match it,
# or a stream started near the end of that interval could have its secret expire under it.
_SECRET_LIFETIME_S = 2 * _SECRET_ROTATION_INTERVAL_S


def _cached_secret(service_principal_id: str) -> tuple[str, str] | None:
    """Return the cached (client_id, client_secret) for `service_principal_id`, or None
    if there is no cache entry or it is older than _SECRET_ROTATION_INTERVAL_S.
    """
    path = _SECRET_CACHE_DIR / f"{service_principal_id}.json"
    try:
        data = json.loads(path.read_text())
        rotated_at, client_id, client_secret = data["rotated_at"], data["client_id"], data["client_secret"]
    except (OSError, json.JSONDecodeError, KeyError):
        return None
    if time.time() - rotated_at >= _SECRET_ROTATION_INTERVAL_S:
        return None
    return client_id, client_secret


def _cache_secret(service_principal_id: str, client_id: str, client_secret: str) -> None:
    _SECRET_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _SECRET_CACHE_DIR / f"{service_principal_id}.json"
    path.write_text(json.dumps({"client_id": client_id, "client_secret": client_secret, "rotated_at": time.time()}))


class StreamFactory(Protocol):
    """How ZerobusStream obtains streams, builds records, and learns what was acked.

    `recreate(old)` must carry `old`'s unacknowledged records over to the new stream,
    which is what makes recovery lossless, and `acked` must count one per record the
    server confirmed, across recreations.
    """

    @property
    def acked(self) -> int: ...

    def create(self): ...
    def recreate(self, old): ...
    def build_record(self, frame: Frame): ...


class _SdkStreamFactory:
    """The real thing: opens Zerobus Ingest streams into one `catalog.schema.table`.

    Credentials are OAuth service-principal client id/secret, not the bearer token the
    Databricks SQL connector uses. `config.service_principal_id`'s secret is rotated --
    deleting any existing ones and creating a fresh one with a server-side lifetime of
    _SECRET_LIFETIME_S -- at most once every _SECRET_ROTATION_INTERVAL_S; the plaintext is
    cached locally under _SECRET_CACHE_DIR between rotations, since Databricks reveals it
    only once, at creation. Nothing else (no `config.profile`-derived ambient credential,
    no environment variable) can supply it. Raises RuntimeError when neither the cache
    nor a fresh rotation yields a usable client id/secret.
    """

    def __init__(self, config: ZerobusConfig) -> None:
        from databricks.sdk import WorkspaceClient
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        if config.profile:
            w = WorkspaceClient(profile=config.profile, auth_type="oauth-m2m")
        else:
            w = WorkspaceClient(auth_type="oauth-m2m")

        cached = _cached_secret(config.service_principal_id)
        if cached is not None:
            client_id, client_secret = cached
        else:
            sp = w.service_principals.get(config.service_principal_id)
            assert sp.id is not None
            for secret in w.service_principal_secrets_proxy.list(sp.id):
                if secret.id:
                    w.service_principal_secrets_proxy.delete(sp.id, secret.id)
            secret = w.service_principal_secrets_proxy.create(sp.id, lifetime=f"{_SECRET_LIFETIME_S}s")
            client_id = sp.application_id
            client_secret = secret.secret
            if client_id and client_secret:
                _cache_secret(config.service_principal_id, client_id, client_secret)

        if not client_id or not client_secret:
            raise RuntimeError(
                "Zerobus Ingest could not rotate usable OAuth credentials for service"
                f" principal {config.service_principal_id!r}; check that it exists and"
                " that the identity behind ZerobusConfig.profile can manage its secrets."
            )
        self._pb = load_record_pb2()
        self.table = f"{config.catalog}.{config.schema}.{config.table}"
        self._sdk = ZerobusSdk(config.endpoint, w.config.host)
        self._client_id = client_id
        self._client_secret = client_secret
        self._table_properties = TableProperties(self.table, self._pb.TestbenchFrame.DESCRIPTOR)
        self._ledger = _make_ack_ledger()
        self._options = StreamConfigurationOptions(record_type=RecordType.PROTO, ack_callback=self._ledger)

    @property
    def acked(self) -> int:
        return self._ledger.acked

    def create(self):
        return self._sdk.create_stream(
            self._client_id,
            self._client_secret,
            self._table_properties,
            self._options,
        )

    def recreate(self, old):
        """Return a new stream re-ingesting whatever `old` never got acknowledged.

        `recreate_stream` requires a closed stream, so `old` is closed first; it may
        already be closed (that is the usual case here), which is why the failure is
        ignored.
        """
        try:
            old.close()
        except Exception:
            pass
        return self._sdk.recreate_stream(old)

    def build_record(self, frame: Frame):
        return self._pb.TestbenchFrame(**frame.to_row())


class ZerobusStream:
    """One Zerobus Ingest protobuf stream into `catalog.schema.table`, with the
    guarantee that a frame it accepted is never lost silently.

    `record()` is fire-and-forget; call `flush()` to bound the delay before rows become
    SQL-visible, and `close()` before exiting -- `close()` is where an unacknowledged
    record is caught, and it raises rather than returning quietly (see its docstring).
    """

    def __init__(self, config: ZerobusConfig, *, factory: StreamFactory | None = None) -> None:
        self._factory = factory or _SdkStreamFactory(config)
        self._table = f"{config.catalog}.{config.schema}.{config.table}"
        self._stream = self._factory.create()
        self._sent = 0

    def _recover(self):
        """Replace the current stream with one re-ingesting its unacknowledged records."""
        self._stream = self._factory.recreate(self._stream)
        return self._stream

    def _with_stream_retry(self, op):
        # Recreate the stream once on failure rather than silently losing the rest of the
        # run's frames. Must be recreate(), not create() -- a fresh stream would start with
        # an empty buffer, discarding the records in flight.
        try:
            return op()
        except Exception as exc:
            print(f"[bench] zerobus {self._table}: {exc!r}; recreating stream and retrying once", flush=True)
            self._recover()
            return op()

    def record(self, frame: Frame) -> None:
        """Ingest one frame, fire-and-forget. Protobuf's constructor drops the
        None-valued variant fields, so a CAN frame simply leaves the Ethernet columns
        unset.
        """
        self._with_stream_retry(lambda: self._stream.ingest_record_nowait(self._factory.build_record(frame)))
        self._sent += 1

    def flush(self) -> None:
        """Block until the server has acknowledged everything sent so far.

        This cannot on its own prove the records got there: a broken stream flushes
        without complaining, and acknowledgement callbacks lag flush() by design (a
        500-record flush returned with 460 counted, all 500 by close). The SDK's
        per-offset confirmation is unusable too (its Python wrapper omits a required
        argument), which is why close() is where every record gets accounted for.
        """
        self._with_stream_retry(lambda: self._stream.flush())

    def close(self) -> None:
        """Flush pending records, close the stream, and account for every record.

        A broken stream reports nothing on its own -- ingest raises nothing and flush()
        returns successfully -- so without this accounting a dropped frame would be
        silent. Anything missing is resent by recreating the stream; what is still
        missing after that raises RuntimeError.
        """
        missing = self._missing(self._flush_and_close(self._stream))
        if not missing:
            return
        print(
            f"[bench] zerobus {self._table}: {missing} record(s) unaccounted for; recreating stream to resend",
            flush=True,
        )
        remaining = self._missing(self._flush_and_close(self._recover()))
        if remaining:
            raise RuntimeError(f"zerobus {self._table}: {remaining} record(s) were never acknowledged and are lost")

    def _missing(self, unacked: int) -> int:
        """Return how many records are unaccounted for, on the more pessimistic of two
        independent counts: the SDK's own `unacked` (what it still holds) and the
        ledger's (what it dropped without ever holding). Both are only exact once the
        stream is closed.
        """
        return max(unacked, self._sent - self._factory.acked)

    @staticmethod
    def _flush_and_close(stream) -> int:
        """Flush and close `stream`, returning how many records it never got acked.

        A flush that raises is not fatal here: the point of closing is to find out what
        was lost, and `get_unacked_records()` only answers once the stream is closed.
        """
        try:
            stream.flush()
        except Exception as exc:
            print(f"[bench] zerobus: flush before close failed ({exc!r}); closing to count what was lost", flush=True)
        try:
            stream.close()
        except Exception:
            pass
        return sum(1 for _ in stream.get_unacked_records())


class ZerobusTx:
    """Transmit side of a Zerobus channel: fire-and-forget protobuf ingest into a
    Delta table.
    """

    def __init__(self, *, config: ZerobusConfig) -> None:
        self._stream = ZerobusStream(config)

    def send(self, frame: Frame) -> None:
        self._stream.record(frame)

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        self._stream.close()


class ZerobusRx:
    """Zerobus is Ingest-only -- there is no subscribe/receive API. Kept so
    `Bus`'s dispatch stays total; poll() always raises. Use a Lakebase or loopback
    channel for Ecu-to-Ecu receive.
    """

    def poll(self, timeout: float = 1.0) -> list[Frame]:
        raise NotImplementedError(
            "Zerobus is Ingest-only (no subscribe/receive API); use a Lakebase or"
            " loopback channel for Ecu-to-Ecu receive."
        )

    def close(self) -> None:
        pass


def open_tx(bus: Bus) -> ZerobusTx:
    """Registered as this transport's Bus.open_tx() -- see bench.bus.register_transport."""
    assert isinstance(bus.config, ZerobusConfig)
    return ZerobusTx(config=bus.config)


def open_rx(
    bus: Bus,
    *,
    run_id: str,
    can_ids: Sequence[int] | None = None,
    message_types: Sequence[str] | None = None,
    source_file: str | None = None,
    run_epoch_ns: int | None = None,
) -> ZerobusRx:
    """Registered as this transport's Bus.open_rx() -- see bench.bus.register_transport.
    Always returns a ZerobusRx, whose poll() raises -- see its docstring.
    """
    return ZerobusRx()
