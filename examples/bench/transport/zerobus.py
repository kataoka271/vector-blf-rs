"""Zerobus channel: streams protobuf `TestbenchFrame` records into a Delta table over
Zerobus's gRPC endpoint, which the blf_ingestion DLT pipeline unions into blf_bronze
(see the `blf.testbench_frames_table` pipeline parameter).

Zerobus is Ingest-only -- there is no subscribe/receive API -- so this module only ever
backs the transmit side of a bus (`ZerobusTx`); `ZerobusRx.poll()` raises
NotImplementedError. Use Lakebase or loopback for ECU-to-ECU receive.

`databricks.sdk`/`zerobus.sdk` are imported lazily inside `ZerobusStream.__init__`, not
at module level, so constructing a `ZerobusConfig` never requires either package to be
installed.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import warnings
from typing import TYPE_CHECKING, Annotated

from bench.frame import Frame
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationInfo, field_validator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from bench.bus import Bus

# catalog/schema/table are joined into an unquoted `catalog.schema.table` and handed to
# Zerobus as one string, so a name carrying a dot, a backtick, or a space would silently
# address a different table (or none). Unity Catalog allows a leading digit, unlike a
# bare SQL identifier.
_UC_NAME = re.compile(r"^[A-Za-z0-9_]+$")

# workspace_id and region are interpolated into the endpoint hostname (see
# ZerobusConfig.endpoint), so each has to be a single DNS label. This is what catches a
# whole URL or a `https://` prefix pasted into either one.
_HOST_LABEL = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$")

_NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

# ZerobusConfig's `schema` field shadows BaseModel.schema(), pydantic v1's deprecated
# JSON-schema classmethod, so pydantic warns when the class is created. The name is Unity
# Catalog's own ("catalog.schema.table") and worth keeping; the method it shadows is
# never called here and is scheduled for removal in pydantic v3. Instance access still
# resolves to the field -- a classmethod is a non-data descriptor, so the instance
# __dict__ wins over it.
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message='Field name "schema"', category=UserWarning)

    class ZerobusConfig(BaseModel):
        """Connection settings for one Zerobus Ingest stream into `catalog.schema.table`.

        `client_id`/`client_secret` are the OAuth service-principal credentials; leaving
        them empty defers to the ambient `databricks.sdk.core.Config` (see
        `ZerobusStream`), so unlike the other fields "" is meaningful here and allowed.

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
        client_id: str = ""
        # repr=False so a secret passed in explicitly never reaches a log line or a
        # traceback through the model's repr.
        client_secret: str = Field(default="", repr=False)
        profile: _NonEmpty | None = None

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
        def _blank_profile_means_none(cls, value: object) -> object:
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

    Uses importlib rather than a relative `from . import record_pb2` so a type checker
    does not try (and fail) to statically resolve a module that exists only after a
    build step -- see _generate_record_pb2() above.
    """
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


class ZerobusStream:
    """One Zerobus Ingest protobuf stream into `catalog.schema.table`.

    Credentials are OAuth service-principal client id/secret, not the bearer token the
    Databricks SQL connector uses; resolved from `config.client_id`/`client_secret` or,
    when unset, from the ambient `databricks.sdk.core.Config` (DATABRICKS_CLIENT_ID /
    DATABRICKS_CLIENT_SECRET, or `config.profile`). Raises RuntimeError when no client
    credentials are found.

    `record()` is fire-and-forget; call `flush()` to bound the delay before rows become
    SQL-visible, and `close()` before exiting.
    """

    def __init__(self, config: ZerobusConfig) -> None:
        from databricks.sdk.core import Config
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        # auth_type is forced rather than left to unified-auth detection: Zerobus always
        # needs service-principal M2M credentials (see the class docstring), and letting
        # detection run would raise "more than one authorization method configured" the
        # moment some other ambient credential (e.g. a DATABRICKS_TOKEN a Lakebase bus in
        # the same process is using) is also present.
        dbx_cfg = (
            Config(profile=config.profile, auth_type="oauth-m2m") if config.profile else Config(auth_type="oauth-m2m")
        )
        client_id = config.client_id or dbx_cfg.client_id
        client_secret = config.client_secret or dbx_cfg.client_secret
        if not client_id or not client_secret:
            raise RuntimeError(
                "Zerobus Ingest needs OAuth service-principal credentials: set"
                " ZerobusConfig.client_id/client_secret, or DATABRICKS_CLIENT_ID /"
                " DATABRICKS_CLIENT_SECRET in the environment."
            )
        self._pb = load_record_pb2()
        self._table = f"{config.catalog}.{config.schema}.{config.table}"
        sdk = ZerobusSdk(config.endpoint, dbx_cfg.host)
        self._create_stream = lambda: sdk.create_stream(
            client_id,
            client_secret,
            TableProperties(self._table, self._pb.TestbenchFrame.DESCRIPTOR),
            StreamConfigurationOptions(record_type=RecordType.PROTO),
        )
        self._stream = self._create_stream()

    def _with_stream_retry(self, op):
        # A long-running stream can go bad mid-run; recreate it once on any failure
        # rather than silently losing the rest of the run's frames.
        try:
            return op()
        except Exception as exc:
            print(f"[bench] zerobus {self._table}: {exc!r}; recreating stream and retrying once", flush=True)
            self._stream = self._create_stream()
            return op()

    def record(self, frame: Frame) -> None:
        """Ingest one frame, fire-and-forget. Protobuf's constructor drops the
        None-valued variant fields, so a CAN frame simply leaves the Ethernet columns
        unset.
        """
        self._with_stream_retry(lambda: self._stream.ingest_record_nowait(self._pb.TestbenchFrame(**frame.to_row())))

    def flush(self) -> None:
        """Block until the server has acknowledged everything sent so far."""
        self._with_stream_retry(lambda: self._stream.flush())

    def close(self) -> None:
        """Flush pending records and close the stream."""
        self._stream.flush()
        self._stream.close()


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
