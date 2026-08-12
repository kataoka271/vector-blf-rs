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
import subprocess
import sys
from dataclasses import dataclass

from bench.frame import Frame

_ZEROBUS_HOST_SUFFIX = {"aws": "cloud.databricks.com", "azure": "azuredatabricks.net"}


@dataclass
class ZerobusConfig:
    workspace_id: str = ""
    region: str = ""
    cloud: str = "aws"
    catalog: str | None = None
    schema: str | None = None
    table: str = "blf_testbench_frames"
    client_id: str = ""
    client_secret: str = ""
    profile: str | None = None

    @property
    def endpoint(self) -> str:
        try:
            suffix = _ZEROBUS_HOST_SUFFIX[self.cloud]
        except KeyError:
            raise ValueError(
                f"unknown zerobus.cloud {self.cloud!r}; expected one of {sorted(_ZEROBUS_HOST_SUFFIX)}"
            ) from None
        return f"https://{self.workspace_id}.zerobus.{self.region}.{suffix}"


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

    def __init__(self, config: ZerobusConfig, *, default_catalog: str = "main", default_schema: str = "blf") -> None:
        from databricks.sdk.core import Config
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        dbx_cfg = Config(profile=config.profile) if config.profile else Config()
        client_id = config.client_id or dbx_cfg.client_id
        client_secret = config.client_secret or dbx_cfg.client_secret
        if not client_id or not client_secret:
            raise RuntimeError(
                "Zerobus Ingest needs OAuth service-principal credentials: set"
                " ZerobusConfig.client_id/client_secret, or DATABRICKS_CLIENT_ID /"
                " DATABRICKS_CLIENT_SECRET in the environment."
            )
        self._pb = load_record_pb2()
        catalog = config.catalog or default_catalog
        schema = config.schema or default_schema
        self._table = f"{catalog}.{schema}.{config.table}"
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
