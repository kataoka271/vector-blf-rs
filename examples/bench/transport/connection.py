"""Where a bench process gets its Lakebase/Zerobus connection settings.

Deliberately small: running examples/bench against real Lakebase/Zerobus should need a
Lakebase endpoint and a Databricks profile, and nothing else. The Zerobus fields exist
for ReceiverEcu's upload leg and stay empty in the common (loopback/CAN-only) case.

Field defaults already read BENCH_*/ZEROBUS_* environment variables, so plain
`ConnectionConfig()` is "from the environment" with no separate from_env() needed --
from_yaml() layers explicit YAML values on top of those same defaults, mirroring
bench.db.ReplayConfig.from_yaml().
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import yaml

from transport.lakebase import DEFAULT_DATABASE, LakebaseConfig
from transport.zerobus import ZerobusConfig


@dataclasses.dataclass(frozen=True)
class ConnectionConfig:
    """Connection settings shared by every Lakebase/Zerobus bus in one process."""

    lakebase_endpoint: str = dataclasses.field(default_factory=lambda: os.environ.get("BENCH_LAKEBASE_ENDPOINT", ""))
    lakebase_profile: str | None = dataclasses.field(default_factory=lambda: os.environ.get("BENCH_LAKEBASE_PROFILE"))
    lakebase_database: str = dataclasses.field(
        default_factory=lambda: os.environ.get("BENCH_LAKEBASE_DATABASE", DEFAULT_DATABASE)
    )
    zerobus_workspace_id: str = dataclasses.field(default_factory=lambda: os.environ.get("ZEROBUS_WORKSPACE_ID", ""))
    zerobus_region: str = dataclasses.field(default_factory=lambda: os.environ.get("ZEROBUS_REGION", ""))
    zerobus_cloud: str = dataclasses.field(default_factory=lambda: os.environ.get("ZEROBUS_CLOUD", "aws"))
    zerobus_profile: str | None = dataclasses.field(default_factory=lambda: os.environ.get("ZEROBUS_SP_PROFILE"))

    @classmethod
    def from_yaml(cls, path: str | Path) -> ConnectionConfig:
        """Load a ConnectionConfig from a YAML file.

        Missing keys keep their dataclass defaults (environment variable, then hardcoded
        default), matching bench.db.ReplayConfig.from_yaml.
        """
        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls(**data)

    def lakebase_config(self, *, table: str = "") -> LakebaseConfig:
        """Return a LakebaseConfig for `table` using these connection settings.

        `table` defaults to "" so the owning Bus's generated name is used instead -- see
        transport.lakebase.on_construct.
        """
        return LakebaseConfig(
            profile=self.lakebase_profile,
            endpoint_name=self.lakebase_endpoint,
            dbname=self.lakebase_database,
            table=table,
        )

    def zerobus_config(self, *, table: str = "blf_testbench_frames") -> ZerobusConfig:
        """Return a ZerobusConfig for `table` using these connection settings."""
        return ZerobusConfig(
            workspace_id=self.zerobus_workspace_id,
            region=self.zerobus_region,
            cloud=self.zerobus_cloud,
            table=table,
            profile=self.zerobus_profile,
        )
