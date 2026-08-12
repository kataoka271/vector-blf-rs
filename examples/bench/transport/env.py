"""Where a bench process gets its connection settings from the environment.

Deliberately small: running examples/bench against real Lakebase/Zerobus should need a
Lakebase endpoint and a Databricks profile, and nothing else. The Zerobus fields exist
for ReceiverEcu's upload leg and stay empty in the common (loopback/CAN-only) case.
"""

from __future__ import annotations

import dataclasses
import os

from transport.lakebase import DEFAULT_DATABASE, LakebaseConfig
from transport.zerobus import ZerobusConfig


@dataclasses.dataclass(frozen=True)
class Environment:
    """Connection settings shared by every component in one process."""

    lakebase_endpoint: str = ""
    lakebase_profile: str | None = None
    lakebase_database: str = DEFAULT_DATABASE
    zerobus_workspace_id: str = ""
    zerobus_region: str = ""
    zerobus_cloud: str = "aws"
    zerobus_profile: str | None = None

    @classmethod
    def from_env(cls) -> Environment:
        """Build an Environment from BENCH_*/ZEROBUS_* variables."""
        return cls(
            lakebase_endpoint=os.environ.get("BENCH_LAKEBASE_ENDPOINT", ""),
            lakebase_profile=os.environ.get("BENCH_LAKEBASE_PROFILE"),
            lakebase_database=os.environ.get("BENCH_LAKEBASE_DATABASE", DEFAULT_DATABASE),
            zerobus_workspace_id=os.environ.get("ZEROBUS_WORKSPACE_ID", ""),
            zerobus_region=os.environ.get("ZEROBUS_REGION", ""),
            zerobus_cloud=os.environ.get("ZEROBUS_CLOUD", "aws"),
            zerobus_profile=os.environ.get("ZEROBUS_SP_PROFILE"),
        )

    def lakebase_config(self, *, table: str) -> LakebaseConfig:
        """Return a LakebaseConfig for `table` using these connection settings."""
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
