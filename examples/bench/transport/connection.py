"""Where a bench process gets its Lakebase/Zerobus connection settings.

Every field except `lakebase_profile`/`zerobus_profile` is required, with no
library-level default -- constructing a `ConnectionConfig` with one missing is a
`TypeError` naming it, rather than silently falling back to an empty string or ambient
environment variable. A wrong-but-present default (a stale catalog, someone else's
endpoint) is worse than a loud failure at construction.

`from_environ()` builds one from LAKEBASE_*/ZEROBUS_* environment variables (profiles
optional there too); `from_yaml()` builds one from a YAML file's keys. Neither layers on
top of the other or on top of ambient environment variables -- pick one source per
process.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from transport.lakebase import LakebaseConfig
from transport.zerobus import ZerobusConfig


@dataclass(frozen=True)
class ConnectionConfig:
    """Connection settings shared by every Lakebase/Zerobus bus in one process."""

    lakebase_endpoint: str
    lakebase_database: str
    zerobus_catalog: str
    zerobus_schema: str
    zerobus_workspace_id: str
    zerobus_region: str

    lakebase_profile: str | None = None
    zerobus_profile: str | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> ConnectionConfig:
        """Load a ConnectionConfig from a YAML file.

        Missing keys keep their dataclass defaults (environment variable, then hardcoded
        default), matching bench.db.ReplayConfig.from_yaml.
        """
        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls(**data)

    @classmethod
    def from_environ(cls) -> ConnectionConfig:
        return cls(
            lakebase_endpoint=os.environ["LAKEBASE_ENDPOINT"],
            lakebase_database=os.environ["LAKEBASE_DATABASE"],
            zerobus_catalog=os.environ["ZEROBUS_CATALOG"],
            zerobus_schema=os.environ["ZEROBUS_SCHEMA"],
            zerobus_workspace_id=os.environ["ZEROBUS_WORKSPACE_ID"],
            zerobus_region=os.environ["ZEROBUS_REGION"],
            lakebase_profile=os.environ.get("LAKEBASE_PROFILE"),
            zerobus_profile=os.environ.get("ZEROBUS_PROFILE"),
        )

    def lakebase_config(self, *, table: str) -> LakebaseConfig:
        """Return a LakebaseConfig for `table` using these connection settings. `table`
        must be named explicitly by the caller (see the module docstring).

        Raises `pydantic.ValidationError` when a connection setting is blank or `table`
        is not a bare SQL identifier -- see LakebaseConfig.
        """
        return LakebaseConfig(
            profile=self.lakebase_profile,
            endpoint_name=self.lakebase_endpoint,
            dbname=self.lakebase_database,
            table=table,
        )

    def zerobus_config(self, *, table: str) -> ZerobusConfig:
        """Return a ZerobusConfig for `table` using these connection settings.

        Raises `pydantic.ValidationError` when a connection setting is blank, a name is
        not a bare Unity Catalog name, or `zerobus_workspace_id`/`zerobus_region` is not
        a single DNS label -- see ZerobusConfig.
        """
        return ZerobusConfig(
            workspace_id=self.zerobus_workspace_id,
            region=self.zerobus_region,
            catalog=self.zerobus_catalog,
            schema=self.zerobus_schema,
            table=table,
            profile=self.zerobus_profile,
        )
