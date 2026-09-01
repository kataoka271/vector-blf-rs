"""Where a bench process gets its Lakebase/Zerobus connection settings.

No setting has a library-level default: a wrong-but-present one (a stale catalog,
someone else's endpoint) is worse than a loud failure. But the failure is per bus, not
per process -- a Lakebase-only topology must not need ZEROBUS_* set, so an unset setting
only raises `MissingSetting` when the bus that needs it is actually built
(`lakebase_config()`/`zerobus_config()`), naming exactly what is missing. A topology that
overrides a slot with a Loopback never calls the corresponding method and so needs
nothing at all.

`from_environ()` builds one from LAKEBASE_*/ZEROBUS_* environment variables (each named
after the field it fills); `from_yaml()` builds one from a YAML file's keys, and an
unknown key there is a `TypeError`. Neither layers on top of the other or on top of
ambient environment variables -- pick one source per process.

Unset and blank are different: a setting left out entirely is `MissingSetting`, while one
present but blank reaches the bus config and fails its `pydantic` validation. Only a
blank `lakebase_profile`/`zerobus_profile` is folded into None, since both spell "no
profile".
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from transport.lakebase import LakebaseConfig
from transport.zerobus import ZerobusConfig


class MissingSetting(Exception):
    """A bus config was derived from connection settings that are unset.

    Carries `settings` (the unset field names, in the order the bus config wants them)
    so a caller can report them; `main.py` catches this to turn an incomplete
    environment or YAML file into a CLI usage error rather than a traceback.
    """

    def __init__(self, what: str, settings: list[str]) -> None:
        names = ", ".join(settings)
        variables = ", ".join(name.upper() for name in settings)
        super().__init__(
            f"{what} needs connection settings that are unset: {names} "
            f"(set {variables}, or give them in the --connection-config YAML)"
        )
        self.settings = settings


def _require(what: str, **settings: str | None) -> list[str]:
    """Return each value of `settings` in order, or raise `MissingSetting` naming every
    one that is None.

    Reports all of them at once: filling in one variable only to be told about the next
    turns a single mistake into a series of runs.
    """
    missing = [name for name, value in settings.items() if value is None]
    if missing:
        raise MissingSetting(what, missing)
    return [value for value in settings.values() if value is not None]


@dataclass(frozen=True)
class ConnectionConfig:
    """Connection settings shared by every Lakebase/Zerobus bus in one process.

    Every field defaults to None, meaning "not supplied by this process's source" -- see
    the module docstring for why that is not the same as a usable default.
    """

    lakebase_endpoint: str | None = None
    lakebase_database: str | None = None
    lakebase_profile: str | None = None

    zerobus_catalog: str | None = None
    zerobus_schema: str | None = None
    zerobus_workspace_id: str | None = None
    zerobus_region: str | None = None
    zerobus_service_principal_id: str | None = None
    zerobus_profile: str | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> ConnectionConfig:
        """Load a ConnectionConfig from a YAML file. An empty file yields one with every
        setting unset; an unknown key is a `TypeError` naming it.
        """
        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls(**data)

    @classmethod
    def from_environ(cls) -> ConnectionConfig:
        """Read every LAKEBASE_*/ZEROBUS_* variable, leaving unset ones None.

        Never raises for an unset variable -- which of them this process actually needs
        is not known until its buses are built.
        """
        return cls(
            lakebase_endpoint=os.environ.get("LAKEBASE_ENDPOINT"),
            lakebase_database=os.environ.get("LAKEBASE_DATABASE"),
            lakebase_profile=os.environ.get("LAKEBASE_PROFILE"),
            zerobus_catalog=os.environ.get("ZEROBUS_CATALOG"),
            zerobus_schema=os.environ.get("ZEROBUS_SCHEMA"),
            zerobus_workspace_id=os.environ.get("ZEROBUS_WORKSPACE_ID"),
            zerobus_region=os.environ.get("ZEROBUS_REGION"),
            zerobus_service_principal_id=os.environ.get("ZEROBUS_SERVICE_PRINCIPAL_ID"),
            zerobus_profile=os.environ.get("ZEROBUS_PROFILE"),
        )

    def lakebase_config(self, *, table: str) -> LakebaseConfig:
        """Return a LakebaseConfig for `table` using these connection settings. `table`
        must be named explicitly by the caller (see the module docstring).

        Raises `MissingSetting` when a Lakebase setting is unset, or
        `pydantic.ValidationError` when one is blank or `table` is not a bare SQL
        identifier -- see LakebaseConfig.
        """
        endpoint, database = _require(
            "a Lakebase bus",
            lakebase_endpoint=self.lakebase_endpoint,
            lakebase_database=self.lakebase_database,
        )
        return LakebaseConfig(
            profile=self.lakebase_profile,
            endpoint_name=endpoint,
            dbname=database,
            table=table,
        )

    def zerobus_config(self, *, table: str) -> ZerobusConfig:
        """Return a ZerobusConfig for `table` using these connection settings.

        Raises `MissingSetting` when a Zerobus setting is unset, or
        `pydantic.ValidationError` when one is blank, a name is not a bare Unity Catalog
        name, or `zerobus_workspace_id`/`zerobus_region` is not a single DNS label -- see
        ZerobusConfig.
        """
        catalog, schema, workspace_id, region, service_principal_id = _require(
            "a Zerobus stream",
            zerobus_catalog=self.zerobus_catalog,
            zerobus_schema=self.zerobus_schema,
            zerobus_workspace_id=self.zerobus_workspace_id,
            zerobus_region=self.zerobus_region,
            zerobus_service_principal_id=self.zerobus_service_principal_id,
        )
        return ZerobusConfig(
            workspace_id=workspace_id,
            region=region,
            catalog=catalog,
            schema=schema,
            table=table,
            profile=self.zerobus_profile,
            service_principal_id=service_principal_id,
        )
