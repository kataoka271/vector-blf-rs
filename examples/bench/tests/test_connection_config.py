"""Tests for transport/connection.py's ConnectionConfig: from_environ()'s
environment-variable reads, YAML loading, and the LakebaseConfig/ZerobusConfig it
builds -- no network required.
"""

from __future__ import annotations

import pytest
from transport.connection import ConnectionConfig

_ENV_VARS = (
    "LAKEBASE_ENDPOINT",
    "LAKEBASE_PROFILE",
    "LAKEBASE_DATABASE",
    "ZEROBUS_WORKSPACE_ID",
    "ZEROBUS_REGION",
    "ZEROBUS_PROFILE",
    "ZEROBUS_CATALOG",
    "ZEROBUS_SCHEMA",
)


def _clear_env(monkeypatch):
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_construction_reads_environment_variables(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LAKEBASE_ENDPOINT", "projects/p/branches/b/endpoints/e")
    monkeypatch.setenv("LAKEBASE_PROFILE", "my-profile")
    monkeypatch.setenv("LAKEBASE_DATABASE", "my-database")
    monkeypatch.setenv("ZEROBUS_WORKSPACE_ID", "12345")
    monkeypatch.setenv("ZEROBUS_REGION", "us-west-2")
    monkeypatch.setenv("ZEROBUS_PROFILE", "my-zerobus-sp")
    monkeypatch.setenv("ZEROBUS_CATALOG", "my-zerobus-catalog")
    monkeypatch.setenv("ZEROBUS_SCHEMA", "my-zerobus-schema")

    config = ConnectionConfig.from_environ()
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    assert config.lakebase_profile == "my-profile"
    assert config.lakebase_database == "my-database"
    assert config.zerobus_workspace_id == "12345"
    assert config.zerobus_region == "us-west-2"
    assert config.zerobus_profile == "my-zerobus-sp"
    assert config.zerobus_catalog == "my-zerobus-catalog"
    assert config.zerobus_schema == "my-zerobus-schema"


def test_from_environ_raises_on_a_missing_required_variable(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LAKEBASE_ENDPOINT", "projects/p/branches/b/endpoints/e")
    # Every other required variable (LAKEBASE_DATABASE, ZEROBUS_*) is left unset.
    with pytest.raises(KeyError):
        ConnectionConfig.from_environ()


def test_from_yaml_overrides_fields(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    path = tmp_path / "connection.yaml"
    path.write_text(
        "lakebase_endpoint: projects/p/branches/b/endpoints/e\n"
        "lakebase_database: d\n"
        "zerobus_catalog: c\n"
        "zerobus_schema: s\n"
        "zerobus_workspace_id: w\n"
        "zerobus_region: us-west-2\n"
    )

    config = ConnectionConfig.from_yaml(path)
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    assert config.zerobus_region == "us-west-2"


def test_from_yaml_missing_optional_keys_keep_dataclass_defaults(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LAKEBASE_PROFILE", "from-env")
    path = tmp_path / "connection.yaml"
    path.write_text(
        "lakebase_endpoint: projects/p/branches/b/endpoints/e\n"
        "lakebase_database: d\n"
        "zerobus_catalog: c\n"
        "zerobus_schema: s\n"
        "zerobus_workspace_id: w\n"
        "zerobus_region: r\n"
    )

    config = ConnectionConfig.from_yaml(path)
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    # lakebase_profile isn't in the YAML, so this falls through to the dataclass's own
    # hardcoded default (None) -- from_yaml() never reads the environment, unlike
    # bench.db.ReplayConfig.from_yaml, so LAKEBASE_PROFILE being set has no effect here.
    assert config.lakebase_profile is None


def test_lakebase_config_carries_connection_settings(monkeypatch):
    _clear_env(monkeypatch)
    config = ConnectionConfig(
        lakebase_endpoint="e",
        lakebase_profile="p",
        lakebase_database="d",
        zerobus_catalog="c",
        zerobus_schema="s",
        zerobus_workspace_id="w",
        zerobus_region="r",
    )
    lakebase = config.lakebase_config(table="bus_frames")
    assert lakebase.endpoint_name == "e"
    assert lakebase.profile == "p"
    assert lakebase.dbname == "d"
    assert lakebase.table == "bus_frames"


def test_zerobus_config_carries_connection_settings_and_explicit_table(monkeypatch):
    _clear_env(monkeypatch)
    config = ConnectionConfig(
        lakebase_endpoint="e",
        lakebase_database="d",
        zerobus_workspace_id="w",
        zerobus_region="r",
        zerobus_profile="p",
        zerobus_catalog="c",
        zerobus_schema="s",
    )
    zerobus = config.zerobus_config(table="frames")
    assert zerobus.workspace_id == "w"
    assert zerobus.region == "r"
    assert zerobus.profile == "p"
    assert zerobus.catalog == "c"
    assert zerobus.schema == "s"
    assert zerobus.table == "frames"
