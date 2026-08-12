"""Tests for transport/connection.py's ConnectionConfig: environment-variable defaults,
YAML loading, and the LakebaseConfig/ZerobusConfig it builds -- no network required.
"""

from __future__ import annotations

from transport.connection import ConnectionConfig

_ENV_VARS = (
    "BENCH_LAKEBASE_ENDPOINT",
    "BENCH_LAKEBASE_PROFILE",
    "BENCH_LAKEBASE_DATABASE",
    "ZEROBUS_WORKSPACE_ID",
    "ZEROBUS_REGION",
    "ZEROBUS_CLOUD",
    "ZEROBUS_SP_PROFILE",
)


def _clear_env(monkeypatch):
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_defaults_are_empty_without_environment_variables(monkeypatch):
    _clear_env(monkeypatch)
    config = ConnectionConfig()
    assert config.lakebase_endpoint == ""
    assert config.lakebase_profile is None
    assert config.lakebase_database == "databricks_postgres"
    assert config.zerobus_workspace_id == ""
    assert config.zerobus_cloud == "aws"


def test_construction_reads_environment_variables(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BENCH_LAKEBASE_ENDPOINT", "projects/p/branches/b/endpoints/e")
    monkeypatch.setenv("BENCH_LAKEBASE_PROFILE", "my-profile")
    monkeypatch.setenv("ZEROBUS_WORKSPACE_ID", "12345")
    monkeypatch.setenv("ZEROBUS_REGION", "us-west-2")

    config = ConnectionConfig()
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    assert config.lakebase_profile == "my-profile"
    assert config.zerobus_workspace_id == "12345"
    assert config.zerobus_region == "us-west-2"


def test_from_yaml_overrides_fields(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    path = tmp_path / "connection.yaml"
    path.write_text("lakebase_endpoint: projects/p/branches/b/endpoints/e\nzerobus_region: us-west-2\n")

    config = ConnectionConfig.from_yaml(path)
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    assert config.zerobus_region == "us-west-2"


def test_from_yaml_missing_keys_keep_dataclass_defaults(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BENCH_LAKEBASE_PROFILE", "from-env")
    path = tmp_path / "connection.yaml"
    path.write_text("lakebase_endpoint: projects/p/branches/b/endpoints/e\n")

    config = ConnectionConfig.from_yaml(path)
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    # Not present in the YAML, so this falls through to the field's own default, which
    # itself reads the environment variable -- same precedence as bench.db.ReplayConfig.
    assert config.lakebase_profile == "from-env"


def test_from_yaml_empty_file_is_all_defaults(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    path = tmp_path / "connection.yaml"
    path.write_text("")

    config = ConnectionConfig.from_yaml(path)
    assert config == ConnectionConfig()


def test_lakebase_config_carries_connection_settings(monkeypatch):
    _clear_env(monkeypatch)
    config = ConnectionConfig(lakebase_endpoint="e", lakebase_profile="p", lakebase_database="d")
    lakebase = config.lakebase_config()
    assert lakebase.endpoint_name == "e"
    assert lakebase.profile == "p"
    assert lakebase.dbname == "d"
    # Empty by default so the owning Bus's generated name is used -- see
    # transport.lakebase.on_construct.
    assert lakebase.table == ""


def test_lakebase_config_accepts_an_explicit_table(monkeypatch):
    _clear_env(monkeypatch)
    lakebase = ConnectionConfig().lakebase_config(table="bus_frames")
    assert lakebase.table == "bus_frames"


def test_zerobus_config_carries_connection_settings_and_default_table(monkeypatch):
    _clear_env(monkeypatch)
    config = ConnectionConfig(zerobus_workspace_id="w", zerobus_region="r", zerobus_cloud="azure", zerobus_profile="p")
    zerobus = config.zerobus_config()
    assert zerobus.workspace_id == "w"
    assert zerobus.region == "r"
    assert zerobus.cloud == "azure"
    assert zerobus.profile == "p"
    assert zerobus.table == "blf_testbench_frames"
