"""Tests for the three connection-settings objects: `transport/connection.py`'s
`ConnectionConfig` (environment/YAML sources, and the per-bus configs it derives) and the
two pydantic models it builds, `LakebaseConfig` and `ZerobusConfig`. No network required.

Only runtime behaviour is tested. A missing or misspelled field is a `ty` error at the
call site (both models set `extra="forbid"`, and `ConnectionConfig` is a plain dataclass),
so tests for those would restate the annotation. What remains is what a type checker
cannot see: which environment variable feeds which field, which of the two sources reads
the environment, and validation of the *values* -- blank strings, and names interpolated
into SQL or into a hostname.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from transport.connection import ConnectionConfig
from transport.lakebase import LakebaseConfig
from transport.zerobus import ZerobusConfig

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

LAKEBASE_KWARGS = {
    "dbname": "databricks_postgres",
    "table": "bus_frames",
    "endpoint_name": "projects/p/branches/b/endpoints/e",
}

ZEROBUS_KWARGS = {
    "catalog": "main",
    "schema": "blf",
    "table": "testbench_frames",
    "workspace_id": "1234567890",
    "region": "us-east-2",
}

REQUIRED_YAML = (
    "lakebase_endpoint: projects/p/branches/b/endpoints/e\n"
    "lakebase_database: d\n"
    "zerobus_catalog: c\n"
    "zerobus_schema: s\n"
    "zerobus_workspace_id: w\n"
)


def _clear_env(monkeypatch):
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _connection_config(**overrides) -> ConnectionConfig:
    return ConnectionConfig(
        **{
            "lakebase_endpoint": "e",
            "lakebase_database": "d",
            "zerobus_catalog": "c",
            "zerobus_schema": "s",
            "zerobus_workspace_id": "w",
            "zerobus_region": "r",
            **overrides,
        }
    )


# -- ConnectionConfig: where the settings come from ---------------------------------


def test_from_environ_maps_each_variable_to_its_field(monkeypatch):
    _clear_env(monkeypatch)
    for name in _ENV_VARS:
        monkeypatch.setenv(name, f"value-of-{name}")

    config = ConnectionConfig.from_environ()
    assert config.lakebase_endpoint == "value-of-LAKEBASE_ENDPOINT"
    assert config.lakebase_profile == "value-of-LAKEBASE_PROFILE"
    assert config.lakebase_database == "value-of-LAKEBASE_DATABASE"
    assert config.zerobus_workspace_id == "value-of-ZEROBUS_WORKSPACE_ID"
    assert config.zerobus_region == "value-of-ZEROBUS_REGION"
    assert config.zerobus_profile == "value-of-ZEROBUS_PROFILE"
    assert config.zerobus_catalog == "value-of-ZEROBUS_CATALOG"
    assert config.zerobus_schema == "value-of-ZEROBUS_SCHEMA"


def test_from_environ_raises_naming_the_missing_variable(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LAKEBASE_ENDPOINT", "projects/p/branches/b/endpoints/e")
    # Every other required variable (LAKEBASE_DATABASE, ZEROBUS_*) is left unset.
    with pytest.raises(KeyError, match="LAKEBASE_DATABASE"):
        ConnectionConfig.from_environ()


def test_from_environ_leaves_the_two_profiles_unset_when_absent(monkeypatch):
    _clear_env(monkeypatch)
    for name in _ENV_VARS:
        if not name.endswith("_PROFILE"):
            monkeypatch.setenv(name, "x")

    config = ConnectionConfig.from_environ()
    assert config.lakebase_profile is None
    assert config.zerobus_profile is None


def test_from_yaml_reads_every_key(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    path = tmp_path / "connection.yaml"
    path.write_text(REQUIRED_YAML + "zerobus_region: us-west-2\n")

    config = ConnectionConfig.from_yaml(path)
    assert config.lakebase_endpoint == "projects/p/branches/b/endpoints/e"
    assert config.zerobus_region == "us-west-2"


def test_from_yaml_never_falls_back_to_the_environment(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("LAKEBASE_PROFILE", "from-env")
    path = tmp_path / "connection.yaml"
    path.write_text(REQUIRED_YAML + "zerobus_region: r\n")

    # lakebase_profile isn't in the YAML, so it falls through to the dataclass's own
    # default (None) -- from_yaml() never reads the environment, unlike
    # bench.db.ReplayConfig.from_yaml, so LAKEBASE_PROFILE being set has no effect.
    assert ConnectionConfig.from_yaml(path).lakebase_profile is None


# -- ConnectionConfig: the per-bus configs it derives -------------------------------


def test_lakebase_config_renames_the_connection_fields_it_carries_over():
    lakebase = _connection_config(
        lakebase_endpoint="an-endpoint",
        lakebase_profile="a-profile",
        lakebase_database="a-database",
    ).lakebase_config(table="bus_frames")
    assert lakebase.endpoint_name == "an-endpoint"
    assert lakebase.profile == "a-profile"
    assert lakebase.dbname == "a-database"
    # The table is per-bus, never carried over from the shared connection settings.
    assert lakebase.table == "bus_frames"


def test_zerobus_config_carries_the_connection_fields_and_a_per_bus_table():
    zerobus = _connection_config(
        zerobus_workspace_id="1234567890",
        zerobus_region="us-east-2",
        zerobus_profile="a-profile",
        zerobus_catalog="a_catalog",
        zerobus_schema="a_schema",
    ).zerobus_config(table="frames")
    assert (zerobus.workspace_id, zerobus.region, zerobus.profile) == ("1234567890", "us-east-2", "a-profile")
    assert (zerobus.catalog, zerobus.schema, zerobus.table) == ("a_catalog", "a_schema", "frames")


def test_deriving_a_bus_config_from_a_blank_connection_setting_fails_at_construction():
    # A blank value reaching a bus config is the failure ConnectionConfig exists to make
    # loud: nothing downstream would notice `dbname=""` until the first connect.
    with pytest.raises(ValidationError, match="dbname"):
        _connection_config(lakebase_database="  ").lakebase_config(table="bus_frames")


# -- LakebaseConfig ------------------------------------------------------------------


@pytest.mark.parametrize("blank", ["", "   "])
def test_lakebase_config_rejects_a_blank_setting(blank):
    with pytest.raises(ValidationError, match="dbname"):
        LakebaseConfig(**{**LAKEBASE_KWARGS, "dbname": blank})


@pytest.mark.parametrize("table", ['schema."bus_frames"', "bus frames", "1_frames", "bus;drop", ""])
def test_lakebase_config_rejects_a_table_that_is_not_a_bare_identifier(table):
    # The table name is interpolated into DDL/DML, so this is the check that keeps a
    # crafted name out of a statement -- see transport/lakebase.py::sql.
    with pytest.raises(ValidationError, match="table"):
        LakebaseConfig(**{**LAKEBASE_KWARGS, "table": table})


def test_lakebase_config_treats_a_blank_profile_as_none():
    assert LakebaseConfig(**LAKEBASE_KWARGS, profile="").profile is None


def test_lakebase_config_strips_surrounding_whitespace():
    assert LakebaseConfig(**{**LAKEBASE_KWARGS, "dbname": "  db  "}).dbname == "db"


def test_lakebase_config_is_frozen():
    # Not a type-checker restatement: ty does not model pydantic's frozen=True, and both
    # of a bus's channel threads re-read the config across reconnects.
    config = LakebaseConfig(**LAKEBASE_KWARGS)
    with pytest.raises(ValidationError):
        config.table = "other_frames"


# -- ZerobusConfig -------------------------------------------------------------------


def test_zerobus_config_builds_its_endpoint_from_workspace_id_and_region():
    assert ZerobusConfig(**ZEROBUS_KWARGS).endpoint == "https://1234567890.zerobus.us-east-2.cloud.databricks.com"


def test_zerobus_schema_field_wins_over_the_basemodel_method_it_shadows():
    # `schema` is Unity Catalog's own name for this and shadows pydantic v1's deprecated
    # BaseModel.schema() classmethod; instance access must still resolve to the field.
    assert ZerobusConfig(**ZEROBUS_KWARGS).schema == "blf"


@pytest.mark.parametrize("field", ["catalog", "schema", "table"])
@pytest.mark.parametrize("name", ["main.blf", "`main`", "bus frames", ""])
def test_zerobus_config_rejects_a_name_that_is_not_a_bare_unity_catalog_name(field, name):
    # The three are joined into an unquoted `catalog.schema.table`, so a dot or a
    # backtick in any of them addresses a different table than the one intended.
    with pytest.raises(ValidationError, match=field):
        ZerobusConfig(**{**ZEROBUS_KWARGS, field: name})


def test_zerobus_config_accepts_a_name_starting_with_a_digit():
    # Unity Catalog allows it, unlike a bare SQL identifier.
    assert ZerobusConfig(**{**ZEROBUS_KWARGS, "table": "2024_frames"}).table == "2024_frames"


@pytest.mark.parametrize("field", ["workspace_id", "region"])
@pytest.mark.parametrize("value", ["https://example.com", "us east 2", "a.b", "-leading", ""])
def test_zerobus_config_rejects_a_host_part_that_is_not_a_dns_label(field, value):
    with pytest.raises(ValidationError, match=field):
        ZerobusConfig(**{**ZEROBUS_KWARGS, field: value})


def test_zerobus_config_treats_a_blank_profile_as_none():
    assert ZerobusConfig(**ZEROBUS_KWARGS, profile="").profile is None


def test_zerobus_config_allows_empty_client_credentials():
    # "" is meaningful here: it defers to the ambient databricks.sdk Config.
    config = ZerobusConfig(**ZEROBUS_KWARGS)
    assert config.client_id == ""
    assert config.client_secret == ""


def test_zerobus_config_keeps_the_client_secret_out_of_its_repr():
    config = ZerobusConfig(**ZEROBUS_KWARGS, client_id="an-id", client_secret="a-secret")
    assert "an-id" in repr(config)
    assert "a-secret" not in repr(config)


def test_zerobus_config_is_frozen():
    config = ZerobusConfig(**ZEROBUS_KWARGS)
    with pytest.raises(ValidationError):
        config.table = "other_frames"
