"""Validation tests for the two pydantic transport configs -- transport/lakebase.py's
LakebaseConfig and transport/zerobus.py's ZerobusConfig. No network required.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from transport.lakebase import LakebaseConfig
from transport.zerobus import ZerobusConfig

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


def test_lakebase_config_accepts_a_complete_set_of_settings():
    config = LakebaseConfig(**LAKEBASE_KWARGS, profile="my-profile")
    assert config.dbname == "databricks_postgres"
    assert config.table == "bus_frames"
    assert config.endpoint_name == "projects/p/branches/b/endpoints/e"
    assert config.profile == "my-profile"


@pytest.mark.parametrize("field", sorted(LAKEBASE_KWARGS))
def test_lakebase_config_rejects_a_missing_setting(field):
    kwargs = {k: v for k, v in LAKEBASE_KWARGS.items() if k != field}
    with pytest.raises(ValidationError, match=field):
        LakebaseConfig(**kwargs)


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


def test_lakebase_config_rejects_an_unknown_setting():
    with pytest.raises(ValidationError, match="db_name"):
        LakebaseConfig(**{**LAKEBASE_KWARGS, "db_name": "typo"})


def test_lakebase_config_treats_a_blank_profile_as_none():
    assert LakebaseConfig(**LAKEBASE_KWARGS, profile="").profile is None


def test_lakebase_config_strips_surrounding_whitespace():
    assert LakebaseConfig(**{**LAKEBASE_KWARGS, "dbname": "  db  "}).dbname == "db"


def test_lakebase_config_is_frozen():
    config = LakebaseConfig(**LAKEBASE_KWARGS)
    with pytest.raises(ValidationError):
        config.table = "other_frames"


def test_zerobus_config_accepts_a_complete_set_of_settings():
    config = ZerobusConfig(**ZEROBUS_KWARGS, profile="my-sp")
    assert config.catalog == "main"
    assert config.schema == "blf"
    assert config.table == "testbench_frames"
    assert config.workspace_id == "1234567890"
    assert config.region == "us-east-2"
    assert config.profile == "my-sp"


def test_zerobus_config_builds_its_endpoint_from_workspace_id_and_region():
    config = ZerobusConfig(**ZEROBUS_KWARGS)
    assert config.endpoint == "https://1234567890.zerobus.us-east-2.cloud.databricks.com"


@pytest.mark.parametrize("field", sorted(ZEROBUS_KWARGS))
def test_zerobus_config_rejects_a_missing_setting(field):
    kwargs = {k: v for k, v in ZEROBUS_KWARGS.items() if k != field}
    with pytest.raises(ValidationError, match=field):
        ZerobusConfig(**kwargs)


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


def test_zerobus_config_rejects_an_unknown_setting():
    with pytest.raises(ValidationError, match="scheme"):
        ZerobusConfig(**{**ZEROBUS_KWARGS, "scheme": "typo"})


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
