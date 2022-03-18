import os
from datetime import datetime
from unittest import mock

import pytest

from taipy.core.config._config import _Config
from taipy.core.config.config import Config
from taipy.core.data._data_manager import _DataManager
from taipy.core.exceptions.exceptions import ConfigurationIssueError


@pytest.fixture(scope="function", autouse=True)
def reset_configuration_singleton():
    yield
    Config._python_config = _Config()
    Config._file_config = _Config()
    Config._env_file_config = _Config()
    Config._applied_config = _Config._default_config()


def test_data_node_config_check():
    data_node_config = Config._add_data_node("data_nodes1", "pickle")
    assert list(Config.data_nodes) == ["default", data_node_config.id]

    data_node2_config = Config._add_data_node("data_nodes2", "pickle")
    assert list(Config.data_nodes) == ["default", data_node_config.id, data_node2_config.id]

    data_node3_config = Config._add_data_node("data_nodes3", "csv", has_header=True, path="")
    assert list(Config.data_nodes) == [
        "default",
        data_node_config.id,
        data_node2_config.id,
        data_node3_config.id,
    ]

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", storage_type="bar")
        Config._check()

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", scope="bar")
        Config._check()

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", storage_type="csv")
        Config._check()

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", storage_type="sql")
        Config._check()

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", storage_type="excel")
        Config._check()

    with pytest.raises(ConfigurationIssueError):
        Config._add_data_node("data_nodes", storage_type="generic")
        Config._check()


def test_data_node_count():
    Config._add_data_node("data_nodes1", "pickle")
    assert len(Config.data_nodes) == 2

    Config._add_data_node("data_nodes2", "pickle")
    assert len(Config.data_nodes) == 3

    Config._add_data_node("data_nodes3", "pickle")
    assert len(Config.data_nodes) == 4


def test_data_node_getitem():
    data_node_id = "data_nodes1"
    data_node_config = Config._add_data_node(data_node_id, "pickle")

    assert Config.data_nodes[data_node_id].id == data_node_config.id
    assert Config.data_nodes[data_node_id].storage_type == data_node_config.storage_type
    assert Config.data_nodes[data_node_id].scope == data_node_config.scope
    assert Config.data_nodes[data_node_id].properties == data_node_config.properties
    assert Config.data_nodes[data_node_id].cacheable == data_node_config.cacheable


def test_data_node_creation_no_duplication():
    Config._add_data_node("data_nodes1", "pickle")

    assert len(Config.data_nodes) == 2

    Config._add_data_node("data_nodes1", "pickle")
    assert len(Config.data_nodes) == 2


def test_date_node_create_with_datetime():
    data_node_config = Config._add_data_node(
        id="datetime_data",
        my_property=datetime(1991, 1, 1),
        foo="hello",
        test=1,
        test_dict={"type": "Datetime", 2: "daw"},
    )
    dn = _DataManager._get_or_create(data_node_config)
    dn = _DataManager._get(dn)
    assert dn.foo == "hello"
    assert dn.my_property == datetime(1991, 1, 1)
    assert dn.test == 1
    assert dn.test_dict.get("type") == "Datetime"


def test_data_node_with_env_variable_value():
    with mock.patch.dict(os.environ, {"BAR": "baz"}):
        Config._add_data_node("data_node", prop="ENV[BAR]")
        assert Config.data_nodes["data_node"].prop == "baz"


def test_data_node_with_env_variable_in_write_fct_params():
    with mock.patch.dict(os.environ, {"Foo": "bar", "BAZ": "qux"}):
        Config._add_data_node(
            "data_node", storage_type="generic", write_fct_params=("ENV[FOO]", "my_param", "ENV[BAZ]")
        )
        assert Config.data_nodes["data_node"].write_fct_params == ("bar", "my_param", "qux")


def test_data_node_with_env_variable_in_read_fct_params():
    with mock.patch.dict(os.environ, {"Foo": "bar", "BAZ": "qux"}):
        Config._add_data_node("data_node", storage_type="generic", read_fct_params=("ENV[FOO]", "my_param", "ENV[BAZ]"))
        assert Config.data_nodes["data_node"].read_fct_params == ("bar", "my_param", "qux")
