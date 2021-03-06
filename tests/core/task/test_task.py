# Copyright 2022 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from unittest import mock

import pytest
from taipy.config.config import Config
from taipy.config.data_node.data_node_config import DataNodeConfig
from taipy.config.data_node.scope import Scope
from taipy.config.exceptions.exceptions import InvalidConfigurationId

from src.taipy.core.data._data_manager import _DataManager
from src.taipy.core.data.csv import CSVDataNode
from src.taipy.core.data.data_node import DataNode
from src.taipy.core.data.in_memory import InMemoryDataNode
from src.taipy.core.task._task_manager import _TaskManager
from src.taipy.core.task.task import Task


@pytest.fixture
def output():
    return [DataNode("name_1"), DataNode("name_2"), DataNode("name_3")]


@pytest.fixture
def output_config():
    return [DataNodeConfig("name_1"), DataNodeConfig("name_2"), DataNodeConfig("name_3")]


@pytest.fixture
def input():
    return [DataNode("input_name_1"), DataNode("input_name_2"), DataNode("input_name_3")]


@pytest.fixture
def input_config():
    return [DataNodeConfig("input_name_1"), DataNodeConfig("input_name_2"), DataNodeConfig("input_name_3")]


def test_create_task():
    name = "name_1"
    task = Task(name, print, [], [])
    assert f"TASK_{name}_" in task.id
    assert task.config_id == "name_1"

    with pytest.raises(InvalidConfigurationId):
        Task("foo bar", print, [], [])

    path = "my/csv/path"
    foo_dn = CSVDataNode("foo", Scope.PIPELINE, properties={"path": path, "has_header": True})
    task = Task("name_1", print, [foo_dn], [])
    assert task.config_id == "name_1"
    assert task.id is not None
    assert task.parent_id is None
    assert task.foo == foo_dn
    assert task.foo.path == path
    with pytest.raises(AttributeError):
        task.bar

    path = "my/csv/path"
    abc_dn = InMemoryDataNode("name_1ea", Scope.SCENARIO, properties={"path": path})
    task = Task("name_1ea", print, [abc_dn], [], parent_id="parent_id")
    assert task.config_id == "name_1ea"
    assert task.id is not None
    assert task.parent_id == "parent_id"
    assert task.name_1ea == abc_dn
    assert task.name_1ea.path == path
    with pytest.raises(AttributeError):
        task.bar


def test_can_not_change_task_output(output):
    task = Task("name_1", print, output=output)

    with pytest.raises(Exception):
        task.output = {}

    assert list(task.output.values()) == output
    output.append(output[0])
    assert list(task.output.values()) != output


def test_can_not_change_task_input(input):
    task = Task("name_1", print, input=input)

    with pytest.raises(Exception):
        task.input = {}

    assert list(task.input.values()) == input
    input.append(input[0])
    assert list(task.input.values()) != input


def test_can_not_change_task_config_output(output_config):
    task_config = Config.configure_task("name_1", print, [], output=output_config)

    assert task_config.output_configs == output_config
    with pytest.raises(Exception):
        task_config.output_configs = []

    output_config.append(output_config[0])
    assert task_config._output != output_config


def test_can_not_update_task_output_values(output_config):
    data_node_cfg = Config.configure_data_node("data_node_cfg")
    task_config = Config.configure_task("name_1", print, [], output=output_config)

    task_config.output_configs.append(data_node_cfg)
    assert task_config.output_configs == output_config

    task_config.output_configs[0] = data_node_cfg
    assert task_config.output_configs[0] != data_node_cfg


def test_can_not_update_task_input_values(input_config):
    data_node_config = DataNodeConfig("data_node")
    task_config = Config.configure_task("name_1", print, input=input_config, output=[])

    task_config.input_configs.append(data_node_config)
    assert task_config.input_configs == input_config

    task_config.input_configs[0] = data_node_config
    assert task_config.input_configs[0] != data_node_config


def mock_func():
    pass


def test_auto_set_and_reload(data_node):
    task_1 = Task(config_id="foo", function=print, input=None, output=None, parent_id=None)

    _DataManager._set(data_node)
    _TaskManager._set(task_1)

    task_2 = _TaskManager._get(task_1)

    assert task_1.function == print
    task_1.function = mock_func
    assert task_1.function == mock_func
    assert task_2.function == mock_func

    with task_1 as task:
        assert task.config_id == "foo"
        assert task.parent_id is None
        assert task.function == mock_func
        assert task._is_in_context

        task.function = print

        assert task.config_id == "foo"
        assert task.parent_id is None
        assert task.function == mock_func
        assert task._is_in_context

    assert task_1.config_id == "foo"
    assert task_1.parent_id is None
    assert task_1.function == print
    assert not task_1._is_in_context


def test_submit_task(task: Task):
    with mock.patch("src.taipy.core.task._task_manager._TaskManager._submit") as mock_submit:
        task.submit([], True)
        mock_submit.assert_called_once_with(task, [], True)
