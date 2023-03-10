# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import pytest

from src.taipy.core._version._version_manager import _VersionManager
from src.taipy.core.config import MigrationConfig
from taipy.config.checker.issue_collector import IssueCollector
from taipy.config.config import Config


def mock_func():
    pass


def test_check_if_entity_property_key_used_is_predefined(caplog):
    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.errors) == 0

    Config.unique_sections[MigrationConfig.name]._properties["_entity_owner"] = None
    with pytest.raises(SystemExit):
        Config._collector = IssueCollector()
        Config.check()
    assert len(Config._collector.errors) == 1
    assert (
        "Properties of MigrationConfig `VERSION_MIGRATION` cannot have `_entity_owner` as its property." in caplog.text
    )

    caplog.clear()

    Config.unique_sections[MigrationConfig.name]._properties["_entity_owner"] = "entity_owner"
    with pytest.raises(SystemExit):
        Config._collector = IssueCollector()
        Config.check()
    assert len(Config._collector.errors) == 1
    expected_error_message = (
        "Properties of MigrationConfig `VERSION_MIGRATION` cannot have `_entity_owner` as its property."
        ' Current value of property `_entity_owner` is "entity_owner".'
    )
    assert expected_error_message in caplog.text


def test_check_callable_function(caplog):
    data_nodes1 = Config.configure_data_node("data_nodes1", "pickle")
    latest_version = _VersionManager._get_latest_version()

    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.errors) == 0
    assert len(Config._collector.warnings) == 0

    Config.add_data_node_migration_function("latest", data_nodes1, 1)
    with pytest.raises(SystemExit):
        Config._collector = IssueCollector()
        Config.check()
    assert len(Config._collector.errors) == 1
    expected_error_message = (
        f"The migration function of config `data_nodes1` from version {latest_version} must be populated with"
        " Callable value. Current value of property `migration_fcts` is 1."
    )
    assert expected_error_message in caplog.text
    assert len(Config._collector.warnings) == 0

    Config.add_data_node_migration_function("latest", data_nodes1, "bar")
    with pytest.raises(SystemExit):
        Config._collector = IssueCollector()
        Config.check()
    assert len(Config._collector.errors) == 1
    expected_error_message = (
        f"The migration function of config `data_nodes1` from version {latest_version} must be populated with"
        ' Callable value. Current value of property `migration_fcts` is "bar".'
    )
    assert expected_error_message in caplog.text
    assert len(Config._collector.warnings) == 0

    Config.add_data_node_migration_function("latest", data_nodes1, mock_func)
    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.errors) == 0
    assert len(Config._collector.warnings) == 0


def test_check_valid_version(caplog):
    data_nodes1 = Config.configure_data_node("data_nodes1", "pickle")
    latest_version = _VersionManager._get_latest_version()

    Config.add_data_node_migration_function("latest", data_nodes1, mock_func)
    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.warnings) == 0
    assert len(Config._collector.errors) == 0

    Config.add_data_node_migration_function("dev", data_nodes1, mock_func)
    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.warnings) == 0
    assert len(Config._collector.errors) == 0

    Config.add_data_node_migration_function(latest_version, data_nodes1, mock_func)
    Config._collector = IssueCollector()
    Config.check()
    assert len(Config._collector.warnings) == 0
    assert len(Config._collector.errors) == 0

    Config.unique_sections[MigrationConfig.name].migration_fcts = {"foo": {"data_nodes1": mock_func}}
    with pytest.raises(SystemExit):
        Config._collector = IssueCollector()
        Config.check()
    assert len(Config._collector.warnings) == 0
    assert len(Config._collector.errors) == 1
    expected_error_message = (
        "The source version for a migration function must be a valid version number."
        ' Current value of property `migration_fcts` is "foo".'
    )
    assert expected_error_message in caplog.text
